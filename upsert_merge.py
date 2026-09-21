from __future__ import annotations

import argparse
import json
import math
import os
import re
from datetime import datetime, timezone
from decimal import Decimal
from numbers import Integral
from typing import Any, Dict, Iterable, List, Optional

import psycopg2
from neo4j import GraphDatabase

from config import load_project_env

load_project_env()

_LABEL_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_label(value: Any, default: str = "Record") -> str:
    """Validate a user-supplied label/relationship type before inlining it into Cypher."""
    text = re.sub(r"[^A-Za-z0-9_]", "_", str(value or "").strip())
    return text if _LABEL_RE.match(text) else default


def chunked(rows: List[Dict[str, Any]], batch_size: int) -> Iterable[List[Dict[str, Any]]]:
    for i in range(0, len(rows), batch_size):
        yield rows[i : i + batch_size]


def _sanitize_neo4j_value(value: Any, path: str = "$") -> Any:
    """Keep values sent through the Neo4j driver within supported numeric ranges."""
    if isinstance(value, bool):
        return value
    if isinstance(value, Integral):
        integer_value = int(value)
        if -(2**63) <= integer_value <= 2**63 - 1:
            return integer_value
        print(f"Warning: replaced out-of-range integer at {path} with 0")
        return 0
    if isinstance(value, Decimal):
        if not value.is_finite():
            print(f"Warning: replaced non-finite decimal at {path} with null")
            return None
        as_float = float(value)
        if math.isfinite(as_float):
            return as_float
        print(f"Warning: replaced out-of-range decimal at {path} with null")
        return None
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        print(f"Warning: replaced non-finite float at {path} with null")
        return None
    if isinstance(value, dict):
        return {
            key: _sanitize_neo4j_value(nested, f"{path}.{key}")
            for key, nested in value.items()
        }
    if isinstance(value, list):
        return [
            _sanitize_neo4j_value(item, f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    return value


def _canonical_rows_from_postgres(
    connection_string: str,
    batch_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    query = """
        SELECT id, batch_id, source_record_id, payload
        FROM canonical.otc_records
    """
    params: List[Any] = []

    if batch_id:
        query += " WHERE batch_id = %s"
        params.append(batch_id)

    query += " ORDER BY id"
    if limit is not None:
        query += " LIMIT %s"
        params.append(limit)

    with psycopg2.connect(connection_string) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            results = cursor.fetchall()

    rows: List[Dict[str, Any]] = []
    for row_id, row_batch_id, source_record_id, payload in results:
        if isinstance(payload, str):
            payload = json.loads(payload)
        rows.append(
            {
                "id": row_id,
                "batch_id": row_batch_id,
                "source_record_id": source_record_id,
                "payload": payload,
            }
        )
    return rows


def _node_labels(entity_type: str, attributes: Dict[str, Any]) -> List[str]:
    """Build the Entity:<node_type>:<entity_type> compound label scheme the
    analytical Cypher (e.g. Entity:BUSINESS_OBJECT:ORDER) expects, instead of
    a single bare label."""
    node_type = _safe_label(attributes.get("node_type") or "BUSINESS_OBJECT", default="BUSINESS_OBJECT")
    labels = ["Entity", node_type, entity_type]
    seen: List[str] = []
    for label in labels:
        if label and label not in seen:
            seen.append(label)
    return seen


def _derive_events(
    attributes: Dict[str, Any], *, entity_type: str, entity_id: str, batch_id: str
) -> List[Dict[str, Any]]:
    """Derive one EVENT node per timestamp-like attribute (any key ending in
    '_at' or '_date' with a non-empty string value). This is a generic,
    naming-convention-driven derivation, not tied to any specific entity
    shape: whatever the mapping's canonical output happens to name that way
    becomes an event. case_id/process are carried over when the mapping
    happens to produce them, since analytical queries commonly group events
    by case."""
    events: List[Dict[str, Any]] = []
    case_id = attributes.get("case_id")
    process = attributes.get("process")
    for key, value in attributes.items():
        if not isinstance(value, str) or not value:
            continue
        if not (key.endswith("_at") or key.endswith("_date")):
            continue
        events.append(
            {
                "event_id": f"{case_id or entity_id}::{entity_type}::{entity_id}::{key.upper()}",
                "process": process,
                "case_id": case_id,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "event_type": f"{entity_type}_{key.upper()}",
                "event_time": value,
                "batch_id": batch_id,
            }
        )
    return events


def _extract_node_and_relationships(
    row: Dict[str, Any],
) -> tuple[List[str], Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Turn one canonical row into a graph node plus its outgoing relationships
    and derived events, driven entirely by entity.type/entity.id/entity.attributes
    and the top-level relationships array. No entity shape is assumed."""
    payload = row.get("payload") or {}
    metadata = payload.get("metadata") or {}
    entity = payload.get("entity") or {}
    attributes = entity.get("attributes") or {}
    if not isinstance(attributes, dict):
        attributes = {}

    batch_id = str(row.get("batch_id") or metadata.get("batch_id") or "")
    source_record_id = str(row.get("source_record_id") or metadata.get("source_record_id") or "")
    entity_id = str(entity.get("id") or source_record_id or f"RECORD::{row.get('id')}")
    entity_type = _safe_label(entity.get("type") or metadata.get("source_entity"))
    labels = _node_labels(entity_type, attributes)

    node = {
        "id": entity_id,
        "batch_id": batch_id,
        "attributes": attributes,
    }

    relationships: List[Dict[str, Any]] = []
    for rel in payload.get("relationships") or []:
        if not isinstance(rel, dict):
            continue
        from_id = rel.get("from_id")
        to_id = rel.get("to_id")
        if from_id in (None, "") or to_id in (None, ""):
            continue
        relationships.append(
            {
                "type": _safe_label(rel.get("type"), default="RELATED_TO"),
                "from_id": str(from_id),
                "to_id": str(to_id),
                "attributes": rel.get("attributes") or {},
                "batch_id": batch_id,
            }
        )

    events = _derive_events(attributes, entity_type=entity_type, entity_id=entity_id, batch_id=batch_id)

    return labels, node, relationships, events


def _node_cypher(labels: List[str]) -> str:
    label_pattern = "".join(f"`{label}`:" for label in labels).rstrip(":")
    return f"""
    UNWIND $rows AS row
    MERGE (n:{label_pattern} {{id: row.id}})
    SET n += row.attributes
    SET n.batch_id = row.batch_id, n.updated_at = $now
    RETURN count(n) AS written
    """


def _relationship_cypher(rel_type: str) -> str:
    # MATCH (not MERGE) on both endpoints: a relationship only materializes
    # once both entities have themselves been ingested as nodes.
    return f"""
    UNWIND $rows AS row
    MATCH (a {{id: row.from_id}})
    MATCH (b {{id: row.to_id}})
    MERGE (a)-[r:`{rel_type}`]->(b)
    SET r += row.attributes
    SET r.batch_id = row.batch_id, r.updated_at = $now
    RETURN count(r) AS written
    """


def _constraint_cypher(label: str) -> str:
    return f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:`{label}`) REQUIRE n.id IS UNIQUE"


def _event_cypher() -> str:
    return """
    UNWIND $rows AS row
    MERGE (ev:EVENT {event_id: row.event_id})
    SET ev += row, ev.updated_at = $now
    RETURN count(ev) AS written
    """


def _event_relationship_cypher() -> str:
    # Links each derived EVENT back to the business-object node it came from,
    # matched by entity_id/entity_type rather than a fixed relationship type
    # per domain, so this stays generic across whatever gets ingested.
    return """
    UNWIND $rows AS row
    MATCH (ev:EVENT {event_id: row.event_id})
    MATCH (n {id: row.entity_id})
    WHERE row.entity_type IN labels(n)
    MERGE (ev)-[r:LOGGED_FOR]->(n)
    RETURN count(r) AS written
    """


def _neo4j_driver():
    uri = os.getenv("NEO4J_URI") or os.getenv("NEO4J_URL")
    user = os.getenv("NEO4J_USERNAME") or os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")
    if not uri or not user or not password:
        raise RuntimeError(
            "Missing Neo4j env vars. Set NEO4J_URI, NEO4J_USERNAME, and NEO4J_PASSWORD."
        )
    return GraphDatabase.driver(uri, auth=(user, password))


def _run_upsert_chunk(session: Any, query: str, rows: List[Dict[str, Any]], now: str) -> int:
    """Run one UNWIND batch and return how many nodes/relationships Neo4j actually wrote."""
    written = 0
    if not rows:
        return written
    for chunk in chunked(rows, 1000):
        safe_chunk = [
            _sanitize_neo4j_value(row, "rows")
            for row in chunk
        ]
        record = session.run(query, rows=safe_chunk, now=now).single()
        written += int(record["written"]) if record else 0
    return written


def upsert_postgres_canonical_to_neo4j(
    connection_string: str,
    *,
    batch_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Pull canonical rows from PostgreSQL and upsert them as generic KG nodes and
    relationships. Node labels and relationship types come entirely from each
    record's entity.type and relationships array; no fixed entity shape is assumed."""
    rows = _canonical_rows_from_postgres(connection_string, batch_id=batch_id, limit=limit)
    if not rows:
        return {"nodes": 0, "relationships": 0, "labels": {}}

    nodes_by_label: Dict[tuple[str, ...], List[Dict[str, Any]]] = {}
    rels_by_type: Dict[str, List[Dict[str, Any]]] = {}
    all_events: List[Dict[str, Any]] = []

    for row in rows:
        labels, node, relationships, events = _extract_node_and_relationships(row)
        label_key = tuple(labels)
        nodes_by_label.setdefault(label_key, []).append(_sanitize_neo4j_value(node, ":".join(labels)))
        for rel in relationships:
            rels_by_type.setdefault(rel["type"], []).append(_sanitize_neo4j_value(rel, rel["type"]))
        all_events.extend(_sanitize_neo4j_value(event, "EVENT") for event in events)

    driver = _neo4j_driver()
    now = str(datetime.now(timezone.utc).isoformat())

    nodes_written_by_label: Dict[str, int] = {}
    relationships_written_by_type: Dict[str, int] = {}

    try:
        with driver.session() as session:
            for labels in nodes_by_label:
                session.run(_constraint_cypher(labels[-1]))
            for labels, node_rows in nodes_by_label.items():
                nodes_written_by_label[":".join(labels)] = _run_upsert_chunk(
                    session, _node_cypher(list(labels)), node_rows, now
                )
            for rel_type, rel_rows in rels_by_type.items():
                relationships_written_by_type[rel_type] = _run_upsert_chunk(
                    session, _relationship_cypher(rel_type), rel_rows, now
                )
            events_written = _run_upsert_chunk(session, _event_cypher(), all_events, now)
            events_linked = _run_upsert_chunk(session, _event_relationship_cypher(), all_events, now)
    finally:
        driver.close()

    relationships_submitted = sum(len(v) for v in rels_by_type.values())
    relationships_written = sum(relationships_written_by_type.values())

    return {
        "nodes": sum(nodes_written_by_label.values()),
        "relationships": relationships_written,
        "relationships_skipped_missing_endpoint": relationships_submitted - relationships_written,
        "events": events_written,
        "events_linked": events_linked,
        "labels": nodes_written_by_label,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load canonical Postgres rows into Neo4j Aura by bridging the DB layer to the KG upsert model."
    )
    parser.add_argument("--batch_id", default="", help="Only upsert a single canonical batch_id")
    parser.add_argument("--limit", type=int, default=0, help="Optional row cap; 0 means no limit")
    parser.add_argument("--dry_run", action="store_true", help="Show what would be loaded without writing")
    args = parser.parse_args()

    connection_string = os.getenv("DATABASE_URL")
    if not connection_string:
        raise RuntimeError("Set DATABASE_URL before running the canonical-to-KG bridge.")

    limit = None if args.limit <= 0 else args.limit
    rows = _canonical_rows_from_postgres(connection_string, batch_id=args.batch_id or None, limit=limit)
    if args.dry_run:
        sample = None
        if rows:
            labels, node, relationships, events = _extract_node_and_relationships(rows[0])
            sample = {"label": ":".join(labels), "node": node, "relationships": relationships, "events": events}
        print({
            "row_count": len(rows),
            "batch_id_filter": args.batch_id or "(all)",
            "sample": sample,
        })
        return

    results = upsert_postgres_canonical_to_neo4j(
        connection_string,
        batch_id=args.batch_id or None,
        limit=limit,
    )
    print("Canonical -> Neo4j bridge complete")
    print(results)


if __name__ == "__main__":
    main()
