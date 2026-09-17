from __future__ import annotations

import argparse
import json
import math
from decimal import Decimal
from numbers import Integral
import os
import re
import uuid
from typing import Any, Dict, Iterable, List, Optional

import psycopg2
from dotenv import load_dotenv
from neo4j import GraphDatabase

from config import load_project_env
from upsert import (
    CONSTRAINTS,
    CYPHER_ENRICH_FACILITY_NAMES,
    CYPHER_ENRICH_PARTNER_NAMES,
    CYPHER_ENRICH_ROUTE_NAMES,
    CYPHER_UPSERT_CARRIERS,
    CYPHER_UPSERT_EVENTS,
    CYPHER_UPSERT_EVENT_RELS,
    CYPHER_UPSERT_FACILITIES,
    CYPHER_UPSERT_ORDERS,
    CYPHER_UPSERT_PARTNERS,
    CYPHER_UPSERT_ROUTES,
    CYPHER_UPSERT_SHIPMENT_RELS,
    CYPHER_UPSERT_SHIPMENTS,
    CYPHER_UPSERT_STAGE_BASELINES,
    CYPHER_WIPE_TIMELINE_FOR_ORDERS,
    REAL_CROSSDOCK_NAMES,
    REAL_DC_NAMES,
    REAL_PARTNER_NAMES,
    REAL_PORT_NAMES,
    chunked,
)


load_project_env()


def _slug(value: Any) -> str:
    text = str(value or "").strip()
    text = re.sub(r"[^A-Za-z0-9]+", "_", text)
    text = text.strip("_")
    return text.lower() or "node"


def _as_bool(value: Any) -> bool:
    return bool(value) if value is not None else False


def _get_nested(mapping: Dict[str, Any], *keys: str) -> Any:
    current: Any = mapping
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_optional_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _safe_neo4j_int(value: Any, default: int = 0) -> int:
    if isinstance(value, str) and ("e" in value.lower() or "." in value):
        try:
            numeric_value = float(value)
        except (TypeError, ValueError, OverflowError):
            return default
        if not numeric_value.is_integer() or not -(2**63) <= numeric_value <= 2**63 - 1:
            return default
        value = numeric_value
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return default
    if not -(2**63) <= number <= 2**63 - 1:
        return default
    return number


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


def _pick_order_id(order: Dict[str, Any], default: str) -> str:
    for key in ("order_id", "orderId", "id"):
        if order.get(key):
            return str(order[key])
    return default


def _pick_shipment_id(shipment: Dict[str, Any], default: str) -> str:
    for key in ("shipment_id", "shipmentId", "id"):
        if shipment.get(key):
            return str(shipment[key])
    return default


def _pick_facility_id(facility: Dict[str, Any], default: str) -> str:
    for key in ("facility_id", "facilityId", "id"):
        if facility.get(key):
            return str(facility[key])
    return default


def _pick_carrier_id(carrier: Dict[str, Any], default: str) -> str:
    for key in ("carrier_id", "carrierId", "id"):
        if carrier.get(key):
            return str(carrier[key])
    carrier_name = carrier.get("carrier_name") or carrier.get("name") or "UNKNOWN_CARRIER"
    return f"CARRIER::{_slug(carrier_name)}"


def _pick_partner_id(partner: Dict[str, Any], default: str) -> str:
    for key in ("partner_id", "partnerId", "id"):
        if partner.get(key):
            return str(partner[key])
    partner_name = partner.get("partner_name") or partner.get("partner_display_name") or "UNKNOWN_PARTNER"
    return f"PARTNER::{_slug(partner_name)}"


def _pick_route_id(route: Dict[str, Any], default: str) -> str:
    for key in ("route_id", "routeId", "id"):
        if route.get(key):
            return str(route[key])
    origin = route.get("origin_id") or route.get("originId") or "UNKNOWN_ORIGIN"
    dest = route.get("dest_id") or route.get("destId") or "UNKNOWN_DEST"
    return f"{origin}__{dest}"


def _pick_event_id(event: Dict[str, Any], shipment_id: str) -> str:
    for key in ("event_id", "eventId", "id"):
        if event.get(key):
            return str(event[key])
    event_type = event.get("event_type") or "UNKNOWN_EVENT"
    ts = event.get("timestamp_iso") or event.get("timestamp") or "1970-01-01T00:00:00Z"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{shipment_id}|{event_type}|{ts}"))


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


def _flatten_canonical_record(row: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    payload = row.get("payload") or {}
    metadata = payload.get("metadata") or {}
    entity = payload.get("entity") or {}
    attrs = entity.get("attributes") or {}

    order = attrs.get("order") or {}
    shipment = attrs.get("shipment") or {}
    facility = attrs.get("facility") or {}
    carrier = attrs.get("carrier") or {}
    partner = attrs.get("delivery_partner") or {}
    route = attrs.get("route") or {}
    event = attrs.get("event") or {}

    batch_id = str(row.get("batch_id") or metadata.get("batch_id") or "")
    source_record_id = str(row.get("source_record_id") or metadata.get("source_record_id") or entity.get("id") or "")

    order_id = _pick_order_id(order, f"ORDER::{source_record_id or entity.get('id') or 'unknown'}")
    shipment_id = _pick_shipment_id(shipment, f"SHIPMENT::{source_record_id or entity.get('id') or 'unknown'}")
    facility_id = _pick_facility_id(facility, f"FACILITY::{source_record_id or entity.get('id') or 'unknown'}")
    carrier_name = carrier.get("carrier_name") or carrier.get("name") or partner.get("partner_name") or partner.get("partner_display_name") or "UNKNOWN_CARRIER"
    carrier_id = str(carrier.get("carrier_id") or f"CARRIER::{_slug(carrier_name)}")
    partner_name = partner.get("partner_name") or partner.get("partner_display_name") or carrier_name or "UNKNOWN_PARTNER"
    partner_id = str(partner.get("partner_id") or f"PARTNER::{_slug(partner_name)}")
    origin_id = str(route.get("origin_id") or route.get("originId") or facility_id)
    dest_id = str(route.get("dest_id") or route.get("destId") or order.get("customer_id") or f"CUSTOMER::{order_id}")
    route_id = _pick_route_id(route, f"{origin_id}__{dest_id}")
    event_id = _pick_event_id(event, shipment_id)

    facility_geo = facility.get("geo_point") or {}
    route_risk = _safe_float(route.get("risk_score"), 0.0)
    carrier_rel = _safe_float(carrier.get("reliability_idx"), 0.0)
    carrier_vol = _safe_float(carrier.get("cost_volatility"), 0.0)
    partner_cap = _safe_float(partner.get("capacity_z"), 0.0)
    partner_success = _safe_float(partner.get("success_rate_z"), 0.0)
    dwell_sigma = _safe_float(shipment.get("dwell_time_sigma"), 0.0)
    throughput_z = _safe_float(facility.get("throughput_z"), 0.0)
    delta_hours = _safe_float(event.get("delta_t_hrs"), 0.0)

    order_row = {
        "batch_id": batch_id,
        "order_id": order_id,
        "erp_source": order.get("erp_source") or "GENERIC",
        "customer_tier": order.get("customer_tier") or "STANDARD",
        "order_value_usd": _safe_float(order.get("order_value_usd"), 0.0),
        "original_promise_dt": order.get("original_promise_dt") or "",
        "target_delivery_dt": order.get("target_delivery_dt") or "",
        "estimated_delivery_dt": order.get("estimated_delivery_dt") or "",
        "order_priority": _safe_neo4j_int(order.get("order_priority")),
        "sla_health": order.get("sla_health") or "UNKNOWN",
    }

    shipment_row = {
        "batch_id": batch_id,
        "shipment_id": shipment_id,
        "order_id": order_id,
        "carrier_id": carrier_id,
        "facility_id": facility_id,
        "route_id": route_id,
        "partner_id": partner_id,
        "tracking_number": shipment.get("tracking_number") or f"TRACK::{shipment_id}",
        "dwell_time_sigma": dwell_sigma,
        "arrival_eta": shipment.get("arrival_eta") or "",
    }

    facility_row = {
        "batch_id": batch_id,
        "facility_id": facility_id,
        "facility_type": facility.get("facility_type") or "UNKNOWN",
        "region_id": facility.get("region_id") or "REGION::UNKNOWN",
        "geo_lat": _safe_optional_float(facility_geo.get("lat")),
        "geo_lon": _safe_optional_float(facility_geo.get("lon")),
        "throughput_z": throughput_z,
        "status_flag": facility.get("status_flag") or "ACTIVE",
    }

    carrier_row = {
        "batch_id": batch_id,
        "carrier_id": carrier_id,
        "carrier_name": carrier_name,
        "reliability_idx": carrier_rel,
        "cost_volatility": carrier_vol,
    }

    partner_row = {
        "batch_id": batch_id,
        "partner_id": partner_id,
        "partner_name": partner_name,
        "partner_display_name": partner.get("partner_display_name") or partner_name,
        "service_zone_id": partner.get("service_zone_id") or "UNKNOWN_ZONE",
        "capacity_z": partner_cap,
        "success_rate_z": partner_success,
    }

    route_row = {
        "batch_id": batch_id,
        "route_id": route_id,
        "origin_id": origin_id,
        "dest_id": dest_id,
        "risk_score": route_risk,
    }

    event_row = {
        "batch_id": batch_id,
        "event_id": event_id,
        "shipment_id": shipment_id,
        "order_id": order_id,
        "event_type": event.get("event_type") or "UNKNOWN",
        "timestamp_iso": event.get("timestamp_iso") or event.get("timestamp") or "1970-01-01T00:00:00Z",
        "audit_hash": event.get("audit_hash") or str(uuid.uuid4()),
        "delta_t_hrs": delta_hours,
        "location_id": event.get("location_id") or origin_id,
        "location_kind": event.get("location_kind") or "FACILITY",
        "exception_reason": event.get("exception_reason") or None,
    }

    return {
        "orders": order_row,
        "shipments": shipment_row,
        "facilities": facility_row,
        "carriers": carrier_row,
        "partners": partner_row,
        "routes": route_row,
        "events": event_row,
    }


def _neo4j_driver():
    uri = os.getenv("NEO4J_URI") or os.getenv("NEO4J_URL")
    user = os.getenv("NEO4J_USERNAME") or os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")
    if not uri or not user or not password:
        raise RuntimeError(
            "Missing Neo4j env vars. Set NEO4J_URI, NEO4J_USERNAME, and NEO4J_PASSWORD."
        )
    return GraphDatabase.driver(uri, auth=(user, password))


def _run_upsert_chunk(session: Any, query: str, rows: List[Dict[str, Any]], now: str) -> None:
    if not rows:
        return
    for chunk in chunked(rows, 1000):
        safe_chunk = [
            _sanitize_neo4j_value(row, "rows")
            for row in chunk
        ]
        session.run(query, rows=safe_chunk, now=now)


def upsert_postgres_canonical_to_neo4j(
    connection_string: str,
    *,
    batch_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> Dict[str, int]:
    """Pull canonical rows from PostgreSQL and upsert them as KG nodes/relationships."""
    rows = _canonical_rows_from_postgres(connection_string, batch_id=batch_id, limit=limit)
    if not rows:
        return {
            "orders": 0,
            "shipments": 0,
            "facilities": 0,
            "carriers": 0,
            "partners": 0,
            "routes": 0,
            "events": 0,
        }

    order_rows: List[Dict[str, Any]] = []
    shipment_rows: List[Dict[str, Any]] = []
    facility_rows: List[Dict[str, Any]] = []
    carrier_rows: List[Dict[str, Any]] = []
    partner_rows: List[Dict[str, Any]] = []
    route_rows: List[Dict[str, Any]] = []
    event_rows: List[Dict[str, Any]] = []

    for row in rows:
        flattened = _flatten_canonical_record(row)
        order_rows.append(_sanitize_neo4j_value(flattened["orders"], "orders"))
        shipment_rows.append(_sanitize_neo4j_value(flattened["shipments"], "shipments"))
        facility_rows.append(_sanitize_neo4j_value(flattened["facilities"], "facilities"))
        carrier_rows.append(_sanitize_neo4j_value(flattened["carriers"], "carriers"))
        partner_rows.append(_sanitize_neo4j_value(flattened["partners"], "partners"))
        route_rows.append(_sanitize_neo4j_value(flattened["routes"], "routes"))
        event_rows.append(_sanitize_neo4j_value(flattened["events"], "events"))

    driver = _neo4j_driver()
    now = str(__import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat())

    try:
        with driver.session() as session:
            for statement in CONSTRAINTS:
                session.run(statement)

            _run_upsert_chunk(session, CYPHER_UPSERT_FACILITIES, facility_rows, now)
            _run_upsert_chunk(session, CYPHER_UPSERT_CARRIERS, carrier_rows, now)
            _run_upsert_chunk(session, CYPHER_UPSERT_PARTNERS, partner_rows, now)
            _run_upsert_chunk(session, CYPHER_UPSERT_ROUTES, route_rows, now)
            _run_upsert_chunk(session, CYPHER_UPSERT_ORDERS, order_rows, now)
            _run_upsert_chunk(session, CYPHER_UPSERT_SHIPMENTS, shipment_rows, now)
            _run_upsert_chunk(session, CYPHER_UPSERT_SHIPMENT_RELS, shipment_rows, now)

            order_ids = sorted({row.get("order_id") for row in order_rows if row.get("order_id")})
            if order_ids:
                session.run(CYPHER_WIPE_TIMELINE_FOR_ORDERS, order_ids=order_ids).consume()

            _run_upsert_chunk(session, CYPHER_UPSERT_EVENTS, event_rows, now)
            _run_upsert_chunk(session, CYPHER_UPSERT_EVENT_RELS, event_rows, now)

            session.run(
                CYPHER_ENRICH_FACILITY_NAMES,
                bid=batch_id or "",
                dc_names=REAL_DC_NAMES,
                port_names=REAL_PORT_NAMES,
                crossdock_names=REAL_CROSSDOCK_NAMES,
            )
            session.run(
                CYPHER_ENRICH_PARTNER_NAMES,
                bid=batch_id or "",
                partner_names=REAL_PARTNER_NAMES,
            )
            session.run(CYPHER_ENRICH_ROUTE_NAMES, bid=batch_id or "")

            rec = session.run(CYPHER_UPSERT_STAGE_BASELINES, now=now).single()
            if rec is not None:
                print(
                    f"✅ StageBaseline upserted: {int(rec.get('n_baselines') or 0)} groups from {int(rec.get('n_events') or 0)} events"
                )
    finally:
        driver.close()

    counts = {
        "orders": len(order_rows),
        "shipments": len(shipment_rows),
        "facilities": len(facility_rows),
        "carriers": len(carrier_rows),
        "partners": len(partner_rows),
        "routes": len(route_rows),
        "events": len(event_rows),
    }
    return counts


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
        print({
            "row_count": len(rows),
            "batch_id_filter": args.batch_id or "(all)",
            "sample": _flatten_canonical_record(rows[0]) if rows else None,
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
