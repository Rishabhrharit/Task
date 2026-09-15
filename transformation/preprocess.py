"""Combine a raw API payload with a source mapping into canonical staging data."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
import yaml

from config import load_project_env
from transformation.enrichment import enrich_order, enrich_shipment
from transformation.generic_mapper import map_payload_to_canonical, load_mapping_config

PROVENANCE_PATH = Path(__file__).with_name("field_provenance.yaml")
load_project_env()


def load_provenance() -> dict[str, Any]:
    with PROVENANCE_PATH.open(encoding="utf-8") as provenance_file:
        provenance = yaml.safe_load(provenance_file)
    if not isinstance(provenance, dict):
        raise ValueError("The provenance file must contain a top-level mapping.")
    return provenance


def build_enriched_attributes(
    payload: dict[str, Any],
    *,
    source_system: str,
    source_entity: str,
    batch_id: str,
    mapping_path: str | None = None,
) -> dict[str, Any]:
    mapping = load_mapping_config(mapping_path)
    if mapping is not None:
        mapped = map_payload_to_canonical(
            payload,
            mapping=mapping,
            source_system=source_system,
            source_entity=source_entity,
            batch_id=batch_id,
        )
        mapped["batch_id"] = str(batch_id)
        return mapped

    if source_entity in {"orders", "order"}:
        enriched_attributes = enrich_order(payload)
    else:
        payload_for_enrichment = dict(payload)
        payload_for_enrichment["synthetic_tracking_number"] = (
            payload_for_enrichment.get("synthetic_tracking_number")
            or f"src_{source_entity}_{batch_id[:8]}"
        )
        enriched_attributes = enrich_shipment(payload_for_enrichment)
    enriched_attributes["batch_id"] = str(batch_id)
    return enriched_attributes


def preprocess_records(connection_string: str, batch_id: str | None = None) -> list[int]:
    mapping_path = os.getenv("SOURCE_MAPPING_PATH") or os.getenv("MAPPING_CONFIG_PATH")
    with psycopg2.connect(connection_string) as connection:
        with connection.cursor() as cursor:
            query = """
                SELECT id, batch_id, source_system, source_entity,
                       source_record_id, payload
                FROM raw.api_records
                WHERE ingestion_status = 'SUCCESS'
            """
            params: list[Any] = []
            if batch_id:
                query += " AND batch_id = %s"
                params.append(batch_id)
            query += """
                  AND NOT EXISTS (
                      SELECT 1
                      FROM staging.preprocessed_records AS staged
                      WHERE staged.raw_record_id = raw.api_records.id
                  )
                ORDER BY id
                """
            cursor.execute(query, params)
            records = cursor.fetchall()
            if not records:
                return []

            provenance = load_provenance()
            staged_ids: list[int] = []
            for raw_record_id, batch_id, source_system, source_entity, source_record_id, payload in records:
                payload_dict = payload if isinstance(payload, dict) else {}
                payload_for_mapping = dict(payload_dict)
                if not payload_for_mapping.get("synthetic_tracking_number"):
                    payload_for_mapping["synthetic_tracking_number"] = (
                        f"{source_system}_{source_entity}_{raw_record_id:06d}"
                    )

                enriched_attributes = build_enriched_attributes(
                    payload_for_mapping,
                    source_system=source_system,
                    source_entity=source_entity,
                    batch_id=str(batch_id),
                    mapping_path=mapping_path,
                )
                combined_payload: dict[str, Any] = {
                    "source_payload": payload_dict,
                    "enriched_attributes": enriched_attributes,
                    "field_provenance": provenance,
                    "source_system": source_system,
                    "source_entity": source_entity,
                }
                cursor.execute(
                    """
                    INSERT INTO staging.preprocessed_records (
                        raw_record_id,
                        batch_id,
                        source_system,
                        source_record_id,
                        processed_at,
                        payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        raw_record_id,
                        str(batch_id),
                        source_system,
                        source_record_id,
                        datetime.now(timezone.utc),
                        json.dumps(combined_payload),
                    ),
                )
                staged_ids.append(cursor.fetchone()[0])
            return staged_ids


def main() -> int:
    connection_string = os.environ.get("DATABASE_URL")
    if not connection_string:
        print("Set DATABASE_URL before running preprocessing.", file=sys.stderr)
        return 2

    record_ids = preprocess_records(connection_string)
    print(f"Stored {len(record_ids)} combined preprocessed records")
    return 0


if __name__ == "__main__":
    sys.exit(main())
