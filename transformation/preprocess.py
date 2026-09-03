"""Combine a raw Shippo payload with generated attributes in staging."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
import yaml

from transformation.enrichment import enrich_shipment


PROVENANCE_PATH = Path(__file__).with_name("field_provenance.yaml")


def load_provenance() -> dict[str, Any]:
    with PROVENANCE_PATH.open(encoding="utf-8") as provenance_file:
        provenance = yaml.safe_load(provenance_file)
    if not isinstance(provenance, dict):
        raise ValueError("The provenance file must contain a top-level mapping.")
    return provenance


def preprocess_latest(connection_string: str) -> int:
    with psycopg2.connect(connection_string) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, batch_id, source_system, source_record_id, payload
                FROM raw.api_records
                WHERE ingestion_status = 'SUCCESS'
                ORDER BY id DESC
                LIMIT 1
                """
            )
            record = cursor.fetchone()
            if record is None:
                raise RuntimeError("No successful raw API records are available.")

            raw_record_id, batch_id, source_system, source_record_id, payload = record
            combined_payload: dict[str, Any] = {
                "source_payload": payload,
                "enriched_attributes": enrich_shipment(payload),
                "field_provenance": load_provenance(),
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
            return cursor.fetchone()[0]


def main() -> int:
    connection_string = os.environ.get("DATABASE_URL")
    if not connection_string:
        print("Set DATABASE_URL before running preprocessing.", file=sys.stderr)
        return 2

    record_id = preprocess_latest(connection_string)
    print(f"Stored combined preprocessed record {record_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
