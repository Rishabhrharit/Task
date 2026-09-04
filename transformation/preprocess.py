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


def preprocess_records(connection_string: str) -> list[int]:
    with psycopg2.connect(connection_string) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, batch_id, source_system, source_record_id, payload
                FROM raw.api_records
                WHERE ingestion_status = 'SUCCESS'
                  AND source_system = 'shippo'
                  AND source_entity = 'shipments'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM staging.preprocessed_records AS staged
                      WHERE staged.raw_record_id = raw.api_records.id
                  )
                ORDER BY id
                """
            )
            records = cursor.fetchall()
            if not records:
                return []

            cursor.execute(
                """
                SELECT payload
                FROM raw.api_records
                WHERE ingestion_status = 'SUCCESS'
                  AND source_system = 'shippo'
                  AND source_entity = 'transactions'
                """
            )
            transactions = {
                transaction.get("shipment"): transaction
                for (transaction,) in cursor.fetchall()
                if transaction.get("shipment")
            }

            provenance = load_provenance()
            staged_ids: list[int] = []
            for sequence, (
                raw_record_id,
                batch_id,
                source_system,
                source_record_id,
                payload,
            ) in enumerate(records, start=1):
                payload_for_enrichment = dict(payload)
                transaction = transactions.get(payload.get("object_id"))
                if transaction is not None:
                    payload_for_enrichment["transaction"] = transaction
                payload_for_enrichment["synthetic_tracking_number"] = (
                    f"shp_{sequence:06d}"
                )
                enriched_attributes = enrich_shipment(payload_for_enrichment)
                enriched_attributes["batch_id"] = str(batch_id)
                combined_payload: dict[str, Any] = {
                    "source_payload": payload,
                    "enriched_attributes": enriched_attributes,
                    "field_provenance": provenance,
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
