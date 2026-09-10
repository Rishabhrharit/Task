"""Normalize staged records into the fixed otc.v1 contract."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

import psycopg2

from config import load_project_env
from transformation.canonical_schema import apply_canonical_schema

load_project_env()

def clean_values(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: clean_values(nested) for key, nested in value.items()}
    if isinstance(value, list):
        return [clean_values(item) for item in value]
    if isinstance(value, str):
        return value.strip()
    return value


def numeric_values(value: Any, prefix: str = "") -> dict[str, float]:
    if isinstance(value, dict):
        result: dict[str, float] = {}
        for key, nested in value.items():
            result.update(numeric_values(nested, f"{prefix}.{key}" if prefix else key))
        return result
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return {prefix: float(value)}
    return {}


def remove_numeric_outliers(
    records: list[tuple[int, Any, str | None, dict[str, Any]]],
) -> list[tuple[int, Any, str | None, dict[str, Any]]]:
    """Remove the lowest/highest 1% per numeric field when the batch is large enough."""
    trim_count = int(len(records) * 0.01)
    if trim_count == 0:
        return records

    values_by_path: dict[str, list[float]] = {}
    for record in records:
        for path, value in numeric_values(record[3]).items():
            values_by_path.setdefault(path, []).append(value)

    bounds = {}
    for path, values in values_by_path.items():
        ordered = sorted(values)
        bounds[path] = (ordered[trim_count], ordered[-trim_count - 1])

    return [
        record
        for record in records
        if all(
            lower <= value <= upper
            for path, value in numeric_values(record[3]).items()
            for lower, upper in [bounds[path]]
        )
    ]


def normalize_record(staged: dict[str, Any]) -> dict[str, Any]:
    source = clean_values(staged["source_payload"])
    enriched = clean_values(staged["enriched_attributes"])
    shipment_id = enriched["entity_id"]
    source_system = staged.get("source_system", "shippo")
    source_entity = staged.get("source_entity", "shipments")
    metadata = {
        "batch_id": staged["source_payload"].get("batch_id", ""),
        "source_system": source_system,
        "source_entity": source_entity,
        "source_record_id": shipment_id,
        "extracted_at": source.get("object_created") or source.get("updated_at", ""),
        "updated_at": source.get("object_updated") or source.get("updated_at", ""),
    }
    record = {
        "schema_version": "otc.v1",
        "metadata": metadata,
        "entity": {
            "type": "shipment",
            "id": shipment_id,
            "attributes": {
                "batch_id": enriched.get("batch_id"),
                "erp_source": enriched["erp_source"],
                "order": enriched["order"],
                "shipment": enriched["shipment"],
                "facility": enriched["facility"],
                "carrier": enriched["carrier"],
                "delivery_partner": enriched["delivery_partner"],
                "route": enriched["route"],
                "event": enriched["event"],
            },
        },
        "relationships": [
            {
                "type": "SHIPS_TO",
                "from_id": enriched["route"]["origin_id"],
                "to_id": enriched["route"]["dest_id"],
                "attributes": {},
            }
        ],
        "derived": enriched["derived"],
    }
    return apply_canonical_schema(record)


def normalize_staged_records(connection_string: str) -> int:
    with psycopg2.connect(connection_string) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, batch_id, source_record_id, payload
                FROM staging.preprocessed_records
                WHERE NOT EXISTS (
                    SELECT 1 FROM canonical.otc_records AS canonical
                    WHERE canonical.staging_record_id = staging.preprocessed_records.id
                )
                ORDER BY id
                """
            )
            records = cursor.fetchall()
            normalized_records = [
                (staging_id, batch_id, source_record_id, normalize_record(payload))
                for staging_id, batch_id, source_record_id, payload in records
            ]
            normalized_records = remove_numeric_outliers(normalized_records)
            for staging_id, batch_id, source_record_id, canonical_payload in normalized_records:
                cursor.execute(
                    """
                    INSERT INTO canonical.otc_records
                        (staging_record_id, batch_id, source_record_id, processed_at, payload)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        staging_id,
                        str(batch_id),
                        source_record_id,
                        datetime.now(timezone.utc),
                        json.dumps(canonical_payload),
                    ),
                )
            return len(normalized_records)


def main() -> int:
    connection_string = os.environ.get("DATABASE_URL")
    if not connection_string:
        print("Set DATABASE_URL before normalizing.", file=sys.stderr)
        return 2
    print(f"Stored {normalize_staged_records(connection_string)} canonical records")
    return 0


if __name__ == "__main__":
    sys.exit(main())
