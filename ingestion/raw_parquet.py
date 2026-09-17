"""Convert MinIO raw landing JSON batches into a Parquet twin (raw layer)."""

from __future__ import annotations

import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.object_storage import _build_client, minio_enabled, upload_bytes


def _landing_objects_for_batch(client: Any, bucket: str, batch_id: str) -> list[str]:
    response = client.list_objects_v2(Bucket=bucket, Prefix="raw/")
    return [
        item["Key"]
        for item in response.get("Contents", [])
        if item["Key"].endswith(f"/{batch_id}.json")
    ]


def _parquet_objects_for_batch(client: Any, bucket: str, batch_id: str | None) -> list[str]:
    response = client.list_objects_v2(Bucket=bucket, Prefix="parquet/raw/")
    return [
        item["Key"]
        for item in response.get("Contents", [])
        if batch_id is None or item["Key"].endswith(f"/{batch_id}.parquet")
    ]


def convert_landing_batch_to_parquet(
    batch_id: str,
    *,
    output_dir: str | os.PathLike[str] = "exports/raw",
) -> list[str]:
    """Write a Parquet twin of every MinIO JSON landing object for a batch.

    The nested source payload is kept as a JSON string column rather than
    flattened, since arbitrary API payloads (e.g. nested rate/address arrays)
    cannot be losslessly flattened into fixed relational columns.

    Returns the MinIO keys written. Returns an empty list when MinIO is not
    configured or the batch has no landing objects yet (nothing to convert).
    """
    if not minio_enabled():
        return []

    bucket = os.getenv("MINIO_BUCKET", "otc-raw")
    client = _build_client()
    object_keys = _landing_objects_for_batch(client, bucket, batch_id)
    if not object_keys:
        return []

    written_keys: list[str] = []
    extracted_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for object_key in object_keys:
        landing = json.loads(client.get_object(Bucket=bucket, Key=object_key)["Body"].read())
        raw_records = landing.get("payload", {}).get("raw_records") or []
        if not raw_records:
            continue

        rows = [
            {
                "source_record_id": record.get("source_record_id"),
                "extracted_at": record.get("extracted_at"),
                "ingestion_status": record.get("ingestion_status", "SUCCESS"),
                "error_message": record.get("error_message"),
                "payload_json": json.dumps(record.get("payload", {})),
            }
            for record in raw_records
        ]
        dataframe = pd.DataFrame(rows)
        dataframe["source_system"] = landing.get("source_system")
        dataframe["source_entity"] = landing.get("source_entity")
        dataframe["batch_id"] = landing.get("batch_id")

        table = pa.Table.from_pandas(dataframe, preserve_index=False)
        local_path = (
            Path(output_dir)
            / f"{landing.get('source_system')}_{landing.get('source_entity')}_{batch_id}.parquet"
        )
        local_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, local_path)

        parquet_key = (
            f"parquet/raw/{landing.get('source_system')}/{landing.get('source_entity')}"
            f"/{extracted_date}/{batch_id}.parquet"
        )
        upload_bytes(key=parquet_key, body=local_path.read_bytes(), bucket=bucket)
        written_keys.append(parquet_key)
    return written_keys


def _clean(value: Any) -> Any:
    """Convert pandas NaN/NaT back to None; leave real values untouched."""
    if isinstance(value, float) and pd.isna(value):
        return None
    return value


def hydrate_from_parquet(connection: Any, batch_id: str | None) -> int:
    """Replay Parquet raw-layer object(s) into raw.api_records exactly once.

    This is the Postgres-facing counterpart to convert_landing_batch_to_parquet:
    preprocessing reads the Parquet twin rather than the original JSON object.
    """
    if not minio_enabled():
        return 0

    bucket = os.getenv("MINIO_BUCKET", "otc-raw")
    client = _build_client()
    object_keys = _parquet_objects_for_batch(client, bucket, batch_id)
    if not object_keys:
        if batch_id is None:
            return 0
        raise RuntimeError(f"No Parquet landing object found for batch {batch_id}.")

    inserted = 0
    with connection.cursor() as cursor:
        for object_key in object_keys:
            body = client.get_object(Bucket=bucket, Key=object_key)["Body"].read()
            table = pq.read_table(io.BytesIO(body))
            dataframe = table.to_pandas()
            if dataframe.empty:
                continue
            for _, row in dataframe.iterrows():
                object_batch_id = _clean(row.get("batch_id")) or batch_id
                source_system = _clean(row.get("source_system"))
                source_entity = _clean(row.get("source_entity"))
                source_record_id = _clean(row.get("source_record_id"))
                extracted_at = datetime.fromisoformat(row["extracted_at"])
                payload = json.loads(row["payload_json"]) if _clean(row.get("payload_json")) else {}
                ingestion_status = _clean(row.get("ingestion_status")) or "SUCCESS"
                error_message = _clean(row.get("error_message"))
                cursor.execute(
                    """
                    INSERT INTO raw.api_records (
                        batch_id, source_system, source_entity, source_record_id,
                        extracted_at, payload, ingestion_status, error_message
                    )
                    SELECT %s, %s, %s, %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM raw.api_records
                        WHERE batch_id = %s
                          AND source_system = %s
                          AND source_entity = %s
                          AND source_record_id IS NOT DISTINCT FROM %s
                    )
                    """,
                    (
                        object_batch_id,
                        source_system,
                        source_entity,
                        source_record_id,
                        extracted_at,
                        json.dumps(payload),
                        ingestion_status,
                        error_message,
                        object_batch_id,
                        source_system,
                        source_entity,
                        source_record_id,
                    ),
                )
                inserted += cursor.rowcount
    return inserted

