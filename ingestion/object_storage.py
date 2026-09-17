"""S3-compatible object storage for immutable ingestion landing files."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any


def minio_enabled() -> bool:
    return bool(os.getenv("MINIO_ENDPOINT"))


def _build_client() -> Any:
    access_key = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD")
    if not access_key or not secret_key:
        raise RuntimeError(
            "MINIO_ACCESS_KEY and MINIO_SECRET_KEY are required when MINIO_ENDPOINT is set."
        )

    import boto3
    from botocore.client import Config

    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
        use_ssl=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )


def upload_bytes(
    *,
    key: str,
    body: bytes,
    content_type: str = "application/octet-stream",
    bucket: str | None = None,
) -> str | None:
    """Upload raw bytes (e.g. a Parquet file) to the configured MinIO bucket."""
    if not minio_enabled():
        return None
    client = _build_client()
    client.put_object(
        Bucket=bucket or os.getenv("MINIO_BUCKET", "otc-raw"),
        Key=key,
        Body=body,
        ContentType=content_type,
    )
    return key


def upload_landing_records(
    *,
    batch_id: str,
    source_system: str,
    source_entity: str,
    records: list[dict[str, Any]],
) -> str | None:
    return upload_landing_payload(
        batch_id=batch_id,
        source_system=source_system,
        source_entity=source_entity,
        payload={"raw_records": records},
    )


def upload_landing_payload(
    *,
    batch_id: str,
    source_system: str,
    source_entity: str,
    payload: Any,
) -> str | None:
    endpoint = os.getenv("MINIO_ENDPOINT")
    if not endpoint:
        return None

    access_key = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD")
    bucket = os.getenv("MINIO_BUCKET", "otc-raw")
    if not access_key or not secret_key:
        raise RuntimeError(
            "MINIO_ACCESS_KEY and MINIO_SECRET_KEY are required when MINIO_ENDPOINT is set."
        )

    import boto3
    from botocore.client import Config

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
        use_ssl=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )
    extracted_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"raw/{source_system}/{source_entity}/{extracted_date}/{batch_id}.json"
    body = json.dumps(
        {
            "batch_id": batch_id,
            "source_system": source_system,
            "source_entity": source_entity,
            "payload": payload,
        },
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    return key