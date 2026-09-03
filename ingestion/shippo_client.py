"""Fetch a Shippo response and preserve it in PostgreSQL's raw layer."""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
import requests


SHIPPO_API_URL = "https://api.goshippo.com"


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as request_file:
        payload = json.load(request_file)
    if not isinstance(payload, dict):
        raise ValueError("The request JSON must contain an object at the top level.")
    return payload


def response_payload(response: requests.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError:
        return {"raw_text": response.text}
    if isinstance(payload, dict):
        return payload
    return {"response": payload}


def store_raw_response(
    connection_string: str,
    batch_id: uuid.UUID,
    source_entity: str,
    extracted_at: datetime,
    payload: dict[str, Any],
    status: str,
    error_message: str | None = None,
) -> None:
    source_record_id = payload.get("object_id")
    with psycopg2.connect(connection_string) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO raw.api_records (
                    batch_id,
                    source_system,
                    source_entity,
                    source_record_id,
                    extracted_at,
                    payload,
                    ingestion_status,
                    error_message
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(batch_id),
                    "shippo",
                    source_entity,
                    source_record_id,
                    extracted_at,
                    json.dumps(payload),
                    status,
                    error_message,
                ),
            )


def ingest(request_path: Path, endpoint: str, token: str, connection_string: str) -> uuid.UUID:
    batch_id = uuid.uuid4()
    extracted_at = datetime.now(timezone.utc)
    request_payload = load_json(request_path)
    response = requests.post(
        f"{SHIPPO_API_URL.rstrip('/')}/{endpoint.lstrip('/')}",
        headers={
            "Authorization": f"ShippoToken {token}",
            "Content-Type": "application/json",
        },
        json=request_payload,
        timeout=30,
    )
    payload = response_payload(response)
    status = "SUCCESS" if response.ok else "FAILED"
    error_message = None if response.ok else f"HTTP {response.status_code}"
    store_raw_response(
        connection_string,
        batch_id,
        endpoint.strip("/").replace("/", "_"),
        extracted_at,
        payload,
        status,
        error_message,
    )
    response.raise_for_status()
    return batch_id


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("request_file", type=Path)
    parser.add_argument("--endpoint", default="shipments/")
    args = parser.parse_args()

    token = os.environ.get("SHIPPO_API_TOKEN")
    connection_string = os.environ.get("DATABASE_URL")
    if not token:
        parser.error("Set SHIPPO_API_TOKEN before running the ingestion script.")
    if not connection_string:
        parser.error("Set DATABASE_URL before running the ingestion script.")

    batch_id = ingest(args.request_file, args.endpoint, token, connection_string)
    print(f"Stored Shippo response in batch {batch_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
