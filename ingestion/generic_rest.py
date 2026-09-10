"""Configurable REST ingestion template for adding a new API source."""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
import requests
from config import load_project_env

load_project_env()


def required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Set {name} before running generic ingestion.")
    return value


def value_at(payload: Any, path: str) -> Any:
    value = payload
    for part in filter(None, path.split(".")):
        if not isinstance(value, dict):
            raise ValueError(f"Response path '{path}' does not resolve to a value.")
        value = value.get(part)
    return value


def records_from_response(payload: Any) -> list[dict[str, Any]]:
    path = os.getenv("API_RECORDS_PATH", "").strip()
    records = value_at(payload, path) if path else payload
    if not isinstance(records, list) or not all(isinstance(item, dict) for item in records):
        raise ValueError("The configured API response must contain a list of objects.")
    return records


def database_connection():
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return psycopg2.connect(database_url)
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def ingest() -> str:
    url = required("API_URL")
    source_system = required("SOURCE_SYSTEM")
    source_entity = required("SOURCE_ENTITY")
    id_field = required("API_ID_FIELD")
    token = os.getenv("API_TOKEN")
    header = os.getenv("API_TOKEN_HEADER", "Authorization")
    prefix = os.getenv("API_TOKEN_PREFIX", "Bearer")
    headers = {"Accept": "application/json"}
    if token:
        headers[header] = f"{prefix} {token}".strip()

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    records = records_from_response(response.json())
    batch_id = str(uuid.uuid4())
    connection = database_connection()
    try:
        with connection.cursor() as cursor:
            for record in records:
                source_record_id = record.get(id_field)
                if source_record_id in (None, ""):
                    raise ValueError(
                        f"Record is missing configured identifier field '{id_field}'."
                    )
                cursor.execute(
                    """
                    INSERT INTO raw.api_records (
                        batch_id, source_system, source_entity, source_record_id,
                        extracted_at, payload, ingestion_status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        batch_id,
                        source_system,
                        source_entity,
                        str(source_record_id),
                        datetime.now(timezone.utc),
                        psycopg2.extras.Json(record),
                        "SUCCESS",
                    ),
                )
        connection.commit()
    except (psycopg2.Error, ValueError, KeyError) as error:
        connection.rollback()
        raise RuntimeError(f"Generic ingestion failed: {error}") from error
    finally:
        connection.close()
    return batch_id


if __name__ == "__main__":
    print(f"Ingested generic API batch {ingest()}")
