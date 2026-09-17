"""Generic REST ingestion for any API source using endpoint/token/mapping config."""

from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
import requests
from config import load_project_env
from ingestion.object_storage import minio_enabled, upload_landing_records

load_project_env()


def required(name: str, *, config: dict[str, Any] | None = None) -> str:
    value = (config or {}).get(name) if config else None
    if value is None:
        value = os.getenv(name)
    if value is None or str(value).strip() == "":
        raise RuntimeError(f"Set {name} before running generic ingestion.")
    return str(value)


def value_at(payload: Any, path: str) -> Any:
    if not path:
        return payload
    current = payload
    for part in filter(None, str(path).split(".")):
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list):
            if part.isdigit():
                idx = int(part)
                current = current[idx] if idx < len(current) else None
            else:
                return None
        else:
            return None
    return current


def load_source_config(path: str | None = None) -> dict[str, Any]:
    config_path = path or os.getenv("API_CONFIG_PATH")
    if not config_path:
        return {}
    with Path(config_path).open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    if not isinstance(config, dict):
        raise ValueError("The source config must be a JSON object.")
    return config


def records_from_response(payload: Any, *, response_path: str | None = None) -> list[dict[str, Any]]:
    records = value_at(payload, response_path) if response_path else payload
    if records is None:
        raise ValueError("The configured response path did not resolve to any data.")
    if isinstance(records, dict):
        # Common response wrapper: {data: [...]} or {items: [...]} or {results: [...]}
        if "data" in records and isinstance(records["data"], list):
            records = records["data"]
        elif "items" in records and isinstance(records["items"], list):
            records = records["items"]
        elif "results" in records and isinstance(records["results"], list):
            records = records["results"]
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


def build_headers(config: dict[str, Any]) -> dict[str, str]:
    headers: dict[str, str] = {"Accept": "application/json"}
    auth = config.get("auth") or {}
    if isinstance(auth, dict):
        auth_type = str(auth.get("type", "bearer")).lower()
        token = auth.get("token") or os.getenv("API_TOKEN") or os.getenv("TOKEN")
        if token:
            header = str(auth.get("header") or os.getenv("API_TOKEN_HEADER", "Authorization"))
            prefix = str(auth.get("prefix") or os.getenv("API_TOKEN_PREFIX", "Bearer")).strip()
            if auth_type == "basic":
                headers[header] = "Basic " + token
            else:
                headers[header] = f"{prefix} {token}".strip()
    return headers


def fetch_records(config: dict[str, Any]) -> list[dict[str, Any]]:
    method = str((config.get("method") or "GET")).upper()
    url = required("endpoint", config=config)
    response = requests.request(
        method,
        url,
        headers=build_headers(config),
        params=config.get("params") or {},
        json=config.get("body") or None,
        timeout=30,
    )
    response.raise_for_status()
    return records_from_response(response.json(), response_path=(config.get("response") or {}).get("records_path"))


def ingest_from_config(
    config_path: str | None = None,
    *,
    config: dict[str, Any] | None = None,
    max_records: int | None = None,
) -> str:
    config = config or load_source_config(config_path)
    if not config:
        config = {
            "endpoint": os.getenv("API_URL"),
            "source_system": os.getenv("SOURCE_SYSTEM"),
            "source_entity": os.getenv("SOURCE_ENTITY"),
            "record_id_field": os.getenv("API_ID_FIELD"),
            "response": {"records_path": os.getenv("API_RECORDS_PATH", "")},
            "auth": {
                "type": os.getenv("API_AUTH_TYPE", "bearer"),
                "token": os.getenv("API_TOKEN"),
                "header": os.getenv("API_TOKEN_HEADER", "Authorization"),
                "prefix": os.getenv("API_TOKEN_PREFIX", "Bearer"),
            },
        }

    endpoint = required("endpoint", config=config)
    source_system = required("source_system", config=config)
    source_entity = required("source_entity", config=config)
    id_field = required("record_id_field", config=config)
    records = fetch_records(config)
    if max_records is not None:
        if max_records < 1:
            raise ValueError("max_records must be at least 1.")
        records = records[:max_records]
    batch_id = str(uuid.uuid4())
    extracted_at = datetime.now(timezone.utc)
    landing_records: list[dict[str, Any]] = []
    for record in records:
        source_record_id = record.get(id_field)
        if source_record_id in (None, ""):
            raise ValueError(f"Record is missing configured identifier field '{id_field}'.")
        landing_records.append(
            {
                "source_record_id": str(source_record_id),
                "extracted_at": extracted_at.isoformat(),
                "payload": record,
                "ingestion_status": "SUCCESS",
                "error_message": None,
            }
        )
    if minio_enabled():
        upload_landing_records(
            batch_id=batch_id,
            source_system=source_system,
            source_entity=source_entity,
            records=landing_records,
        )
        return batch_id
    connection = database_connection()
    try:
        with connection.cursor() as cursor:
            for record in records:
                source_record_id = record.get(id_field)
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest any REST API into the generic raw Postgres layer.")
    parser.add_argument("--config", default=None, help="Path to a JSON config containing endpoint, auth, and mapping config.")
    args = parser.parse_args()
    batch_id = ingest_from_config(args.config)
    print(f"Ingested generic API batch {batch_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
