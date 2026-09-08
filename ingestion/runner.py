"""Ingest orders from the ERP-style API into the shared raw layer."""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv("ERP_API_URL", "http://localhost:8000/orders")


def get_db_connection():
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


def ingest_orders() -> None:
    batch_id = str(uuid.uuid4())
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    orders: Any = response.json()
    if not isinstance(orders, list):
        raise ValueError("ERP API response must be a list of orders.")

    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            for order in orders:
                if not isinstance(order, dict) or not order.get("order_id"):
                    raise ValueError("Every ERP order must contain order_id.")
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
                        order.get("erp_system", "erp").lower(),
                        "orders",
                        order["order_id"],
                        datetime.now(timezone.utc),
                        psycopg2.extras.Json(order),
                        "SUCCESS",
                    ),
                )
        connection.commit()
    except (psycopg2.Error, ValueError, KeyError) as error:
        connection.rollback()
        raise RuntimeError(f"ERP ingestion failed: {error}") from error
    finally:
        connection.close()

    print(f"Ingested {len(orders)} ERP orders in batch {batch_id}")


if __name__ == "__main__":
    ingest_orders()
