import os
import uuid
from datetime import datetime, timezone

import requests
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

API_URL = "http://localhost:8000/orders"


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def ingest_orders():
    # Create a unique ID for this ingestion run
    batch_id = str(uuid.uuid4())

    # Get data from the API
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()

    orders = response.json()

    print(f"Received {len(orders)} orders from API")

    # Connect to PostgreSQL
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        for order in orders:
            cursor.execute(
                """
                INSERT INTO raw.api_records (
                    batch_id,
                    source_system,
                    source_entity,
                    source_record_id,
                    extracted_at,
                    payload,
                    ingestion_status
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    batch_id,
                    "synthetic_otc_api",
                    "orders",
                    order["order_id"],
                    datetime.now(timezone.utc),
                    psycopg2.extras.Json(order),
                    "SUCCESS",
                ),
            )

        connection.commit()

        print("Ingestion successful!")
        print(f"Batch ID: {batch_id}")
        print(f"Records ingested: {len(orders)}")

    except Exception as e:
        connection.rollback()
        print(f"Ingestion failed: {e}")

    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    ingest_orders()