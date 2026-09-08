"""Ingest Odoo Community stock pickings into the shared raw layer."""

from __future__ import annotations

import os
import uuid
import xmlrpc.client
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Set {name} before running Odoo ingestion.")
    return value


def _db_connection():
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


def _odoo_records(
    models: Any,
    db: str,
    uid: int,
    password: str,
    model: str,
    fields: list[str],
) -> list[dict[str, Any]]:
    return models.execute_kw(
        db,
        uid,
        password,
        model,
        "search_read",
        [[]],
        {"fields": fields, "limit": int(os.getenv("ODOO_LIMIT", "100"))},
    )


def ingest_odoo() -> str:
    url = _required("ODOO_URL").rstrip("/")
    db = _required("ODOO_DB")
    username = _required("ODOO_USERNAME")
    password = _required("ODOO_PASSWORD")
    common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common", allow_none=True)
    uid = common.authenticate(db, username, password, {})
    if not uid:
        raise RuntimeError("Odoo authentication failed.")

    models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object", allow_none=True)
    pickings = _odoo_records(
        models,
        db,
        uid,
        password,
        "stock.picking",
        [
            "id",
            "name",
            "origin",
            "partner_id",
            "scheduled_date",
            "date_done",
            "state",
            "carrier_id",
            "carrier_tracking_ref",
            "picking_type_id",
            "write_date",
        ],
    )
    sales_orders = _odoo_records(
        models,
        db,
        uid,
        password,
        "sale.order",
        ["name", "amount_total", "currency_id", "commitment_date"],
    )
    sales_by_name = {order["name"]: order for order in sales_orders}
    batch_id = str(uuid.uuid4())
    connection = _db_connection()
    try:
        with connection.cursor() as cursor:
            for picking in pickings:
                partner = picking.get("partner_id") or [None, None]
                carrier = picking.get("carrier_id") or [None, None]
                warehouse = picking.get("picking_type_id") or [None, None]
                sales_order = sales_by_name.get(picking.get("origin"), {})
                currency = sales_order.get("currency_id") or [None, "USD"]
                payload = {
                    "shipment_id": picking["name"],
                    "order_id": picking.get("origin") or picking["name"],
                    "customer_id": partner[0],
                    "customer_name": partner[1],
                    "carrier": carrier[1],
                    "tracking_number": picking.get("carrier_tracking_ref"),
                    "warehouse_id": warehouse[0],
                    "order_value": str(sales_order.get("amount_total", 0)),
                    "currency": currency[1],
                    "order_status": picking.get("state"),
                    "promise_date": (
                        sales_order.get("commitment_date")
                        or picking.get("scheduled_date")
                    ),
                    "target_date": picking.get("scheduled_date"),
                    "updated_at": picking.get("write_date"),
                    "erp_system": "ODOO",
                    "odoo_record_id": picking["id"],
                    "date_done": picking.get("date_done"),
                }
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
                        "odoo",
                        "orders",
                        str(picking["id"]),
                        datetime.now(timezone.utc),
                        psycopg2.extras.Json(payload),
                        "SUCCESS",
                    ),
                )
        connection.commit()
    except (psycopg2.Error, KeyError) as error:
        connection.rollback()
        raise RuntimeError(f"Odoo ingestion failed: {error}") from error
    finally:
        connection.close()
    return batch_id


if __name__ == "__main__":
    print(f"Ingested Odoo batch {ingest_odoo()}")
