from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from neo4j import GraphDatabase

from config import load_project_env
from upsert import (
    CONSTRAINTS,
    CYPHER_ENRICH_FACILITY_NAMES,
    CYPHER_ENRICH_PARTNER_NAMES,
    CYPHER_ENRICH_ROUTE_NAMES,
    CYPHER_UPSERT_CARRIERS,
    CYPHER_UPSERT_EVENTS,
    CYPHER_UPSERT_EVENT_RELS,
    CYPHER_UPSERT_FACILITIES,
    CYPHER_UPSERT_ORDERS,
    CYPHER_UPSERT_PARTNERS,
    CYPHER_UPSERT_ROUTES,
    CYPHER_UPSERT_SHIPMENT_RELS,
    CYPHER_UPSERT_SHIPMENTS,
    CYPHER_UPSERT_STAGE_BASELINES,
    CYPHER_WIPE_TIMELINE_FOR_ORDERS,
    REAL_CROSSDOCK_NAMES,
    REAL_DC_NAMES,
    REAL_PARTNER_NAMES,
    REAL_PORT_NAMES,
    chunked,
)

load_project_env()

SHIPPO_API_URL = "https://api.goshippo.com"


def ensure_env() -> None:
    if not os.getenv("SHIPPO_API_TOKEN"):
        raise RuntimeError("Set SHIPPO_API_TOKEN before running the graph pipeline.")
    if not os.getenv("NEO4J_URI") or not os.getenv("NEO4J_USERNAME") or not os.getenv("NEO4J_PASSWORD"):
        raise RuntimeError("Set NEO4J_URI, NEO4J_USERNAME, and NEO4J_PASSWORD before running the graph pipeline.")


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("The request JSON must contain an object at the top level.")
    return payload


def shippo_response_payload(response: requests.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError:
        return {"raw_text": response.text}
    if isinstance(payload, dict):
        return payload
    return {"response": payload}


def neo4j_driver():
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USERNAME")
    pwd = os.getenv("NEO4J_PASSWORD")
    return GraphDatabase.driver(uri, auth=(user, pwd))


def _slug(value: Any) -> str:
    text = str(value or "").strip()
    filtered = "".join(ch if ch.isalnum() else "_" for ch in text)
    return filtered.strip("_").lower() or "node"


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _canonicalize_shippo_payload(payload: Dict[str, Any], *, batch_id: str) -> Dict[str, Any]:
    object_id = payload.get("object_id") or payload.get("id") or f"SHIP_{uuid.uuid4().hex[:12]}"
    rates = payload.get("rates") or []
    selected_rate = rates[0] if rates else {}
    address_from = payload.get("address_from") or {}
    address_to = payload.get("address_to") or {}
    shipment_date = payload.get("shipment_date") or "1970-01-01T00:00:00Z"
    order_id = payload.get("order_id") or payload.get("reference") or f"ORDER::{object_id}"
    facility_id = payload.get("facility_id") or f"FACILITY::{address_from.get('object_id') or 'unknown'}"
    carrier_name = selected_rate.get("provider") or "UNKNOWN_CARRIER"
    carrier_id = f"CARRIER::{_slug(carrier_name)}"
    partner_name = carrier_name
    partner_id = f"PARTNER::{_slug(partner_name)}"
    origin_id = address_from.get("object_id") or facility_id
    dest_id = address_to.get("object_id") or order_id
    route_id = f"{origin_id}__{dest_id}"
    event_type = "SHIPMENT_CREATED"
    event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{object_id}|{event_type}|{shipment_date}"))

    order = {
        "batch_id": batch_id,
        "order_id": order_id,
        "erp_source": "SHIPPO_API",
        "customer_tier": "STANDARD",
        "order_value_usd": _safe_float(payload.get("amount") or selected_rate.get("amount_local") or 0.0),
        "original_promise_dt": shipment_date[:10],
        "target_delivery_dt": shipment_date[:10],
        "estimated_delivery_dt": shipment_date[:10],
        "order_priority": 50,
        "sla_health": "UNKNOWN",
    }

    shipment = {
        "batch_id": batch_id,
        "shipment_id": object_id,
        "order_id": order_id,
        "carrier_id": carrier_id,
        "facility_id": facility_id,
        "route_id": route_id,
        "partner_id": partner_id,
        "tracking_number": payload.get("tracking_number") or f"shp_{uuid.uuid4().hex[:12]}",
        "dwell_time_sigma": 1.0,
        "arrival_eta": shipment_date,
    }

    geo = address_from.get("geo") or {}
    facility = {
        "batch_id": batch_id,
        "facility_id": facility_id,
        "facility_type": "SHIPPO_ORIGIN",
        "region_id": address_from.get("state") or "REGION_UNKNOWN",
        "geo_lat": geo.get("latitude") or address_from.get("latitude"),
        "geo_lon": geo.get("longitude") or address_from.get("longitude"),
        "throughput_z": 0.0,
        "status_flag": "ACTIVE",
    }

    carrier = {
        "batch_id": batch_id,
        "carrier_id": carrier_id,
        "carrier_name": carrier_name,
        "reliability_idx": _safe_float(selected_rate.get("provider") and 0.92),
        "cost_volatility": 0.1,
    }

    partner = {
        "batch_id": batch_id,
        "partner_id": partner_id,
        "partner_name": partner_name,
        "partner_display_name": partner_name,
        "service_zone_id": selected_rate.get("zone") or "ZONE_UNKNOWN",
        "capacity_z": 0.8,
        "success_rate_z": 0.9,
    }

    route = {
        "batch_id": batch_id,
        "route_id": route_id,
        "origin_id": origin_id,
        "dest_id": dest_id,
        "risk_score": _safe_float(selected_rate.get("risk_score"), 0.1),
    }

    event = {
        "batch_id": batch_id,
        "event_id": event_id,
        "shipment_id": object_id,
        "order_id": order_id,
        "event_type": event_type,
        "timestamp_iso": shipment_date,
        "audit_hash": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{object_id}|{event_type}")),
        "delta_t_hrs": 0.0,
        "location_id": origin_id,
        "location_kind": "FACILITY",
        "exception_reason": None,
    }

    return {
        "orders": order,
        "shipments": shipment,
        "facilities": facility,
        "carriers": carrier,
        "partners": partner,
        "routes": route,
        "events": event,
    }


def _load_shippo_batch(request_path: Path, token: str, *, count: int = 1) -> List[Dict[str, Any]]:
    template = load_json(request_path)
    batches: List[Dict[str, Any]] = []
    for _ in range(count):
        payload = json.loads(json.dumps(template))
        payload["reference"] = f"GRAPH-OTC-{uuid.uuid4().hex[:12].upper()}"
        response = requests.post(
            f"{SHIPPO_API_URL}/shipments/",
            headers={
                "Authorization": f"ShippoToken {token}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=30,
        )
        response_payload = shippo_response_payload(response)
        if not response.ok:
            raise RuntimeError(f"Shippo shipment failed: {response_payload}")
        shipment_id = response_payload.get("object_id")
        if not shipment_id:
            raise RuntimeError("Shippo shipment response missing object_id.")
        rates = response_payload.get("rates") or []
        if not rates:
            raise RuntimeError("Shippo shipment response missing rates.")
        transaction_response = requests.post(
            f"{SHIPPO_API_URL}/transactions/",
            headers={
                "Authorization": f"ShippoToken {token}",
                "Content-Type": "application/json",
            },
            json={
                "rate": rates[0]["object_id"],
                "label_file_type": "PDF",
                "async": False,
            },
            timeout=30,
        )
        transaction_payload = shippo_response_payload(transaction_response)
        if not transaction_response.ok:
            raise RuntimeError(f"Shippo transaction failed: {transaction_payload}")

        batch_id = str(uuid.uuid4())
        batches.append({
            "batch_id": batch_id,
            "shipment": response_payload,
            "transaction": transaction_payload,
        })
    return batches


def _run_graph_upserts(session: Any, batch_data: List[Dict[str, Any]], now: str) -> Dict[str, int]:
    order_rows: List[Dict[str, Any]] = []
    shipment_rows: List[Dict[str, Any]] = []
    facility_rows: List[Dict[str, Any]] = []
    carrier_rows: List[Dict[str, Any]] = []
    partner_rows: List[Dict[str, Any]] = []
    route_rows: List[Dict[str, Any]] = []
    event_rows: List[Dict[str, Any]] = []

    for item in batch_data:
        converted = _canonicalize_shippo_payload(item["shipment"], batch_id=item["batch_id"])
        order_rows.append(converted["orders"])
        shipment_rows.append(converted["shipments"])
        facility_rows.append(converted["facilities"])
        carrier_rows.append(converted["carriers"])
        partner_rows.append(converted["partners"])
        route_rows.append(converted["routes"])
        event_rows.append(converted["events"])

    for statement in CONSTRAINTS:
        session.run(statement)

    def upsert_rows(query: str, rows: List[Dict[str, Any]], label: str) -> None:
        if not rows:
            return
        for chunk in chunked(rows, 1000):
            session.run(query, rows=chunk, now=now)
        print(f"✅ upserted {label}: {len(rows)}")

    upsert_rows(CYPHER_UPSERT_FACILITIES, facility_rows, "Facility")
    upsert_rows(CYPHER_UPSERT_CARRIERS, carrier_rows, "Carrier")
    upsert_rows(CYPHER_UPSERT_PARTNERS, partner_rows, "DeliveryPartner")
    upsert_rows(CYPHER_UPSERT_ROUTES, route_rows, "Route")
    upsert_rows(CYPHER_UPSERT_ORDERS, order_rows, "Order")
    upsert_rows(CYPHER_UPSERT_SHIPMENTS, shipment_rows, "Shipment")
    upsert_rows(CYPHER_UPSERT_SHIPMENT_RELS, shipment_rows, "Shipment relationships")

    order_ids = sorted({row.get("order_id") for row in order_rows if row.get("order_id")})
    if order_ids:
        session.run(CYPHER_WIPE_TIMELINE_FOR_ORDERS, order_ids=order_ids).consume()

    upsert_rows(CYPHER_UPSERT_EVENTS, event_rows, "Event")
    upsert_rows(CYPHER_UPSERT_EVENT_RELS, event_rows, "Event relationships")

    session.run(
        CYPHER_ENRICH_FACILITY_NAMES,
        bid="",
        dc_names=REAL_DC_NAMES,
        port_names=REAL_PORT_NAMES,
        crossdock_names=REAL_CROSSDOCK_NAMES,
    )
    session.run(CYPHER_ENRICH_PARTNER_NAMES, bid="", partner_names=REAL_PARTNER_NAMES)
    session.run(CYPHER_ENRICH_ROUTE_NAMES, bid="")

    rec = session.run(CYPHER_UPSERT_STAGE_BASELINES, now=now).single()
    if rec:
        print(f"✅ StageBaseline upserted: {int(rec.get('n_baselines') or 0)} groups")

    return {
        "orders": len(order_rows),
        "shipments": len(shipment_rows),
        "facilities": len(facility_rows),
        "carriers": len(carrier_rows),
        "partners": len(partner_rows),
        "routes": len(route_rows),
        "events": len(event_rows),
    }


def ingest_shippo_to_graph(request_path: Path, *, count: int = 1) -> Dict[str, int]:
    ensure_env()
    token = os.getenv("SHIPPO_API_TOKEN")
    batch_data = _load_shippo_batch(request_path, token or "", count=count)
    driver = neo4j_driver()
    now = datetime.now(timezone.utc).isoformat()
    try:
        with driver.session() as session:
            counts = _run_graph_upserts(session, batch_data, now)
    finally:
        driver.close()
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Graph-native Shippo ingestion that pushes payloads directly to Neo4j.")
    parser.add_argument("request_file", type=Path, help="JSON request payload used to create Shippo shipments")
    parser.add_argument("--count", type=int, default=1, help="Number of shipments to ingest")
    args = parser.parse_args()

    if args.count < 1:
        raise ValueError("count must be at least 1")

    counts = ingest_shippo_to_graph(args.request_file, count=args.count)
    print("Graph-native ingestion complete")
    print(counts)


if __name__ == "__main__":
    main()
