from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

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


def _slug(value: Any) -> str:
    text = str(value or "").strip()
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_").lower() or "node"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_shippo_payload_to_graph(payload: Dict[str, Any], *, batch_id: str) -> Dict[str, Dict[str, Any]]:
    """Build the graph-native canonical entity mapping from a raw Shippo shipment payload."""
    run_ns = batch_id or uuid.uuid4().hex[:12]
    object_id = payload.get("object_id") or payload.get("id") or f"SHIP_{uuid.uuid4().hex[:12]}"
    source_order_id = payload.get("order_id") or payload.get("reference") or f"ORDER::{object_id}"
    order_id = f"ORDER::{run_ns}::{source_order_id}"
    rates = payload.get("rates") or []
    selected_rate = rates[0] if rates else {}
    address_from = payload.get("address_from") or {}
    address_to = payload.get("address_to") or {}
    shipment_date = payload.get("shipment_date") or "1970-01-01T00:00:00Z"
    facility_id = payload.get("facility_id") or f"FACILITY::{run_ns}::{address_from.get('object_id') or 'unknown'}"
    carrier_name = selected_rate.get("provider") or "UNKNOWN_CARRIER"
    carrier_id = f"CARRIER::{run_ns}::{_slug(carrier_name)}"
    partner_name = carrier_name
    partner_id = f"PARTNER::{run_ns}::{_slug(partner_name)}"
    origin_id = address_from.get("object_id") or facility_id
    dest_id = address_to.get("object_id") or order_id
    route_id = f"{origin_id}__{dest_id}__{run_ns}"
    event_type = "SHIPMENT_CREATED"
    event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{object_id}|{event_type}|{shipment_date}|{run_ns}"))
    shipment_id = f"SHIPMENT::{run_ns}::{object_id}"

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
        "shipment_id": shipment_id,
        "order_id": order_id,
        "carrier_id": carrier_id,
        "facility_id": facility_id,
        "route_id": route_id,
        "partner_id": partner_id,
        "tracking_number": payload.get("tracking_number") or f"shp_{uuid.uuid4().hex[:12]}",
        "dwell_time_sigma": 1.0,
        "arrival_eta": shipment_date,
    }

    facility_geo = (payload.get("facility_geo") or {})
    facility = {
        "batch_id": batch_id,
        "facility_id": facility_id,
        "facility_type": "SHIPPO_ORIGIN",
        "region_id": address_from.get("state") or "REGION_UNKNOWN",
        "geo_lat": facility_geo.get("lat") or address_from.get("latitude"),
        "geo_lon": facility_geo.get("lon") or address_from.get("longitude"),
        "throughput_z": 0.0,
        "status_flag": "ACTIVE",
    }

    carrier = {
        "batch_id": batch_id,
        "carrier_id": carrier_id,
        "carrier_name": carrier_name,
        "reliability_idx": 0.92,
        "cost_volatility": 0.10,
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


def graph_driver():
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    if not uri or not user or not password:
        raise RuntimeError("Set NEO4J_URI, NEO4J_USERNAME, and NEO4J_PASSWORD before upserting graph entities.")
    return GraphDatabase.driver(uri, auth=(user, password))


def upsert_graph_entities(graph_rows: Iterable[Dict[str, Dict[str, Any]]], *, batch_id: str) -> Dict[str, int]:
    rows = list(graph_rows)
    if not rows:
        return {"orders": 0, "shipments": 0, "facilities": 0, "carriers": 0, "partners": 0, "routes": 0, "events": 0}

    order_rows: List[Dict[str, Any]] = []
    shipment_rows: List[Dict[str, Any]] = []
    facility_rows: List[Dict[str, Any]] = []
    carrier_rows: List[Dict[str, Any]] = []
    partner_rows: List[Dict[str, Any]] = []
    route_rows: List[Dict[str, Any]] = []
    event_rows: List[Dict[str, Any]] = []

    for row in rows:
        order_rows.append(row["orders"])
        shipment_rows.append(row["shipments"])
        facility_rows.append(row["facilities"])
        carrier_rows.append(row["carriers"])
        partner_rows.append(row["partners"])
        route_rows.append(row["routes"])
        event_rows.append(row["events"])

    driver = graph_driver()
    now = datetime.now(timezone.utc).isoformat()
    try:
        with driver.session() as session:
            for statement in CONSTRAINTS:
                session.run(statement)

            def upsert_rows(query: str, rows_to_write: List[Dict[str, Any]], label: str) -> None:
                if not rows_to_write:
                    return
                for chunk in chunked(rows_to_write, 1000):
                    session.run(query, rows=chunk, now=now)
                print(f"✅ upserted {label}: {len(rows_to_write)}")

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

            session.run(CYPHER_ENRICH_FACILITY_NAMES, bid=batch_id, dc_names=REAL_DC_NAMES, port_names=REAL_PORT_NAMES, crossdock_names=REAL_CROSSDOCK_NAMES)
            session.run(CYPHER_ENRICH_PARTNER_NAMES, bid=batch_id, partner_names=REAL_PARTNER_NAMES)
            session.run(CYPHER_ENRICH_ROUTE_NAMES, bid=batch_id)

            rec = session.run(CYPHER_UPSERT_STAGE_BASELINES, now=now).single()
            if rec:
                print(f"✅ StageBaseline upserted: {int(rec.get('n_baselines') or 0)} groups")
    finally:
        driver.close()

    return {
        "orders": len(order_rows),
        "shipments": len(shipment_rows),
        "facilities": len(facility_rows),
        "carriers": len(carrier_rows),
        "partners": len(partner_rows),
        "routes": len(route_rows),
        "events": len(event_rows),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize a raw Shippo payload into graph-native canonical nodes and edges, then upsert to Neo4j.")
    parser.add_argument("request_file", type=Path, help="Path to the request JSON to send to Shippo")
    parser.add_argument("--count", type=int, default=1, help="Number of Shippo shipments to process")
    args = parser.parse_args()

    if args.count < 1:
        raise ValueError("count must be at least 1")

    token = os.getenv("SHIPPO_API_TOKEN")
    if not token:
        raise RuntimeError("Set SHIPPO_API_TOKEN before running graph normalization.")

    payloads: List[Dict[str, Any]] = []
    with open(args.request_file, "r", encoding="utf-8") as handle:
        seed = json.load(handle)

    for idx in range(args.count):
        request = json.loads(json.dumps(seed))
        request["reference"] = f"GRAPH-NORM-{uuid.uuid4().hex[:12].upper()}"
        response = __import__("requests").post(
            f"https://api.goshippo.com/shipments/",
            headers={
                "Authorization": f"ShippoToken {token}",
                "Content-Type": "application/json",
            },
            json=request,
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(f"Shippo ingestion failed: {response.text}")
        payload = response.json()
        payloads.append({"batch_id": str(uuid.uuid4()), "payload": payload})

    normalized = [normalize_shippo_payload_to_graph(item["payload"], batch_id=item["batch_id"]) for item in payloads]
    counts = upsert_graph_entities(normalized, batch_id=payloads[0]["batch_id"] if payloads else "")
    print("Graph canonical normalization complete")
    print(counts)


if __name__ == "__main__":
    main()
