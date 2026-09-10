from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from dotenv import load_dotenv
from neo4j import GraphDatabase


# ----------------------------
# Realistic display name pools (KG-only enrichment; raw data unchanged)
# ----------------------------
REAL_DC_NAMES = [
    "Chicago Distribution Center",
    "Dallas Distribution Center",
    "Atlanta Distribution Center",
    "New Jersey Distribution Center",
    "Phoenix Distribution Center",
    "Seattle Distribution Center",
    "Columbus Distribution Center",
    "Los Angeles Distribution Center",
    "Denver Distribution Center",
    "Miami Distribution Center",
]

REAL_PORT_NAMES = [
    "Port of Los Angeles",
    "Port of Long Beach",
    "Port of Houston",
    "Port of Savannah",
    "Port of Newark",
    "Port of Oakland",
    "Port of Seattle-Tacoma",
    "Port of Charleston",
    "Port of New York & New Jersey",
]

REAL_CROSSDOCK_NAMES = [
    "Memphis CrossDock Hub",
    "Kansas City CrossDock Hub",
    "Nashville CrossDock Hub",
    "Indianapolis CrossDock Hub",
    "Reno CrossDock Hub",
    "St. Louis CrossDock Hub",
    "Salt Lake City CrossDock Hub",
]

REAL_PARTNER_NAMES = [
    "OnTrac Logistics",
    "LaserShip",
    "Veho",
    "Roadie",
    "SpeedX",
    "GSO Logistics",
    "LSO (Lone Star Overnight)",
    "UniUni",
    "AxleHire",
    "Deliver-It",
]


# ----------------------------
# ENV loading (repo-friendly)
# ----------------------------
def load_env_auto() -> None:
    p = Path(__file__).resolve()
    for parent in [p.parent] + list(p.parents):
        env_path = parent / ".env"
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)
            return
    load_dotenv()


load_env_auto()


def req_env(name: str) -> str:
    v = (os.getenv(name) or "").strip()
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ----------------------------
# IO
# ----------------------------
def read_jsonl(path: Path, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if limit is not None and i >= limit:
                break
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def chunked(
    rows: List[Dict[str, Any]], batch_size: int
) -> Iterable[List[Dict[str, Any]]]:
    for i in range(0, len(rows), batch_size):
        yield rows[i : i + batch_size]


# ----------------------------
# Neo4j
# ----------------------------
def neo4j_driver():
    uri = req_env("NEO4J_URI")
    user = req_env("NEO4J_USERNAME")
    pwd = req_env("NEO4J_PASSWORD")
    return GraphDatabase.driver(uri, auth=(user, pwd))


CONSTRAINTS = [
    "CREATE CONSTRAINT order_id IF NOT EXISTS FOR (n:Order) REQUIRE n.order_id IS UNIQUE",
    "CREATE CONSTRAINT shipment_id IF NOT EXISTS FOR (n:Shipment) REQUIRE n.shipment_id IS UNIQUE",
    "CREATE CONSTRAINT event_id IF NOT EXISTS FOR (n:Event) REQUIRE n.event_id IS UNIQUE",
    "CREATE CONSTRAINT facility_id IF NOT EXISTS FOR (n:Facility) REQUIRE n.facility_id IS UNIQUE",
    "CREATE CONSTRAINT carrier_id IF NOT EXISTS FOR (n:Carrier) REQUIRE n.carrier_id IS UNIQUE",
    "CREATE CONSTRAINT route_id IF NOT EXISTS FOR (n:Route) REQUIRE n.route_id IS UNIQUE",
    "CREATE CONSTRAINT partner_id IF NOT EXISTS FOR (n:DeliveryPartner) REQUIRE n.partner_id IS UNIQUE",
    "CREATE CONSTRAINT region_id IF NOT EXISTS FOR (n:Region) REQUIRE n.region_id IS UNIQUE",
    "CREATE CONSTRAINT stage_baseline_key IF NOT EXISTS FOR (b:StageBaseline) REQUIRE b.group_key IS UNIQUE",
]

# --- Relationship mapping ---
# FULFILLED_BY:   (Order)-[:FULFILLED_BY]->(Shipment)
# DEPARTS_FROM:   (Shipment)-[:DEPARTS_FROM]->(Facility)
# HANDED_OFF_TO:  (Shipment)-[:HANDED_OFF_TO]->(Carrier)
# LOGGED_FOR:     (Event)-[:LOGGED_FOR]->(Shipment)
# SERVICES:       (DeliveryPartner)-[:SERVICES]->(Region)
# IMPACTS:        (Region)-[:IMPACTS]->(Route)

CYPHER_UPSERT_FACILITIES = """
UNWIND $rows AS row
MERGE (f:Facility {facility_id: row.facility_id})
SET
  f.batch_id = row.batch_id,
  f.facility_type = row.facility_type,
  f.facility_name = coalesce(row.facility_name, f.facility_name),
  f.region_id = row.region_id,
  f.geo_lat = row.geo_lat,
  f.geo_lon = row.geo_lon,
  f.throughput_z = row.throughput_z,
  f.status_flag = row.status_flag,
  f.updated_at = $now
WITH f, row
MERGE (r:Region {region_id: row.region_id})
SET
  r.batch_id = row.batch_id,
  r.updated_at = $now
MERGE (f)-[:IN_REGION]->(r)
"""

CYPHER_UPSERT_CARRIERS = """
UNWIND $rows AS row
MERGE (c:Carrier {carrier_id: row.carrier_id})
SET
  c.batch_id = row.batch_id,
  c.carrier_name = row.carrier_name,
  c.reliability_idx = row.reliability_idx,
  c.cost_volatility = row.cost_volatility,
  c.updated_at = $now
"""

CYPHER_UPSERT_PARTNERS = """
UNWIND $rows AS row
MERGE (p:DeliveryPartner {partner_id: row.partner_id})
SET
  p.batch_id = row.batch_id,
  p.partner_name = row.partner_name,
  p.partner_display_name = coalesce(row.partner_display_name, p.partner_display_name),
  p.service_zone_id = row.service_zone_id,
  p.capacity_z = row.capacity_z,
  p.success_rate_z = row.success_rate_z,
  p.updated_at = $now
WITH p, row
MERGE (r:Region {region_id: row.service_zone_id})
SET
  r.batch_id = row.batch_id,
  r.updated_at = $now
MERGE (p)-[:SERVICES]->(r)
"""

CYPHER_UPSERT_ROUTES = """
UNWIND $rows AS row
MERGE (rt:Route {route_id: row.route_id})
SET
  rt.batch_id = row.batch_id,
  rt.origin_id = row.origin_id,
  rt.dest_id = row.dest_id,
  rt.route_name = coalesce(row.route_name, rt.route_name),
  rt.risk_score = row.risk_score,
  rt.updated_at = $now
WITH rt, row
// IMPACTS: Region -> Route, derived from destination Facility's region_id
OPTIONAL MATCH (fd:Facility {facility_id: row.dest_id})
FOREACH (_ IN CASE WHEN fd IS NULL OR fd.region_id IS NULL THEN [] ELSE [1] END |
  MERGE (r:Region {region_id: fd.region_id})
  SET
    r.batch_id = row.batch_id,
    r.updated_at = $now
  MERGE (r)-[:IMPACTS]->(rt)
)
"""

CYPHER_UPSERT_ORDERS = """
UNWIND $rows AS row
MERGE (o:Order {order_id: row.order_id})
SET
  o.batch_id = row.batch_id,
  o.erp_source = row.erp_source,
  o.customer_tier = row.customer_tier,
  o.order_value_usd = row.order_value_usd,
  o.original_promise_dt = row.original_promise_dt,
  o.target_delivery_dt = row.target_delivery_dt,
  o.estimated_delivery_dt = row.estimated_delivery_dt,
  o.order_priority = row.order_priority,
  o.sla_health = row.sla_health,
  o.updated_at = $now
"""

CYPHER_UPSERT_SHIPMENTS = """
UNWIND $rows AS row
MERGE (s:Shipment {shipment_id: row.shipment_id})
SET
  s.batch_id = row.batch_id,
  s.order_id = row.order_id,
  s.carrier_id = row.carrier_id,
  s.facility_id = row.facility_id,
  s.route_id = row.route_id,
  s.partner_id = row.partner_id,
  s.tracking_number = row.tracking_number,
  s.dwell_time_sigma = row.dwell_time_sigma,
  s.arrival_eta = row.arrival_eta,
  s.updated_at = $now
"""

CYPHER_UPSERT_SHIPMENT_RELS = """
UNWIND $rows AS row
MATCH (s:Shipment {shipment_id: row.shipment_id})
OPTIONAL MATCH (o:Order   {order_id:   row.order_id})
OPTIONAL MATCH (c:Carrier {carrier_id: row.carrier_id})
OPTIONAL MATCH (f:Facility {facility_id: row.facility_id})
OPTIONAL MATCH (rt:Route  {route_id:   row.route_id})
FOREACH (_ IN CASE WHEN o  IS NULL THEN [] ELSE [1] END | MERGE (o)-[:FULFILLED_BY]->(s))
FOREACH (_ IN CASE WHEN c  IS NULL THEN [] ELSE [1] END | MERGE (s)-[:HANDED_OFF_TO]->(c))
FOREACH (_ IN CASE WHEN f  IS NULL THEN [] ELSE [1] END | MERGE (s)-[:DEPARTS_FROM]->(f))
FOREACH (_ IN CASE WHEN rt IS NULL THEN [] ELSE [1] END | MERGE (s)-[:TRAVELS_VIA]->(rt))
"""

CYPHER_UPSERT_EVENTS = """
UNWIND $rows AS row
MERGE (e:Event {event_id: row.event_id})
SET
  e.batch_id = row.batch_id,
  e.shipment_id = row.shipment_id,
  e.order_id = row.order_id,
  e.event_type = row.event_type,
  e.timestamp_iso = row.timestamp_iso,
  e.audit_hash = row.audit_hash,
  e.delta_t_hrs = row.delta_t_hrs,
  e.location_id = row.location_id,
  e.location_kind = row.location_kind,
  e.exception_reason = row.exception_reason,
  e.updated_at = $now
"""

CYPHER_UPSERT_EVENT_RELS = """
UNWIND $rows AS row
MATCH (e:Event {event_id: row.event_id})
MATCH (s:Shipment {shipment_id: row.shipment_id})
MERGE (e)-[:LOGGED_FOR]->(s)
"""

# Wipe prior :Event and :CoCStep nodes for the order_ids we're about to re-ingest,
# so re-upserting an existing order replaces its timeline instead of stacking events.
# Order/Shipment nodes themselves are MERGEd (props overwritten) — only the
# event timeline and derived CoC steps need explicit deletion.
CYPHER_WIPE_TIMELINE_FOR_ORDERS = """
UNWIND $order_ids AS oid
CALL {
  WITH oid
  MATCH (e:Event {order_id: oid})
  DETACH DELETE e
}
CALL {
  WITH oid
  MATCH (cs:CoCStep {order_id: oid})
  DETACH DELETE cs
}
RETURN count(*) AS n
"""


# ----------------------------
# KG-only enrichment (realistic names)
# ----------------------------
CYPHER_ENRICH_FACILITY_NAMES = """
MATCH (f:Facility)
WHERE ($bid = "" OR f.batch_id = $bid)
  AND f.facility_name IS NULL
WITH f,
  CASE f.facility_type
    WHEN "DC" THEN $dc_names
    WHEN "Port" THEN $port_names
    ELSE $crossdock_names
  END AS pool,
  // stable integer from the last 4 digits in ...::F_0001
  abs(toInteger(substring(f.facility_id, size(f.facility_id) - 4, 4))) AS idx
SET f.facility_name = pool[idx % size(pool)]
"""

CYPHER_ENRICH_PARTNER_NAMES = """
MATCH (p:DeliveryPartner)
WHERE ($bid = "" OR p.batch_id = $bid)
  AND p.partner_display_name IS NULL
WITH p,
  $partner_names AS pool,
  // stable integer from the last 4 digits in ...::P_0001
  abs(toInteger(substring(p.partner_id, size(p.partner_id) - 4, 4))) AS idx
SET p.partner_display_name = pool[idx % size(pool)]
"""

CYPHER_ENRICH_ROUTE_NAMES = """
MATCH (rt:Route)
WHERE ($bid = "" OR rt.batch_id = $bid)
  AND rt.route_name IS NULL
OPTIONAL MATCH (fo:Facility {facility_id: rt.origin_id})
OPTIONAL MATCH (fd:Facility {facility_id: rt.dest_id})
SET rt.route_name =
  coalesce(fo.facility_name, rt.origin_id) + " → " + coalesce(fd.facility_name, rt.dest_id)
"""

# Per-stage dwell baseline computed from CLOSED orders (have a DELIVERED event).
# One :StageBaseline node per (event_type, location_kind), refreshed each upsert run
# from the full corpus (not filtered by batch_id) so small batches don't destabilize it.
CYPHER_UPSERT_STAGE_BASELINES = """
MATCH (o:Order)
WHERE EXISTS {
  MATCH (:Event {order_id: o.order_id, event_type: "DELIVERED"})
}
MATCH (e:Event {order_id: o.order_id})
WHERE e.delta_t_hrs IS NOT NULL
  AND e.delta_t_hrs > 0
  AND e.event_type IS NOT NULL
WITH
  e.event_type AS event_type,
  coalesce(e.location_kind, "UNKNOWN") AS location_kind,
  count(e)             AS n,
  avg(e.delta_t_hrs)   AS mean_dwell_h,
  stDev(e.delta_t_hrs) AS std_dwell_h
WITH
  event_type, location_kind, n, mean_dwell_h, std_dwell_h,
  event_type + "|" + location_kind AS group_key
MERGE (b:StageBaseline {group_key: group_key})
SET
  b.event_type    = event_type,
  b.location_kind = location_kind,
  b.mean_dwell_h  = mean_dwell_h,
  b.std_dwell_h   = std_dwell_h,
  b.n             = n,
  b.scope         = "CLOSED_ORDERS",
  b.updated_at    = $now
RETURN count(b) AS n_baselines, sum(n) AS n_events
"""


# ----------------------------
# Normalization
# ----------------------------
def norm_facilities(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        gp = r.get("geo_point") or {}
        out.append(
            {
                "batch_id": r.get("batch_id"),
                "facility_id": r.get("facility_id"),
                "facility_type": r.get("facility_type"),
                "region_id": r.get("region_id"),
                "geo_lat": gp.get("lat"),
                "geo_lon": gp.get("lon"),
                "throughput_z": r.get("throughput_z"),
                "status_flag": r.get("status_flag"),
            }
        )
    return out


def pick(rows: List[Dict[str, Any]], keys: List[str]) -> List[Dict[str, Any]]:
    return [{k: r.get(k) for k in keys} for r in rows]


def filter_batch(rows: List[Dict[str, Any]], batch_id: str) -> List[Dict[str, Any]]:
    if not batch_id:
        return rows
    return [r for r in rows if (r.get("batch_id") == batch_id)]


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_dir", default="data", help="Directory containing *.jsonl")
    ap.add_argument(
        "--batch_id", default="", help="Only ingest this batch_id (recommended)"
    )
    ap.add_argument(
        "--limit", type=int, default=0, help="Max rows per file (0 = no limit)"
    )
    ap.add_argument("--batch_size", type=int, default=1000)
    ap.add_argument(
        "--dry_run", action="store_true", help="No DB writes; show counts + sample"
    )
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    limit = None if args.limit <= 0 else args.limit
    now = utc_now_iso()

    facilities_raw = read_jsonl(data_dir / "facilities.jsonl", limit=limit)
    carriers_raw = read_jsonl(data_dir / "carriers.jsonl", limit=limit)
    partners_raw = read_jsonl(data_dir / "partners.jsonl", limit=limit)
    routes_raw = read_jsonl(data_dir / "routes.jsonl", limit=limit)
    orders_raw = read_jsonl(data_dir / "orders.jsonl", limit=limit)
    shipments_raw = read_jsonl(data_dir / "shipments.jsonl", limit=limit)
    events_raw = read_jsonl(data_dir / "events.jsonl", limit=limit)

    facilities = filter_batch(norm_facilities(facilities_raw), args.batch_id)
    carriers = filter_batch(
        pick(
            carriers_raw,
            [
                "batch_id",
                "carrier_id",
                "carrier_name",
                "reliability_idx",
                "cost_volatility",
            ],
        ),
        args.batch_id,
    )
    partners = filter_batch(
        pick(
            partners_raw,
            [
                "batch_id",
                "partner_id",
                "partner_name",
                "service_zone_id",
                "capacity_z",
                "success_rate_z",
            ],
        ),
        args.batch_id,
    )
    routes = filter_batch(
        pick(
            routes_raw, ["batch_id", "route_id", "origin_id", "dest_id", "risk_score"]
        ),
        args.batch_id,
    )
    orders = filter_batch(
        pick(
            orders_raw,
            [
                "batch_id",
                "order_id",
                "erp_source",
                "customer_tier",
                "order_value_usd",
                "original_promise_dt",
                "target_delivery_dt",
                "estimated_delivery_dt",
                "order_priority",
                "sla_health",
            ],
        ),
        args.batch_id,
    )
    shipments = filter_batch(
        pick(
            shipments_raw,
            [
                "batch_id",
                "shipment_id",
                "order_id",
                "carrier_id",
                "facility_id",
                "route_id",
                "partner_id",
                "tracking_number",
                "dwell_time_sigma",
                "arrival_eta",
            ],
        ),
        args.batch_id,
    )
    events = filter_batch(
        pick(
            events_raw,
            [
                "batch_id",
                "event_id",
                "shipment_id",
                "order_id",
                "event_type",
                "timestamp_iso",
                "audit_hash",
                "delta_t_hrs",
                "location_id",
                "location_kind",
                "exception_reason",
            ],
        ),
        args.batch_id,
    )

    if args.dry_run:
        print("DRY RUN ✅ (no DB writes)")
        print("batch_id filter:", args.batch_id or "(none)")
        print(
            "counts:",
            {
                "facilities": len(facilities),
                "carriers": len(carriers),
                "partners": len(partners),
                "routes": len(routes),
                "orders": len(orders),
                "shipments": len(shipments),
                "events": len(events),
            },
        )
        if facilities:
            print("sample facility:", facilities[0])
        if orders:
            print("sample order:", orders[0])
        if shipments:
            print("sample shipment:", shipments[0])
        if events:
            print("sample event:", events[0])
        return

    driver = neo4j_driver()
    try:
        with driver.session() as session:
            for c in CONSTRAINTS:
                session.run(c)

            def upsert(cypher: str, rows: List[Dict[str, Any]], label: str) -> None:
                for b in chunked(rows, args.batch_size):
                    session.run(cypher, rows=b, now=now)
                print(f"✅ upserted {label}: {len(rows)}")

            # Dependency order (Region comes from facilities/partners)
            upsert(
                CYPHER_UPSERT_FACILITIES, facilities, "Facility (+ Region + IN_REGION)"
            )
            upsert(CYPHER_UPSERT_CARRIERS, carriers, "Carrier")
            upsert(
                CYPHER_UPSERT_PARTNERS,
                partners,
                "DeliveryPartner (+ Region + SERVICES)",
            )
            upsert(
                CYPHER_UPSERT_ROUTES,
                routes,
                "Route (+ Region IMPACTS via dest facility)",
            )
            upsert(CYPHER_UPSERT_ORDERS, orders, "Order")
            upsert(CYPHER_UPSERT_SHIPMENTS, shipments, "Shipment (nodes)")
            upsert(
                CYPHER_UPSERT_SHIPMENT_RELS,
                shipments,
                "Shipment relationships (FULFILLED_BY/DEPARTS_FROM/HANDED_OFF_TO/TRAVELS_VIA)",
            )

            # --- Wipe prior timeline (Events + CoCSteps) for any order_ids we're re-upserting ---
            order_ids_in_batch = sorted({o.get("order_id") for o in orders if o.get("order_id")})
            if order_ids_in_batch:
                session.run(
                    CYPHER_WIPE_TIMELINE_FOR_ORDERS, order_ids=order_ids_in_batch
                ).consume()
                print(
                    f"🗑  wiped prior Events + CoCSteps for {len(order_ids_in_batch)} order_ids"
                )

            upsert(CYPHER_UPSERT_EVENTS, events, "Event (nodes)")
            upsert(CYPHER_UPSERT_EVENT_RELS, events, "Event relationships (LOGGED_FOR)")

            # Name enrichment used to live here, but names are now assigned
            # in seed_scenario.py at generation time (like carriers) and
            # persisted directly via the upsert Cypher queries above. The
            # legacy CYPHER_ENRICH_* constants are kept below as a fallback
            # for any pre-existing batches that lack names.
            # Fallback enrichment runs only against rows whose facility_name /
            # partner_display_name / route_name is still NULL. This is a no-op
            # for newly seeded data.
            session.run(
                CYPHER_ENRICH_FACILITY_NAMES,
                bid=args.batch_id or "",
                dc_names=REAL_DC_NAMES,
                port_names=REAL_PORT_NAMES,
                crossdock_names=REAL_CROSSDOCK_NAMES,
            )
            session.run(
                CYPHER_ENRICH_PARTNER_NAMES,
                bid=args.batch_id or "",
                partner_names=REAL_PARTNER_NAMES,
            )
            session.run(
                CYPHER_ENRICH_ROUTE_NAMES,
                bid=args.batch_id or "",
            )
            print("✅ name fallback enrichment ran (no-op for fresh seed data)")

            # --- StageBaseline: per-stage dwell baseline from closed orders ---
            rec = session.run(CYPHER_UPSERT_STAGE_BASELINES, now=now).single()
            n_baselines = int((rec and rec.get("n_baselines")) or 0)
            n_events = int((rec and rec.get("n_events")) or 0)
            print(
                f"✅ StageBaseline upserted: {n_baselines} groups from {n_events} events of closed orders"
            )

        print("\nDone ✅")
        print("Batch filter:", args.batch_id or "(none)")
        print("\nSanity checks (paste into Neo4j Browser):")
        bid = args.batch_id or "<YOUR_BATCH_ID>"
        print(f'MATCH (o:Order {{batch_id:"{bid}"}}) RETURN count(o);')
        print(
            f'MATCH (o:Order {{batch_id:"{bid}"}})-[:FULFILLED_BY]->(:Shipment) RETURN count(*);'
        )
        print(
            f'MATCH (:Shipment {{batch_id:"{bid}"}})-[:DEPARTS_FROM]->(:Facility) RETURN count(*);'
        )
        print(
            f'MATCH (:Shipment {{batch_id:"{bid}"}})-[:HANDED_OFF_TO]->(:Carrier) RETURN count(*);'
        )
        print(
            f'MATCH (:Event {{batch_id:"{bid}"}})-[:LOGGED_FOR]->(:Shipment) RETURN count(*);'
        )
        print(
            f'MATCH (:DeliveryPartner {{batch_id:"{bid}"}})-[:SERVICES]->(:Region) RETURN count(*);'
        )
        print(
            f'MATCH (:Region)-[:IMPACTS]->(:Route {{batch_id:"{bid}"}}) RETURN count(*);'
        )
        print("\nName checks:")
        print(
            f'MATCH (f:Facility {{batch_id:"{bid}"}}) RETURN f.facility_id, f.facility_type, f.facility_name LIMIT 10;'
        )
        print(
            f'MATCH (p:DeliveryPartner {{batch_id:"{bid}"}}) RETURN p.partner_id, p.partner_name, p.partner_display_name LIMIT 10;'
        )
        print(
            f'MATCH (rt:Route {{batch_id:"{bid}"}}) RETURN rt.route_id, rt.route_name LIMIT 10;'
        )

    finally:
        driver.close()


if __name__ == "__main__":
    main()
