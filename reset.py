"""
reset_kg_resilient.py

✅ What it does
- WIPES the ENTIRE Neo4j DB (all nodes + rels) using BATCHED deletes (more stable on Aura)
- Recreates constraints
- Generates a fresh synthetic dataset:
  - 5,000 Orders spread across last 365 days
  - 1 Shipment per Order
  - Events per shipment with mix of:
      * delivered on-time
      * delivered late (~5% of ALL orders)
      * undelivered (~8% default; configurable)
      * a few EXCEPTIONs sprinkled in
- Inserts into Neo4j with retry logic to survive Aura connection resets

⚠️ DESTRUCTIVE
This deletes everything in the DB.

Run:
  python reset_kg_resilient.py --n_orders 5000 --seed 7

Env (.env):
  NEO4J_URI=neo4j+s://xxxx.databases.neo4j.io
  NEO4J_USERNAME=neo4j
  NEO4J_PASSWORD=...
"""

from __future__ import annotations

import argparse
import hashlib
import os
import random
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Callable

from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import SessionExpired, ServiceUnavailable, TransientError

import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parent))   # so seed_scenario imports when run as a script

from seed_scenario import (   # noqa: E402 — shared pools so names stay in sync
    REAL_DC_NAMES, REAL_PORT_NAMES, REAL_CROSSDOCK_NAMES, REAL_PARTNER_NAMES,
    DEFAULT_N_FACILITIES, DEFAULT_N_PARTNERS,
    _pick_unique_name,
)


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


# ----------------------------
# Helpers
# ----------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def weighted_choice(rng: random.Random, items: List[Tuple[str, float]]) -> str:
    total = sum(w for _, w in items)
    r = rng.random() * total
    upto = 0.0
    for val, w in items:
        upto += w
        if upto >= r:
            return val
    return items[-1][0]


def chunked(rows: List[Dict[str, Any]], batch_size: int) -> List[List[Dict[str, Any]]]:
    return [rows[i : i + batch_size] for i in range(0, len(rows), batch_size)]


# ----------------------------
# Retry wrapper (Aura-friendly)
# ----------------------------
RETRYABLE = (SessionExpired, ServiceUnavailable, TransientError)


def run_with_retry(driver, fn: Callable, *, retries: int = 7) -> Any:
    last: Optional[Exception] = None
    for _ in range(retries):
        try:
            with driver.session() as session:
                return fn(session)
        except RETRYABLE as e:
            last = e
    if last:
        raise last
    raise RuntimeError("run_with_retry failed without exception (unexpected)")


# ----------------------------
# Data models
# ----------------------------
@dataclass
class Facility:
    batch_id: str
    facility_id: str
    facility_type: str
    facility_name: str
    region_id: str
    geo_lat: float
    geo_lon: float
    throughput_z: float
    status_flag: str


@dataclass
class Carrier:
    batch_id: str
    carrier_id: str
    carrier_name: str
    reliability_idx: float
    cost_volatility: float


@dataclass
class DeliveryPartner:
    batch_id: str
    partner_id: str
    partner_name: str
    partner_display_name: str
    service_zone_id: str
    capacity_z: float
    success_rate_z: float


@dataclass
class Route:
    batch_id: str
    route_id: str
    origin_id: str
    dest_id: str
    route_name: str
    risk_score: float


@dataclass
class Order:
    batch_id: str
    order_id: str
    erp_source: str
    customer_tier: str
    order_value_usd: float
    original_promise_dt: str
    target_delivery_dt: str
    estimated_delivery_dt: str
    order_priority: int
    sla_health: str


@dataclass
class Shipment:
    batch_id: str
    shipment_id: str
    order_id: str
    carrier_id: str
    facility_id: str
    route_id: str
    partner_id: Optional[str]
    tracking_number: str
    dwell_time_sigma: float
    arrival_eta: str


@dataclass
class Event:
    batch_id: str
    event_id: str
    shipment_id: str
    order_id: str
    event_type: str
    timestamp_iso: str
    audit_hash: str
    delta_t_hrs: float
    location_id: str
    location_kind: str
    exception_reason: Optional[str]


# ----------------------------
# Event taxonomy + location rules
# ----------------------------
EXCEPTION_REASONS: List[Tuple[str, float]] = [
    ("WEATHER", 0.18),
    ("CUSTOMS", 0.12),
    ("DAMAGE", 0.10),
    ("ADDRESS", 0.18),
    ("CAPACITY", 0.22),
    ("OTHER", 0.20),
]

BASE_SEQ = [
    "LABEL_CREATED",
    "PICKED",
    "GATE_OUT",
    "IN_TRANSIT",
    "GATE_IN",
    "OUT_FOR_DELIVERY",
]


def event_location(
    et: str,
    *,
    origin_facility_id: str,
    dest_facility_id: str,
    route_id: str,
    partner_id: Optional[str],
    order_id: str,
) -> Tuple[str, str]:
    et = (et or "").strip()

    if et in ("LABEL_CREATED", "PICKED", "GATE_OUT"):
        return origin_facility_id, "FACILITY"
    if et == "IN_TRANSIT":
        return route_id, "ROUTE"
    if et == "GATE_IN":
        return dest_facility_id, "FACILITY"
    if et == "OUT_FOR_DELIVERY":
        if partner_id:
            return partner_id, "PARTNER"
        return dest_facility_id, "FACILITY"
    if et == "DELIVERED":
        return f"CUST::{order_id}", "CUSTOMER"
    return route_id, "ROUTE"


def tier_weight(tier: str) -> float:
    return {"Platinum": 1.0, "Strategic": 0.85, "Gold": 0.65, "Standard": 0.40}.get(
        tier, 0.40
    )


# ----------------------------
# Generate master data
# ----------------------------
def gen_facilities(batch_id: str, rng: random.Random, n: int) -> List[Facility]:
    pool_for_type = {
        "DC":         REAL_DC_NAMES,
        "Port":       REAL_PORT_NAMES,
        "Cross-dock": REAL_CROSSDOCK_NAMES,
    }
    type_order = ["DC", "Port", "Cross-dock"]
    per_type_idx: Dict[str, int] = {"DC": 0, "Port": 0, "Cross-dock": 0}
    used_names: set = set()

    def type_for_slot(i: int) -> str:
        boundary = 0
        for t in type_order:
            boundary += len(pool_for_type[t])
            if i < boundary:
                return t
        overflow = i - sum(len(pool_for_type[t]) for t in type_order)
        return type_order[overflow % len(type_order)]

    out: List[Facility] = []
    for i in range(n):
        fid   = f"Facility_{i + 1:04d}"
        ftype = type_for_slot(i)
        fname = _pick_unique_name(pool_for_type[ftype], used_names, per_type_idx[ftype])
        per_type_idx[ftype] += 1

        lat = rng.uniform(25.0, 49.0)
        lon = rng.uniform(-124.0, -67.0)
        region_id = f"Z_{rng.randint(1, 250):03d}"

        site_mean = rng.uniform(800, 5000)
        site_std  = rng.uniform(150, 900)
        current_units = max(0.0, rng.gauss(site_mean, site_std))
        throughput_z = (current_units - site_mean) / (site_std if site_std > 1e-9 else 1.0)
        status_flag  = "Bottleneck" if throughput_z < -2.0 else "Normal"

        out.append(
            Facility(
                batch_id=batch_id,
                facility_id=fid,
                facility_type=ftype,
                facility_name=fname,
                region_id=region_id,
                geo_lat=round(lat, 6),
                geo_lon=round(lon, 6),
                throughput_z=round(throughput_z, 4),
                status_flag=status_flag,
            )
        )
    return out


def gen_carriers(batch_id: str, rng: random.Random) -> List[Carrier]:
    carrier_seed = [
        ("FDXE", "FedEx Express"),
        ("UPSN", "UPS"),
        ("DHLG", "DHL"),
        ("CHRW", "C.H. Robinson"),
        ("XPOI", "XPO Logistics"),
    ]
    out: List[Carrier] = []
    for cid, name in carrier_seed:
        reliability_pct = clamp(rng.gauss(93, 4), 70, 99.8)
        cost_vol = clamp(rng.gauss(0.0, 1.1), -3.0, 5.0)
        out.append(
            Carrier(
                batch_id=batch_id,
                carrier_id=cid,
                carrier_name=name,
                reliability_idx=round(reliability_pct, 2),
                cost_volatility=round(cost_vol, 3),
            )
        )
    return out


def gen_partners(batch_id: str, rng: random.Random, n: int) -> List[DeliveryPartner]:
    used_names: set = set()
    out: List[DeliveryPartner] = []
    for i in range(n):
        pid       = f"P_{i + 1:04d}"
        pnm       = f"Partner_{i + 1:03d}"                                  # internal code
        pdn       = _pick_unique_name(REAL_PARTNER_NAMES, used_names, i)    # display name
        zone      = f"Z_{rng.randint(1, 250):03d}"
        cap  = clamp(rng.gauss(0.0, 1.2), -3.5, 3.5)
        succ = clamp(rng.gauss(0.0, 1.0), -3.5, 3.5)
        out.append(
            DeliveryPartner(
                batch_id=batch_id,
                partner_id=pid,
                partner_name=pnm,
                partner_display_name=pdn,
                service_zone_id=zone,
                capacity_z=round(cap, 3),
                success_rate_z=round(succ, 3),
            )
        )
    return out


def gen_routes(
    batch_id: str, rng: random.Random, facilities: List[Facility], n: int
) -> List[Route]:
    fac_ids    = [f.facility_id for f in facilities]
    id_to_name = {f.facility_id: f.facility_name for f in facilities}
    routes: List[Route] = []
    for _ in range(n):
        origin = rng.choice(fac_ids)
        dest   = rng.choice([x for x in fac_ids if x != origin])
        rid    = f"{origin}__{dest}"
        rname  = f"{id_to_name.get(origin, origin)} → {id_to_name.get(dest, dest)}"

        weather = clamp(rng.random() + rng.gauss(0, 0.1), 0, 1)
        traffic = clamp(rng.random() + rng.gauss(0, 0.1), 0, 1)
        history = clamp(rng.random() + rng.gauss(0, 0.1), 0, 1)
        risk = 0.35 * weather + 0.35 * traffic + 0.30 * history

        routes.append(
            Route(
                batch_id=batch_id,
                route_id=rid,
                origin_id=origin,
                dest_id=dest,
                route_name=rname,
                risk_score=round(risk, 4),
            )
        )

    uniq: Dict[str, Route] = {r.route_id: r for r in routes}
    return list(uniq.values())


# ----------------------------
# Generate orders + shipments + events
# ----------------------------
def gen_orders_spread_over_year(
    batch_id: str,
    rng: random.Random,
    n: int,
    now: datetime,
    erp_source: str,
) -> List[Order]:
    tiers = [
        ("Platinum", 0.08),
        ("Strategic", 0.15),
        ("Gold", 0.27),
        ("Standard", 0.50),
    ]
    raw_values = [max(50.0, rng.lognormvariate(9.0, 0.55)) for _ in range(n)]
    vmin, vmax = min(raw_values), max(raw_values)
    denom = (vmax - vmin) if (vmax - vmin) > 1e-9 else 1.0

    out: List[Order] = []
    for i in range(n):
        oid = f"order_{i + 1:06d}"
        tier = weighted_choice(rng, tiers)
        value = round(raw_values[i], 2)

        created_at = now - timedelta(
            days=rng.randint(0, 364),
            hours=rng.randint(0, 23),
            minutes=rng.randint(0, 59),
        )

        original_promise = created_at + timedelta(days=rng.randint(2, 12))

        if rng.random() < 0.25:
            target_delivery = original_promise + timedelta(days=rng.randint(1, 5))
        else:
            target_delivery = original_promise

        est_delivery = target_delivery + timedelta(days=rng.randint(-1, 3))

        tv = tier_weight(tier)
        ov = (value - vmin) / denom
        priority_0_1 = (tv * 0.6) + (ov * 0.4)
        order_priority = int(round(clamp(priority_0_1, 0, 1) * 100))

        if now.date() > target_delivery.date():
            sla = "Critical"
        else:
            hours_to_target = (
                datetime.combine(
                    target_delivery.date(), datetime.min.time(), tzinfo=timezone.utc
                )
                - now
            ).total_seconds() / 3600
            sla = "At-Risk" if hours_to_target <= 48 else "Healthy"

        out.append(
            Order(
                batch_id=batch_id,
                order_id=oid,
                erp_source=erp_source,
                customer_tier=tier,
                order_value_usd=value,
                original_promise_dt=original_promise.date().isoformat(),
                target_delivery_dt=target_delivery.date().isoformat(),
                estimated_delivery_dt=est_delivery.date().isoformat(),
                order_priority=order_priority,
                sla_health=sla,
            )
        )
    return out


def gen_shipments_events_with_delivery_mix(
    batch_id: str,
    rng: random.Random,
    now: datetime,
    orders: List[Order],
    facilities: List[Facility],
    routes: List[Route],
    carriers: List[Carrier],
    partners: List[DeliveryPartner],
    *,
    pct_undelivered: float,
    pct_delivered_late: float,
    min_events: int,
    max_events: int,
) -> Tuple[List[Shipment], List[Event]]:
    carriers_ids = [c.carrier_id for c in carriers]
    route_by_origin: Dict[str, List[Route]] = {}
    for r in routes:
        route_by_origin.setdefault(r.origin_id, []).append(r)

    n = len(orders)
    idxs = list(range(n))
    rng.shuffle(idxs)
    n_undelivered = int(round(pct_undelivered * n))
    n_delivered = n - n_undelivered
    n_late = int(round(pct_delivered_late * n_delivered))
    n_on_time = n_delivered - n_late

    undelivered_set = set(idxs[:n_undelivered])
    late_set = set(idxs[n_undelivered : n_undelivered + n_late])

    fac_dwell_mu_sigma: Dict[str, Tuple[float, float]] = {}
    for f in facilities:
        fac_dwell_mu_sigma[f.facility_id] = (
            rng.uniform(2.0, 24.0),
            rng.uniform(0.8, 6.0),
        )

    shipments: List[Shipment] = []
    events: List[Event] = []

    for i, o in enumerate(orders):
        shipment_id = f"S_{i + 1:06d}"
        origin_fac = rng.choice(facilities).facility_id

        if origin_fac in route_by_origin and route_by_origin[origin_fac]:
            route = rng.choice(route_by_origin[origin_fac])
        else:
            route = rng.choice(routes)

        dest_fac = route.dest_id
        carrier_id = rng.choice(carriers_ids)
        partner_id = (
            rng.choice(partners).partner_id
            if partners and rng.random() < 0.70
            else None
        )
        tracking_number = f"TRK{rng.randint(10**11, 10**12 - 1)}"

        target_dt = datetime.fromisoformat(o.target_delivery_dt).replace(
            tzinfo=timezone.utc
        )
        started_at = target_dt - timedelta(
            days=rng.randint(1, 10),
            hours=rng.randint(0, 23),
            minutes=rng.randint(0, 59),
        )

        n_events = rng.randint(min_events, max_events)
        seq = BASE_SEQ[:]

        while len(seq) < max(2, min(n_events, 10)) and rng.random() < 0.65:
            insert = rng.choice(["IN_TRANSIT", "EXCEPTION"])
            pos = rng.randint(3, max(3, len(seq)))
            seq.insert(pos, insert)

        is_undelivered = i in undelivered_set
        is_late = i in late_set

        if not is_undelivered:
            seq.append("DELIVERED")
        else:
            if rng.random() < 0.35:
                seq.append("EXCEPTION")

        if len(seq) > n_events:
            seq = seq[:n_events]
            if (not is_undelivered) and (seq[-1] != "DELIVERED"):
                seq[-1] = "DELIVERED"

        t = started_at
        prev_t: Optional[datetime] = None

        mu, sig = fac_dwell_mu_sigma.get(origin_fac, (8.0, 2.0))
        current_dwell = max(0.0, rng.gauss(mu, sig))
        dwell_sigma = (current_dwell - mu) / (sig if sig > 1e-9 else 1.0)

        delivered_ts: Optional[datetime] = None

        for et in seq:
            if et == "LABEL_CREATED":
                dt = timedelta(minutes=rng.randint(0, 180))
            elif et == "PICKED":
                dt = timedelta(hours=rng.uniform(0.5, 8.0))
            elif et == "GATE_OUT":
                dt = timedelta(hours=rng.uniform(0.2, 3.0))
            elif et == "IN_TRANSIT":
                dt = timedelta(hours=rng.uniform(2.0, 24.0) * (1.0 + route.risk_score))
            elif et == "GATE_IN":
                dt = timedelta(hours=rng.uniform(1.0, 12.0))
            elif et == "OUT_FOR_DELIVERY":
                dt = timedelta(hours=rng.uniform(0.5, 8.0))
            elif et == "DELIVERED":
                dt = timedelta(hours=rng.uniform(0.2, 8.0))
            else:
                dt = timedelta(hours=rng.uniform(0.1, 14.0))

            t = t + dt
            delta_hrs = 0.0 if prev_t is None else (t - prev_t).total_seconds() / 3600.0

            event_id = str(uuid.uuid4())
            ts = iso(t)
            audit = sha256_hex(f"{shipment_id}|{et}|{ts}|SYNTH")

            exception_reason = (
                weighted_choice(rng, EXCEPTION_REASONS) if et == "EXCEPTION" else None
            )
            loc_id, loc_kind = event_location(
                et,
                origin_facility_id=origin_fac,
                dest_facility_id=dest_fac,
                route_id=route.route_id,
                partner_id=partner_id,
                order_id=o.order_id,
            )

            events.append(
                Event(
                    batch_id=batch_id,
                    event_id=event_id,
                    shipment_id=shipment_id,
                    order_id=o.order_id,
                    event_type=et,
                    timestamp_iso=ts,
                    audit_hash=audit,
                    delta_t_hrs=round(delta_hrs, 4),
                    location_id=loc_id,
                    location_kind=loc_kind,
                    exception_reason=exception_reason,
                )
            )

            if et == "DELIVERED":
                delivered_ts = t

            prev_t = t

        # Force on-time vs late by moving only the DELIVERED timestamp (matches your OTD query)
        if delivered_ts is not None:
            deadline = datetime.combine(
                target_dt.date(),
                datetime.max.time().replace(microsecond=0),
                tzinfo=timezone.utc,
            )
            if is_late:
                new_delivered = deadline + timedelta(hours=rng.randint(1, 72))
            else:
                new_delivered = deadline - timedelta(hours=rng.randint(0, 72))
                if new_delivered <= started_at:
                    new_delivered = started_at + timedelta(hours=rng.randint(1, 24))

            for ev in reversed(events):
                if ev.shipment_id == shipment_id and ev.event_type == "DELIVERED":
                    ev.timestamp_iso = iso(new_delivered)
                    ev.audit_hash = sha256_hex(
                        f"{shipment_id}|DELIVERED|{ev.timestamp_iso}|SYNTH"
                    )
                    delivered_ts = new_delivered
                    break

        last_event_ts = prev_t if prev_t is not None else started_at
        arrival_eta = last_event_ts + timedelta(
            hours=clamp(rng.gauss(18, 6) * (1.0 + route.risk_score), 4, 120)
        )

        shipments.append(
            Shipment(
                batch_id=batch_id,
                shipment_id=shipment_id,
                order_id=o.order_id,
                carrier_id=carrier_id,
                facility_id=origin_fac,
                route_id=route.route_id,
                partner_id=partner_id,
                tracking_number=tracking_number,
                dwell_time_sigma=round(dwell_sigma, 4),
                arrival_eta=iso(arrival_eta),
            )
        )

        if delivered_ts is not None:
            o.estimated_delivery_dt = delivered_ts.date().isoformat()
        else:
            est = arrival_eta + timedelta(days=rng.uniform(0.5, 2.5))
            o.estimated_delivery_dt = est.date().isoformat()

        if now.date() > target_dt.date():
            o.sla_health = "Critical"

    return shipments, events


# ----------------------------
# Neo4j schema + cypher
# ----------------------------
CONSTRAINTS = [
    "CREATE CONSTRAINT order_id IF NOT EXISTS FOR (n:Order) REQUIRE n.order_id IS UNIQUE",
    "CREATE CONSTRAINT shipment_id IF NOT EXISTS FOR (n:Shipment) REQUIRE n.shipment_id IS UNIQUE",
    "CREATE CONSTRAINT event_id IF NOT EXISTS FOR (n:Event) REQUIRE n.event_id IS UNIQUE",
    "CREATE CONSTRAINT facility_id IF NOT EXISTS FOR (n:Facility) REQUIRE n.facility_id IS UNIQUE",
    "CREATE CONSTRAINT carrier_id IF NOT EXISTS FOR (n:Carrier) REQUIRE n.carrier_id IS UNIQUE",
    "CREATE CONSTRAINT route_id IF NOT EXISTS FOR (n:Route) REQUIRE n.route_id IS UNIQUE",
    "CREATE CONSTRAINT partner_id IF NOT EXISTS FOR (n:DeliveryPartner) REQUIRE n.partner_id IS UNIQUE",
    "CREATE CONSTRAINT region_id IF NOT EXISTS FOR (n:Region) REQUIRE n.region_id IS UNIQUE",
]

WIPE_DB_BATCHED = """
CALL {
  MATCH (n)
  WITH n LIMIT $limit
  DETACH DELETE n
  RETURN count(*) AS deleted
}
RETURN deleted;
"""

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
SET r.batch_id = row.batch_id, r.updated_at = $now
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
SET r.batch_id = row.batch_id, r.updated_at = $now
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
OPTIONAL MATCH (fd:Facility {facility_id: row.dest_id})
FOREACH (_ IN CASE WHEN fd IS NULL OR fd.region_id IS NULL THEN [] ELSE [1] END |
  MERGE (r:Region {region_id: fd.region_id})
  SET r.batch_id = row.batch_id, r.updated_at = $now
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

WITH s, row
OPTIONAL MATCH (o:Order {order_id: row.order_id})
FOREACH (_ IN CASE WHEN o IS NULL THEN [] ELSE [1] END |
  MERGE (o)-[:FULFILLED_BY]->(s)
)

WITH s, row
OPTIONAL MATCH (c:Carrier {carrier_id: row.carrier_id})
FOREACH (_ IN CASE WHEN c IS NULL THEN [] ELSE [1] END |
  MERGE (s)-[:HANDED_OFF_TO]->(c)
)

WITH s, row
OPTIONAL MATCH (f:Facility {facility_id: row.facility_id})
FOREACH (_ IN CASE WHEN f IS NULL THEN [] ELSE [1] END |
  MERGE (s)-[:DEPARTS_FROM]->(f)
)

WITH s, row
OPTIONAL MATCH (rt:Route {route_id: row.route_id})
FOREACH (_ IN CASE WHEN rt IS NULL THEN [] ELSE [1] END |
  MERGE (s)-[:TRAVELS_VIA]->(rt)
)
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
WITH e, row
OPTIONAL MATCH (s:Shipment {shipment_id: row.shipment_id})
FOREACH (_ IN CASE WHEN s IS NULL THEN [] ELSE [1] END |
  MERGE (e)-[:LOGGED_FOR]->(s)
)
"""


def wipe_db(driver, *, limit: int = 20000) -> int:
    def _wipe(session):
        total = 0
        while True:
            r = session.run(WIPE_DB_BATCHED, limit=limit).single()
            deleted = int((r and r.get("deleted")) or 0)
            total += deleted
            if deleted == 0:
                break
        return total

    total = run_with_retry(driver, _wipe)
    return int(total or 0)


def ensure_constraints(driver) -> None:
    def _c(session):
        for c in CONSTRAINTS:
            session.run(c).consume()
        return True

    run_with_retry(driver, _c)


def upsert_many(
    driver,
    cypher: str,
    rows: List[Dict[str, Any]],
    *,
    now_iso: str,
    batch_size: int,
    label: str,
) -> None:
    payload = rows
    if not payload:
        print(f"✅ upserted {label}: 0")
        return

    def _run_batch(session, batch):
        session.run(cypher, rows=batch, now=now_iso).consume()

    for b in chunked(payload, batch_size):
        run_with_retry(driver, lambda session, bb=b: _run_batch(session, bb))
    print(f"✅ upserted {label}: {len(payload)}")


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--batch_id", default="")
    ap.add_argument("--erp_source", default="ORACLE")
    ap.add_argument("--n_orders", type=int, default=5000)
    ap.add_argument("--n_facilities", type=int, default=DEFAULT_N_FACILITIES,
                    help=f"default {DEFAULT_N_FACILITIES} — exactly fills the name pools "
                         f"(DC={len(REAL_DC_NAMES)}, Port={len(REAL_PORT_NAMES)}, "
                         f"CrossDock={len(REAL_CROSSDOCK_NAMES)})")
    ap.add_argument("--n_partners", type=int, default=DEFAULT_N_PARTNERS,
                    help=f"default {DEFAULT_N_PARTNERS} — exactly fills REAL_PARTNER_NAMES")
    ap.add_argument("--n_routes", type=int, default=120)
    ap.add_argument("--min_events", type=int, default=5)
    ap.add_argument("--max_events", type=int, default=10)
    ap.add_argument(
        "--pct_undelivered",
        type=float,
        default=0.0,
        help="Fraction of orders to leave undelivered (default 0 = pure happy path)",
    )
    ap.add_argument(
        "--pct_delivered_late",
        type=float,
        default=0.0,
        help="Fraction of delivered orders to push past target_delivery_dt (default 0)",
    )
    ap.add_argument(
        "--batch_size", type=int, default=300
    )  # smaller is more stable on Aura
    ap.add_argument("--wipe_limit", type=int, default=20000)
    ap.add_argument("--dry_run", action="store_true")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    now = utc_now()

    batch_id = (args.batch_id or "").strip()
    if not batch_id:
        batch_id = f"reset_{now.strftime('%Y%m%dT%H%M%SZ')}_seed{args.seed}"

    # Generate
    facilities = gen_facilities(batch_id, rng, args.n_facilities)
    carriers = gen_carriers(batch_id, rng)
    partners = gen_partners(batch_id, rng, args.n_partners)
    routes = gen_routes(batch_id, rng, facilities, args.n_routes)
    orders = gen_orders_spread_over_year(
        batch_id, rng, args.n_orders, now, args.erp_source
    )
    shipments, events = gen_shipments_events_with_delivery_mix(
        batch_id=batch_id,
        rng=rng,
        now=now,
        orders=orders,
        facilities=facilities,
        routes=routes,
        carriers=carriers,
        partners=partners,
        pct_undelivered=args.pct_undelivered,
        pct_delivered_late=args.pct_delivered_late,
        min_events=args.min_events,
        max_events=args.max_events,
    )

    # Normalize to dicts for cypher
    fac_rows = [asdict(x) for x in facilities]
    car_rows = [asdict(x) for x in carriers]
    par_rows = [asdict(x) for x in partners]
    rte_rows = [asdict(x) for x in routes]
    ord_rows = [asdict(x) for x in orders]
    shp_rows = [asdict(x) for x in shipments]
    evt_rows = [asdict(x) for x in events]

    if args.dry_run:
        delivered_events = sum(1 for e in evt_rows if e["event_type"] == "DELIVERED")
        print("DRY RUN ✅ (no DB writes)")
        print("batch_id:", batch_id)
        print(
            "counts:",
            {
                "facilities": len(fac_rows),
                "carriers": len(car_rows),
                "partners": len(par_rows),
                "routes": len(rte_rows),
                "orders": len(ord_rows),
                "shipments": len(shp_rows),
                "events": len(evt_rows),
                "delivered_events": delivered_events,
            },
        )
        return

    # Neo4j connect
    uri = req_env("NEO4J_URI")
    user = req_env("NEO4J_USERNAME")
    pwd = req_env("NEO4J_PASSWORD")
    driver = GraphDatabase.driver(uri, auth=(user, pwd))
    now_iso = iso(now)

    try:
        deleted = wipe_db(driver, limit=args.wipe_limit)
        print(f"🧨 WIPED DB (batched). deleted approx nodes: {deleted}")

        ensure_constraints(driver)
        print("✅ constraints ensured")

        # Upserts (dependency order)
        upsert_many(
            driver,
            CYPHER_UPSERT_FACILITIES,
            fac_rows,
            now_iso=now_iso,
            batch_size=args.batch_size,
            label="Facility (+Region+IN_REGION)",
        )
        upsert_many(
            driver,
            CYPHER_UPSERT_CARRIERS,
            car_rows,
            now_iso=now_iso,
            batch_size=args.batch_size,
            label="Carrier",
        )
        upsert_many(
            driver,
            CYPHER_UPSERT_PARTNERS,
            par_rows,
            now_iso=now_iso,
            batch_size=args.batch_size,
            label="DeliveryPartner (+Region+SERVICES)",
        )
        upsert_many(
            driver,
            CYPHER_UPSERT_ROUTES,
            rte_rows,
            now_iso=now_iso,
            batch_size=args.batch_size,
            label="Route (+Region IMPACTS)",
        )
        upsert_many(
            driver,
            CYPHER_UPSERT_ORDERS,
            ord_rows,
            now_iso=now_iso,
            batch_size=args.batch_size,
            label="Order",
        )
        upsert_many(
            driver,
            CYPHER_UPSERT_SHIPMENTS,
            shp_rows,
            now_iso=now_iso,
            batch_size=args.batch_size,
            label="Shipment (+FULFILLED_BY/DEPARTS_FROM/HANDED_OFF_TO/TRAVELS_VIA)",
        )
        upsert_many(
            driver,
            CYPHER_UPSERT_EVENTS,
            evt_rows,
            now_iso=now_iso,
            batch_size=args.batch_size,
            label="Event (+LOGGED_FOR)",
        )

        print("\nDone ✅")
        print("batch_id:", batch_id)
        print("Reference now (UTC):", now_iso)
        print("\nSanity checks (Neo4j Browser):")
        print(f'MATCH (o:Order {{batch_id:"{batch_id}"}}) RETURN count(o);')
        print(f'MATCH (s:Shipment {{batch_id:"{batch_id}"}}) RETURN count(s);')
        print(f'MATCH (e:Event {{batch_id:"{batch_id}"}}) RETURN count(e);')
        print(
            f'MATCH (:Order {{batch_id:"{batch_id}"}})-[:FULFILLED_BY]->(:Shipment) RETURN count(*);'
        )
        print(
            f'MATCH (:Event {{batch_id:"{batch_id}"}})-[:LOGGED_FOR]->(:Shipment) RETURN count(*);'
        )
        print(
            'MATCH (o:Order) WHERE EXISTS { MATCH (:Event {event_type:"DELIVERED", order_id:o.order_id}) } RETURN count(o) AS delivered_orders;'
        )
        print(
            'MATCH (o:Order) WHERE NOT EXISTS { MATCH (:Event {event_type:"DELIVERED", order_id:o.order_id}) } RETURN count(o) AS undelivered_orders;'
        )

    finally:
        driver.close()


if __name__ == "__main__":
    main()
