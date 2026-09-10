# seed_scenario.py
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ============================
# Helpers
# ============================
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


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_jsonl(path: Path, rows: List[dict]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def write_json(path: Path, obj: dict) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


# ============================
# Schema Definition (machine-readable)
# ============================
SCHEMA_DEFINITION: Dict[str, dict] = {
    "Order": {
        "source": "Oracle Fusion Cloud / SAP S/4HANA",
        "pk": "order_id",
        "fields": {
            "batch_id": {
                "type": "string",
                "derived": False,
                "desc": "Synthetic run identifier",
            },
            "order_id": {
                "type": "string",
                "derived": False,
                "desc": "Unique ERP Header ID (batch-scoped)",
            },
            "erp_source": {
                "type": "string",
                "derived": False,
                "desc": "Originating system",
            },
            "customer_tier": {
                "type": "string",
                "derived": False,
                "desc": "Priority tier",
            },
            "order_value_usd": {
                "type": "decimal",
                "derived": False,
                "desc": "Total monetary value",
            },
            "original_promise_dt": {
                "type": "date",
                "derived": False,
                "desc": "Initial promise date",
            },
            "target_delivery_dt": {
                "type": "date",
                "derived": False,
                "desc": "Current adjusted promise date",
            },
            "estimated_delivery_dt": {
                "type": "date",
                "derived": True,
                "desc": "Latest_Shipment_ETA + Last_Mile_Lead_Time",
            },
            "order_priority": {
                "type": "int",
                "derived": True,
                "desc": "(Tier*0.6)+(Value*0.4) scaled 1-100",
            },
            "sla_health": {
                "type": "string",
                "derived": True,
                "desc": "Critical if now > target, else Healthy/At-Risk",
            },
        },
    },
    "Shipment": {
        "source": "Oracle WMS / SAP EWM / Carrier APIs",
        "pk": "shipment_id",
        "fields": {
            "batch_id": {"type": "string", "derived": False},
            "shipment_id": {"type": "string", "derived": False},
            "order_id": {"type": "string", "derived": False},
            "carrier_id": {"type": "string", "derived": False},
            "facility_id": {"type": "string", "derived": False},
            "route_id": {"type": "string", "derived": False},
            "partner_id": {"type": "string", "derived": False, "nullable": True},
            "tracking_number": {"type": "string", "derived": False},
            "dwell_time_sigma": {"type": "float", "derived": True},
            "arrival_eta": {"type": "datetime", "derived": True},
        },
    },
    "Facility": {
        "source": "Oracle Master Data / WMS Sensors",
        "pk": "facility_id",
        "fields": {
            "batch_id": {"type": "string", "derived": False},
            "facility_id": {"type": "string", "derived": False},
            "facility_type": {"type": "string", "derived": False},
            "geo_point": {"type": "geo", "derived": False, "desc": "lat/long"},
            "throughput_z": {"type": "float", "derived": True},
            "status_flag": {"type": "string", "derived": True},
            "region_id": {
                "type": "string",
                "derived": False,
                "desc": "Region/zone identifier (e.g., Z_042)",
            },
        },
    },
    "Carrier": {
        "source": "FedEx, UPS, DHL, C.H. Robinson",
        "pk": "carrier_id",
        "fields": {
            "batch_id": {
                "type": "string",
                "derived": False,
                "desc": "Set for traceability (carrier_id is global)",
            },
            "carrier_id": {"type": "string", "derived": False},
            "carrier_name": {"type": "string", "derived": False},
            "reliability_idx": {"type": "float", "derived": True},
            "cost_volatility": {"type": "float", "derived": True},
        },
    },
    "Event": {
        "source": "Streaming Data (Kafka / Integration Hub)",
        "pk": "event_id",
        "fields": {
            "batch_id": {"type": "string", "derived": False},
            "event_id": {"type": "uuid", "derived": False},
            "shipment_id": {"type": "string", "derived": False},
            "order_id": {"type": "string", "derived": False},
            "event_type": {"type": "string", "derived": False},
            "timestamp_iso": {"type": "datetime", "derived": False},
            "audit_hash": {"type": "string", "derived": True},
            "delta_t_hrs": {"type": "float", "derived": True},
            "location_id": {
                "type": "string",
                "derived": False,
                "desc": "Facility/Route/Partner/Customer identifier",
            },
            "location_kind": {
                "type": "string",
                "derived": False,
                "desc": "FACILITY|ROUTE|PARTNER|CUSTOMER",
            },
            "exception_reason": {
                "type": "string",
                "derived": False,
                "nullable": True,
                "desc": "WEATHER|CUSTOMS|DAMAGE|ADDRESS|CAPACITY|OTHER",
            },
        },
    },
    "DeliveryPartner": {
        "source": "Partner Registry + Ops Signals",
        "pk": "partner_id",
        "fields": {
            "batch_id": {"type": "string", "derived": False},
            "partner_id": {"type": "string", "derived": False},
            "partner_name": {"type": "string", "derived": False},
            "service_zone_id": {"type": "string", "derived": False},
            "capacity_z": {"type": "float", "derived": True},
            "success_rate_z": {"type": "float", "derived": True},
        },
    },
    "Route": {
        "source": "TMS / Google Maps / Visibility Providers",
        "pk": "route_id",
        "fields": {
            "batch_id": {"type": "string", "derived": False},
            "route_id": {
                "type": "string",
                "derived": True,
                "desc": "origin_id__dest_id",
            },
            "origin_id": {"type": "string", "derived": False},
            "dest_id": {"type": "string", "derived": False},
            "risk_score": {"type": "float", "derived": True},
        },
    },
}


# ============================
# Data Models
# ============================
# ---- Display-name pools (assigned at seed time, persisted to KG) ----------
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

# Defaults derived from the pools so the script "just picks right" without
# requiring --n_facilities / --n_partners overrides on every run.
DEFAULT_N_FACILITIES = (
    len(REAL_DC_NAMES) + len(REAL_PORT_NAMES) + len(REAL_CROSSDOCK_NAMES)
)
DEFAULT_N_PARTNERS = len(REAL_PARTNER_NAMES)


@dataclass
class Facility:
    batch_id: str
    facility_id: str
    facility_type: str
    facility_name: str
    region_id: str
    geo_point: Dict[str, float]
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


# ============================
# Scenario presets (4 simple ones)
# ============================
# Goal: user selects one key; optionally override a few knobs.
SCENARIOS: Dict[str, Dict[str, Any]] = {
    "S1_BASELINE_MOSTLY_ONTIME": {
        "label": "Baseline (mostly on-time, light noise)",
        "n_orders": 200,
        "delay_pct": 0.10,
        "delay_place": "MIXED",  # FACILITY|ROUTE|PARTNER|MIXED
        "delay_hours_range": (6, 30),
        "exception_pct": 0.05,
        "exception_bias": None,
    },
    "S2_FACILITY_BOTTLENECK": {
        "label": "Facility bottleneck (picked/gate-out dwell spike)",
        "n_orders": 200,
        "delay_pct": 0.35,
        "delay_place": "FACILITY",
        "delay_hours_range": (12, 72),
        "exception_pct": 0.08,
        "exception_bias": "CAPACITY",
    },
    "S3_ROUTE_DISRUPTION": {
        "label": "Route disruption (in-transit dwell spike + weather/customs)",
        "n_orders": 200,
        "delay_pct": 0.30,
        "delay_place": "ROUTE",
        "delay_hours_range": (10, 80),
        "exception_pct": 0.15,
        "exception_bias": "WEATHER",
    },
    "S4_LAST_MILE_PARTNER_OVERLOAD": {
        "label": "Last-mile partner overload (OFD→Delivered dwell spike)",
        "n_orders": 200,
        "delay_pct": 0.33,
        "delay_place": "PARTNER",
        "delay_hours_range": (8, 60),
        "exception_pct": 0.10,
        "exception_bias": "ADDRESS",
    },
}


def list_scenarios() -> List[Tuple[str, str]]:
    return [(k, SCENARIOS[k]["label"]) for k in SCENARIOS.keys()]


# ============================
# Generation Logic (entities)
# ============================
def _pick_unique_name(pool: List[str], used: set, idx_in_type: int) -> str:
    """Sequentially picks the next pool name; appends ' #N' when the pool
    wraps so two facilities of the same type always have different names."""
    base = pool[idx_in_type % len(pool)]
    if idx_in_type < len(pool):
        name = base
    else:
        name = f"{base} #{idx_in_type // len(pool) + 1}"
    # Defensive uniqueness: in the unlikely case of a collision (e.g. seed re-run),
    # bump the suffix until clear.
    while name in used:
        idx_in_type += 1
        name = f"{base} #{idx_in_type // len(pool) + 1}"
    used.add(name)
    return name


def gen_facilities(batch_id: str, rng: random.Random, n: int) -> List[Facility]:
    # Deterministic, pool-driven type allocation: fill DCs first (exhausting
    # REAL_DC_NAMES), then Ports, then Cross-docks. At n == DEFAULT_N_FACILITIES
    # the pools fill exactly with zero collisions. For larger n, types cycle
    # back to DC and _pick_unique_name appends a numeric suffix.
    pool_for_type = {
        "DC": REAL_DC_NAMES,
        "Port": REAL_PORT_NAMES,
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
        # past the natural total — cycle by type_order at the overflow position
        overflow = i - sum(len(pool_for_type[t]) for t in type_order)
        return type_order[overflow % len(type_order)]

    facilities: List[Facility] = []
    for i in range(n):
        facility_id = f"Facility_{i + 1:04d}"
        facility_type = type_for_slot(i)
        facility_name = _pick_unique_name(
            pool_for_type[facility_type], used_names, per_type_idx[facility_type]
        )
        per_type_idx[facility_type] += 1

        lat = rng.uniform(25.0, 49.0)
        lon = rng.uniform(-124.0, -67.0)
        region_id = f"Z_{rng.randint(1, 250):03d}"

        site_mean = rng.uniform(800, 5000)
        site_std = rng.uniform(150, 900)
        current_units = max(0.0, rng.gauss(site_mean, site_std))
        throughput_z = (current_units - site_mean) / (
            site_std if site_std > 1e-9 else 1.0
        )
        status_flag = "Bottleneck" if throughput_z < -2.0 else "Normal"

        facilities.append(
            Facility(
                batch_id=batch_id,
                facility_id=facility_id,
                facility_type=facility_type,
                facility_name=facility_name,
                region_id=region_id,
                geo_point={"lat": round(lat, 6), "lon": round(lon, 6)},
                throughput_z=round(throughput_z, 4),
                status_flag=status_flag,
            )
        )
    return facilities


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
        partner_id = f"P_{i + 1:04d}"
        partner_name = f"Partner_{i + 1:03d}"  # internal code, unchanged
        partner_display_name = _pick_unique_name(REAL_PARTNER_NAMES, used_names, i)
        service_zone_id = f"Z_{rng.randint(1, 250):03d}"
        cap = clamp(rng.gauss(0.0, 1.2), -3.5, 3.5)
        succ = clamp(rng.gauss(0.0, 1.0), -3.5, 3.5)
        out.append(
            DeliveryPartner(
                batch_id=batch_id,
                partner_id=partner_id,
                partner_name=partner_name,
                partner_display_name=partner_display_name,
                service_zone_id=service_zone_id,
                capacity_z=round(cap, 3),
                success_rate_z=round(succ, 3),
            )
        )
    return out


def gen_routes(
    batch_id: str, rng: random.Random, facilities: List[Facility], n: int
) -> List[Route]:
    routes: List[Route] = []
    fac_ids = [f.facility_id for f in facilities]
    id_to_name = {f.facility_id: f.facility_name for f in facilities}
    for _ in range(n):
        origin = rng.choice(fac_ids)
        dest = rng.choice([x for x in fac_ids if x != origin])
        route_id = f"{origin}__{dest}"
        route_name = f"{id_to_name.get(origin, origin)} → {id_to_name.get(dest, dest)}"

        weather = clamp(rng.random() + rng.gauss(0, 0.1), 0, 1)
        traffic = clamp(rng.random() + rng.gauss(0, 0.1), 0, 1)
        history = clamp(rng.random() + rng.gauss(0, 0.1), 0, 1)
        risk = 0.35 * weather + 0.35 * traffic + 0.30 * history

        routes.append(
            Route(
                batch_id=batch_id,
                route_id=route_id,
                origin_id=origin,
                dest_id=dest,
                route_name=route_name,
                risk_score=round(risk, 4),
            )
        )

    uniq: Dict[str, Route] = {r.route_id: r for r in routes}
    return list(uniq.values())


def tier_weight(tier: str) -> float:
    return {"Platinum": 1.0, "Strategic": 0.85, "Gold": 0.65, "Standard": 0.40}.get(
        tier, 0.40
    )


def gen_orders(
    batch_id: str, rng: random.Random, n: int, now: datetime, erp_source: str
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
        order_id = f"order_{i + 1:06d}"
        tier = weighted_choice(rng, tiers)
        value = round(raw_values[i], 2)

        created_at = now - timedelta(days=rng.randint(0, 30), hours=rng.randint(0, 23))
        original_promise = created_at + timedelta(days=rng.randint(2, 10))
        renegotiate = rng.random() < 0.25
        target_delivery = (
            original_promise + timedelta(days=rng.randint(1, 5))
            if renegotiate
            else original_promise
        )
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
                order_id=order_id,
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


EVENT_TYPES = [
    "LABEL_CREATED",
    "PICKED",
    "GATE_OUT",
    "IN_TRANSIT",
    "GATE_IN",
    "OUT_FOR_DELIVERY",
    "DELIVERED",
    "EXCEPTION",
]

EXCEPTION_REASONS: List[Tuple[str, float]] = [
    ("WEATHER", 0.18),
    ("CUSTOMS", 0.12),
    ("DAMAGE", 0.10),
    ("ADDRESS", 0.18),
    ("CAPACITY", 0.22),
    ("OTHER", 0.20),
]


def canonical_event_type(src: str) -> str:
    return src


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
    # EXCEPTION (default to route)
    return route_id, "ROUTE"


# ============================
# Delay injection (raw timeline manipulation)
# ============================
# We keep the raw Event schema realistic: no synthetic reason fields are written.
# The only thing scenario injection does is create a larger timestamp gap between
# two real events. Your later derived layer can then detect that gap using
# delta_t_hrs + StageBaseline.

DelayPlace = str  # "FACILITY"|"ROUTE"|"PARTNER"|"MIXED"


def _normalize_delay_place(configured: DelayPlace) -> str:
    val = (configured or "MIXED").strip().upper()
    return val if val in {"FACILITY", "ROUTE", "PARTNER", "MIXED"} else "MIXED"


def delay_target_candidates(
    seq: List[str],
    delay_place: DelayPlace,
    *,
    partner_id: Optional[str],
) -> List[Tuple[int, str]]:
    """
    Return event indexes where an extra timestamp gap can be injected.

    The delay is added to the current event's step duration, so the inflated
    delta_t_hrs represents the gap from previous event -> current event.

    Examples the derived layer can later compute:
    - PICKED: label created -> warehouse pickup took unusually long
    - GATE_OUT: picked -> dispatch from origin facility took unusually long
    - IN_TRANSIT: origin dispatch -> transit scan took unusually long
    - GATE_IN: transit -> destination facility receiving took unusually long
    - DELIVERED: out for delivery -> final delivery took unusually long
    """
    place = _normalize_delay_place(delay_place)
    out: List[Tuple[int, str]] = []

    for idx, et in enumerate(seq):
        if idx == 0:
            continue

        # Origin / destination facility-related timestamp gaps.
        if place in {"FACILITY", "MIXED"} and et in {"PICKED", "GATE_OUT", "GATE_IN"}:
            out.append((idx, "FACILITY"))

        # Route / line-haul timestamp gaps.
        if place in {"ROUTE", "MIXED"} and et == "IN_TRANSIT":
            out.append((idx, "ROUTE"))

        # Last-mile timestamp gaps. Prefer DELIVERED because it captures
        # OUT_FOR_DELIVERY -> DELIVERED, which is the true last-mile dwell.
        if (
            place in {"PARTNER", "MIXED"}
            and partner_id
            and et in {"OUT_FOR_DELIVERY", "DELIVERED"}
        ):
            out.append((idx, "PARTNER"))

    return out


def pick_delay_target(
    rng: random.Random,
    seq: List[str],
    delay_place_config: DelayPlace,
    *,
    partner_id: Optional[str],
) -> Tuple[Optional[int], str]:
    """Pick one concrete event-to-event gap to delay."""
    configured = _normalize_delay_place(delay_place_config)
    candidates = delay_target_candidates(seq, configured, partner_id=partner_id)

    # If a scenario asks for PARTNER but this shipment has no partner or no
    # eligible partner event, fall back to mixed instead of silently no-oping.
    if not candidates and configured != "MIXED":
        candidates = delay_target_candidates(seq, "MIXED", partner_id=partner_id)

    if not candidates:
        return None, "NONE"

    return rng.choice(candidates)


def recompute_delta_t_for_shipment(events: List[Event], shipment_id: str) -> None:
    """Keep delta_t_hrs consistent after any timestamp adjustment."""
    prev: Optional[datetime] = None
    for ev in [e for e in events if e.shipment_id == shipment_id]:
        cur = datetime.fromisoformat(ev.timestamp_iso)
        ev.delta_t_hrs = (
            0.0 if prev is None else round((cur - prev).total_seconds() / 3600.0, 4)
        )
        prev = cur


def maybe_insert_exception(rng: random.Random, exception_pct: float) -> bool:
    return rng.random() < max(0.0, min(1.0, exception_pct))


def choose_exception_reason(rng: random.Random, bias: Optional[str]) -> str:
    if bias and bias in {x for x, _ in EXCEPTION_REASONS} and rng.random() < 0.70:
        return bias
    return weighted_choice(rng, EXCEPTION_REASONS)


def gen_shipments_and_events(
    batch_id: str,
    rng: random.Random,
    now: datetime,
    orders: List[Order],
    facilities: List[Facility],
    routes: List[Route],
    carriers: List[Carrier],
    partners: List[DeliveryPartner],
    min_events_per_shipment: int,
    max_events_per_shipment: int,
    *,
    delay_pct: float,
    delay_place_config: DelayPlace,
    delay_hours_range: Tuple[int, int],
    exception_pct: float,
    exception_bias: Optional[str],
    pct_delivered_late: float = 0.20,
) -> Tuple[List[Shipment], List[Event]]:
    carriers_ids = [c.carrier_id for c in carriers]
    route_by_origin: Dict[str, List[Route]] = {}
    for r in routes:
        route_by_origin.setdefault(r.origin_id, []).append(r)

    shipments: List[Shipment] = []
    events: List[Event] = []

    # used for dwell_time_sigma (origin facility baseline)
    fac_dwell_mu_sigma: Dict[str, Tuple[float, float]] = {}
    for f in facilities:
        fac_dwell_mu_sigma[f.facility_id] = (
            rng.uniform(2.0, 24.0),
            rng.uniform(0.8, 6.0),
        )

    n_orders = len(orders)
    n_delayed = int(round(n_orders * max(0.0, min(1.0, delay_pct))))
    delayed_set = (
        set(rng.sample(range(n_orders), k=n_delayed)) if n_delayed > 0 else set()
    )

    # Outcome split: a fraction are forced to deliver LATE (full sequence ending in
    # DELIVERED, timestamp pushed past target_delivery_dt). The rest stay OPEN —
    # truncated mid-flow at a random stage so they appear "stuck somewhere".
    n_late_delivered = int(round(n_orders * max(0.0, min(1.0, pct_delivered_late))))
    late_idxs = (
        rng.sample(range(n_orders), k=n_late_delivered) if n_late_delivered > 0 else []
    )
    late_delivered_set = set(late_idxs)

    lo_h, hi_h = int(delay_hours_range[0]), int(delay_hours_range[1])
    if hi_h < lo_h:
        lo_h, hi_h = hi_h, lo_h

    for i, o in enumerate(orders):
        shipment_id = f"S_{i + 1:06d}"
        origin_fac = rng.choice(facilities).facility_id
        route = rng.choice(route_by_origin.get(origin_fac) or routes)
        dest_fac = route.dest_id
        carrier_id = rng.choice(carriers_ids)
        partner_id = (
            rng.choice(partners).partner_id
            if partners and rng.random() < 0.70
            else None
        )
        tracking_number = f"TRK{rng.randint(10**11, 10**12 - 1)}"

        started_at = now - timedelta(
            days=rng.randint(0, 10),
            hours=rng.randint(0, 23),
            minutes=rng.randint(0, 59),
        )
        # base sequence (without DELIVERED — appended only for late-delivered orders)
        base_seq_open = [
            "LABEL_CREATED",
            "PICKED",
            "GATE_OUT",
            "IN_TRANSIT",
            "GATE_IN",
            "OUT_FOR_DELIVERY",
        ]
        is_late_delivered = i in late_delivered_set

        if is_late_delivered:
            # Full path through OUT_FOR_DELIVERY → DELIVERED, possibly extended
            seq = base_seq_open + ["DELIVERED"]
            n_events = rng.randint(len(seq), max(len(seq), max_events_per_shipment))
            while len(seq) < n_events:
                insert = rng.choice(["IN_TRANSIT", "EXCEPTION"])
                pos = rng.randint(3, max(3, len(seq) - 2))
                seq.insert(pos, insert)
        else:
            # Open: truncated mid-flow at a random stage (stuck somewhere)
            n_events = rng.randint(
                max(1, min_events_per_shipment),
                max(1, min(len(base_seq_open), max_events_per_shipment)),
            )
            seq = base_seq_open[:n_events]
            # Small chance the last event got swapped to an EXCEPTION
            if seq and rng.random() < 0.10:
                seq[-1] = "EXCEPTION"

        # maybe add an EXCEPTION even if already present in seq
        add_exception = maybe_insert_exception(rng, exception_pct)
        if add_exception and "EXCEPTION" not in seq:
            pos = rng.randint(3, max(3, len(seq) - 2))
            seq.insert(pos, "EXCEPTION")

        # per-shipment delay plan (raw): pick ONE concrete event-to-event gap.
        # We do not write the chosen target/reason into Event rows. The raw data
        # only shows realistic timestamps and delta_t_hrs.
        is_delayed = i in delayed_set
        delay_target_idx: Optional[int] = None
        chosen_place = "NONE"
        extra_delay = timedelta(0)
        if is_delayed:
            delay_target_idx, chosen_place = pick_delay_target(
                rng, seq, delay_place_config, partner_id=partner_id
            )
            if delay_target_idx is not None:
                extra_delay = timedelta(hours=rng.randint(lo_h, hi_h))

        t = started_at
        prev_t: Optional[datetime] = None

        # route transit ETA base
        route_transit_mean_hrs = clamp(
            rng.gauss(18, 6) * (1.0 + route.risk_score), 4, 120
        )

        # shipment-level dwell sigma at origin
        mu, sig = fac_dwell_mu_sigma.get(origin_fac, (8.0, 2.0))
        current_dwell = max(0.0, rng.gauss(mu, sig))
        dwell_sigma = (current_dwell - mu) / (sig if sig > 1e-9 else 1.0)

        for event_idx, et in enumerate(seq):
            # baseline step time
            if et == "LABEL_CREATED":
                dt = timedelta(minutes=rng.randint(0, 180))
            elif et == "PICKED":
                dt = timedelta(hours=rng.uniform(0.5, 8.0))
            elif et == "GATE_OUT":
                dt = timedelta(hours=rng.uniform(0.2, 3.0))
            elif et == "IN_TRANSIT":
                dt = timedelta(hours=rng.uniform(2.0, 20.0))
            elif et == "GATE_IN":
                dt = timedelta(hours=rng.uniform(1.0, 10.0))
            elif et == "OUT_FOR_DELIVERY":
                dt = timedelta(hours=rng.uniform(0.5, 6.0))
            elif et == "DELIVERED":
                dt = timedelta(hours=rng.uniform(0.2, 8.0))
            else:  # EXCEPTION or unknown
                dt = timedelta(hours=rng.uniform(0.1, 12.0))

            # Inject delay only into the selected event-to-event gap.
            # Example: if event_idx points to GATE_OUT, then PICKED -> GATE_OUT
            # becomes unusually long, and the derived layer can flag dispatch dwell.
            if delay_target_idx is not None and event_idx == delay_target_idx:
                dt = dt + extra_delay

            t = t + dt
            delta_hrs = 0.0 if prev_t is None else (t - prev_t).total_seconds() / 3600.0

            event_id = str(uuid.uuid4())
            event_type = canonical_event_type(et)
            ts = iso(t)

            source_id = "SYNTH"
            audit = sha256_hex(f"{shipment_id}|{event_type}|{ts}|{source_id}")

            exception_reason = (
                choose_exception_reason(rng, exception_bias)
                if event_type == "EXCEPTION"
                else None
            )

            loc_id, loc_kind = event_location(
                event_type,
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
                    event_type=event_type,
                    timestamp_iso=ts,
                    audit_hash=audit,
                    delta_t_hrs=round(delta_hrs, 4),
                    location_id=loc_id,
                    location_kind=loc_kind,
                    exception_reason=exception_reason,
                )
            )
            prev_t = t

        # Late-delivered: ensure the final DELIVERED event lands past target_delivery_dt
        # without breaking timeline order, then recompute delta_t_hrs.
        if is_late_delivered:
            target_dt = datetime.fromisoformat(o.target_delivery_dt).replace(
                tzinfo=timezone.utc
            )
            deadline = datetime.combine(
                target_dt.date(),
                datetime.max.time().replace(microsecond=0),
                tzinfo=timezone.utc,
            )
            candidate_delivered = deadline + timedelta(hours=rng.randint(1, 72))

            shipment_events = [ev for ev in events if ev.shipment_id == shipment_id]
            prior_ts: Optional[datetime] = None
            if len(shipment_events) >= 2:
                prior_ts = datetime.fromisoformat(shipment_events[-2].timestamp_iso)

            if prior_ts is not None and candidate_delivered <= prior_ts:
                candidate_delivered = prior_ts + timedelta(hours=rng.uniform(0.5, 8.0))

            for ev in reversed(events):
                if ev.shipment_id == shipment_id and ev.event_type == "DELIVERED":
                    ev.timestamp_iso = iso(candidate_delivered)
                    ev.audit_hash = sha256_hex(
                        f"{shipment_id}|DELIVERED|{ev.timestamp_iso}|SYNTH"
                    )
                    prev_t = candidate_delivered
                    break

        recompute_delta_t_for_shipment(events, shipment_id)

        # shipment ETA
        last_event_ts = prev_t if prev_t is not None else started_at
        arrival_eta = last_event_ts + timedelta(hours=route_transit_mean_hrs)

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

        # update order estimated_delivery_dt (derived)
        last_mile_days = rng.uniform(0.5, 2.5)
        est = arrival_eta + timedelta(days=last_mile_days)
        o.estimated_delivery_dt = est.date().isoformat()

        # update SLA health (derived)
        target_dt = datetime.fromisoformat(o.target_delivery_dt).replace(
            tzinfo=timezone.utc
        )
        if now.date() > target_dt.date():
            o.sla_health = "Critical"

    return shipments, events


# ============================
# Main
# ============================
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Seed scenarios for NEW schema (outputs jsonl tables for upsert)."
    )
    ap.add_argument("--list", action="store_true", help="List scenarios and exit")
    ap.add_argument(
        "--scenario", default="S1_BASELINE_MOSTLY_ONTIME", help="Scenario key"
    )
    ap.add_argument("--out", default="data", help="Output directory")
    ap.add_argument("--seed", type=int, default=7, help="RNG seed")
    ap.add_argument(
        "--batch_id",
        default="",
        help="Optional batch id (default synth_<utc>_seed<seed>)",
    )
    ap.add_argument("--erp_source", default="ORACLE", help='e.g., "ORACLE"')

    # Core generator sizes
    ap.add_argument(
        "--n_facilities",
        type=int,
        default=DEFAULT_N_FACILITIES,
        help=f"default {DEFAULT_N_FACILITIES} — exactly fills the name pools "
        f"(DC={len(REAL_DC_NAMES)}, Port={len(REAL_PORT_NAMES)}, "
        f"CrossDock={len(REAL_CROSSDOCK_NAMES)})",
    )
    ap.add_argument(
        "--n_partners",
        type=int,
        default=DEFAULT_N_PARTNERS,
        help=f"default {DEFAULT_N_PARTNERS} — exactly fills REAL_PARTNER_NAMES",
    )
    ap.add_argument("--n_routes", type=int, default=60)

    # Optional overrides (user doesn't have to provide)
    ap.add_argument(
        "--n_orders",
        type=int,
        default=0,
        help="Override scenario n_orders (0=use preset)",
    )
    ap.add_argument(
        "--delay_pct",
        type=float,
        default=-1.0,
        help="Override scenario delay_pct (0..1)",
    )
    ap.add_argument(
        "--delay_place",
        default="",
        help="Override delay place: FACILITY|ROUTE|PARTNER|MIXED (blank=use preset)",
    )
    ap.add_argument(
        "--delay_hours_lo",
        type=int,
        default=0,
        help="Override delay hours low (0=use preset)",
    )
    ap.add_argument(
        "--delay_hours_hi",
        type=int,
        default=0,
        help="Override delay hours high (0=use preset)",
    )
    ap.add_argument(
        "--exception_pct",
        type=float,
        default=-1.0,
        help="Override exception_pct (0..1)",
    )
    ap.add_argument(
        "--exception_bias",
        default="",
        help="Override exception bias: WEATHER|CUSTOMS|... (blank=use preset)",
    )

    # Events range
    ap.add_argument("--min_events", type=int, default=1)
    ap.add_argument("--max_events", type=int, default=6)
    ap.add_argument(
        "--pct_delivered_late",
        type=float,
        default=0.20,
        help="Fraction of orders forced to deliver late; rest stay open (0..1)",
    )

    args = ap.parse_args()

    if args.list:
        for k, label in list_scenarios():
            print(f"{k}: {label}")
        return

    if args.scenario not in SCENARIOS:
        keys = ", ".join([k for k, _ in list_scenarios()])
        raise SystemExit(
            f"Unknown scenario '{args.scenario}'. Use --list. Valid: {keys}"
        )

    preset = dict(SCENARIOS[args.scenario])

    # Apply minimal optional overrides
    if args.n_orders and args.n_orders > 0:
        preset["n_orders"] = int(args.n_orders)
    if args.delay_pct >= 0.0:
        preset["delay_pct"] = float(clamp(args.delay_pct, 0.0, 1.0))
    if args.delay_place.strip():
        preset["delay_place"] = args.delay_place.strip().upper()
    if args.delay_hours_lo > 0 or args.delay_hours_hi > 0:
        lo = (
            int(args.delay_hours_lo)
            if args.delay_hours_lo > 0
            else int(preset["delay_hours_range"][0])
        )
        hi = (
            int(args.delay_hours_hi)
            if args.delay_hours_hi > 0
            else int(preset["delay_hours_range"][1])
        )
        preset["delay_hours_range"] = (lo, hi)
    if args.exception_pct >= 0.0:
        preset["exception_pct"] = float(clamp(args.exception_pct, 0.0, 1.0))
    if args.exception_bias.strip():
        preset["exception_bias"] = args.exception_bias.strip().upper()

    rng = random.Random(args.seed)
    now = utc_now()

    batch_id = (args.batch_id or "").strip()
    if not batch_id:
        batch_id = f"synth_{now.strftime('%Y%m%dT%H%M%SZ')}_seed{args.seed}"

    out_dir = Path(args.out).expanduser().resolve()
    ensure_dir(out_dir)

    facilities = gen_facilities(batch_id, rng, args.n_facilities)
    carriers = gen_carriers(batch_id, rng)
    partners = gen_partners(batch_id, rng, args.n_partners)
    routes = gen_routes(batch_id, rng, facilities, args.n_routes)
    orders = gen_orders(batch_id, rng, int(preset["n_orders"]), now, args.erp_source)

    shipments, events = gen_shipments_and_events(
        batch_id=batch_id,
        rng=rng,
        now=now,
        orders=orders,
        facilities=facilities,
        routes=routes,
        carriers=carriers,
        partners=partners,
        min_events_per_shipment=int(args.min_events),
        max_events_per_shipment=int(args.max_events),
        delay_pct=float(preset["delay_pct"]),
        delay_place_config=str(preset["delay_place"]).upper(),
        delay_hours_range=tuple(preset["delay_hours_range"]),
        exception_pct=float(preset["exception_pct"]),
        exception_bias=preset.get("exception_bias") or None,
        pct_delivered_late=float(args.pct_delivered_late),
    )

    # Write tables (matches your upsert reader)
    write_json(out_dir / "schema_definition.json", SCHEMA_DEFINITION)
    write_jsonl(out_dir / "facilities.jsonl", [asdict(x) for x in facilities])
    write_jsonl(out_dir / "carriers.jsonl", [asdict(x) for x in carriers])
    write_jsonl(out_dir / "partners.jsonl", [asdict(x) for x in partners])
    write_jsonl(out_dir / "routes.jsonl", [asdict(x) for x in routes])
    write_jsonl(out_dir / "orders.jsonl", [asdict(x) for x in orders])
    write_jsonl(out_dir / "shipments.jsonl", [asdict(x) for x in shipments])
    write_jsonl(out_dir / "events.jsonl", [asdict(x) for x in events])

    manifest = {
        "batch_id": batch_id,
        "scenario_key": args.scenario,
        "scenario_label": preset.get("label"),
        "seed": args.seed,
        "reference_now_utc": iso(now),
        "scenario_params_used": {
            "n_orders": preset["n_orders"],
            "delay_pct": preset["delay_pct"],
            "delay_place": preset["delay_place"],
            "delay_hours_range": list(preset["delay_hours_range"]),
            "exception_pct": preset["exception_pct"],
            "exception_bias": preset.get("exception_bias"),
            "delay_model": "event_to_event_gap_injection_no_reason_fields",
        },
        "counts": {
            "facilities": len(facilities),
            "carriers": len(carriers),
            "partners": len(partners),
            "routes": len(routes),
            "orders": len(orders),
            "shipments": len(shipments),
            "events": len(events),
        },
        "args": {
            "erp_source": args.erp_source,
            "n_facilities": args.n_facilities,
            "n_partners": args.n_partners,
            "n_routes": args.n_routes,
            "min_events": args.min_events,
            "max_events": args.max_events,
            "out": str(out_dir),
        },
    }
    write_json(out_dir / "run_manifest.json", manifest)

    print("✅ Scenario seeded (NEW schema)")
    print("batch_id:", batch_id)
    print("scenario:", args.scenario, "|", preset.get("label", ""))
    print("Output dir:", str(out_dir))
    print("Counts:", manifest["counts"])
    print("\nNext:")
    print(f"  python upsert_newschema.py --data_dir {out_dir} --batch_id {batch_id}")
    print("  (then run KPI hydration/monitor + RCA)")


if __name__ == "__main__":
    main()
