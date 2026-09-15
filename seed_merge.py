from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from dotenv import load_dotenv
from neo4j import GraphDatabase

from seed_scenario import (
    DEFAULT_N_FACILITIES,
    DEFAULT_N_PARTNERS,
    SCENARIOS,
    ensure_dir,
    gen_carriers,
    gen_facilities,
    gen_orders,
    gen_partners,
    gen_routes,
    gen_shipments_and_events,
    iso,
    utc_now,
    write_json,
    write_jsonl,
)
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
    req_env,
)


# ------------------------------------------------------------
# Generalized pipeline entrypoint
# ------------------------------------------------------------

def load_env_auto() -> None:
    p = Path(__file__).resolve()
    for parent in [p.parent] + list(p.parents):
        env_path = parent / ".env"
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)
            return
    load_dotenv()


load_env_auto()


def build_seed_batch(
    *,
    scenario_key: str,
    out_dir: str | Path,
    seed: int,
    batch_id: str | None = None,
    erp_source: str = "ORACLE",
    n_facilities: int = DEFAULT_N_FACILITIES,
    n_partners: int = DEFAULT_N_PARTNERS,
    n_routes: int = 60,
    min_events: int = 1,
    max_events: int = 6,
    pct_delivered_late: float = 0.20,
) -> Dict[str, Any]:
    """Generate a dataset from a scenario and write it to JSONL files."""
    import random

    if scenario_key not in SCENARIOS:
        valid = ", ".join(sorted(SCENARIOS))
        raise ValueError(f"Unknown scenario '{scenario_key}'. Valid: {valid}")

    preset = dict(SCENARIOS[scenario_key])
    rng = random.Random(seed)
    now = utc_now()
    resolved_batch_id = (batch_id or "").strip() or (
        f"synth_{now.strftime('%Y%m%dT%H%M%SZ')}_seed{seed}"
    )

    out_path = Path(out_dir).expanduser().resolve()
    ensure_dir(out_path)

    facilities = gen_facilities(resolved_batch_id, rng, n_facilities)
    carriers = gen_carriers(resolved_batch_id, rng)
    partners = gen_partners(resolved_batch_id, rng, n_partners)
    routes = gen_routes(resolved_batch_id, rng, facilities, n_routes)
    orders = gen_orders(
        resolved_batch_id,
        rng,
        int(preset["n_orders"]),
        now,
        erp_source,
    )
    shipments, events = gen_shipments_and_events(
        batch_id=resolved_batch_id,
        rng=rng,
        now=now,
        orders=orders,
        facilities=facilities,
        routes=routes,
        carriers=carriers,
        partners=partners,
        min_events_per_shipment=int(min_events),
        max_events_per_shipment=int(max_events),
        delay_pct=float(preset["delay_pct"]),
        delay_place_config=str(preset["delay_place"]).upper(),
        delay_hours_range=tuple(preset["delay_hours_range"]),
        exception_pct=float(preset["exception_pct"]),
        exception_bias=preset.get("exception_bias") or None,
        pct_delivered_late=float(pct_delivered_late),
    )

    write_jsonl(out_path / "facilities.jsonl", [item.__dict__ for item in facilities])
    write_jsonl(out_path / "carriers.jsonl", [item.__dict__ for item in carriers])
    write_jsonl(out_path / "partners.jsonl", [item.__dict__ for item in partners])
    write_jsonl(out_path / "routes.jsonl", [item.__dict__ for item in routes])
    write_jsonl(out_path / "orders.jsonl", [item.__dict__ for item in orders])
    write_jsonl(out_path / "shipments.jsonl", [item.__dict__ for item in shipments])
    write_jsonl(out_path / "events.jsonl", [item.__dict__ for item in events])

    manifest = {
        "batch_id": resolved_batch_id,
        "scenario_key": scenario_key,
        "scenario_label": preset.get("label"),
        "seed": seed,
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
            "erp_source": erp_source,
            "n_facilities": n_facilities,
            "n_partners": n_partners,
            "n_routes": n_routes,
            "min_events": min_events,
            "max_events": max_events,
            "out": str(out_path),
        },
    }
    write_json(out_path / "run_manifest.json", manifest)

    return {"batch_id": resolved_batch_id, "manifest": manifest, "output_dir": str(out_path)}


def read_jsonl(path: Path, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for idx, line in enumerate(handle):
            if limit is not None and idx >= limit:
                break
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def filter_batch(rows: List[Dict[str, Any]], batch_id: str) -> List[Dict[str, Any]]:
    if not batch_id:
        return rows
    return [row for row in rows if row.get("batch_id") == batch_id]


def norm_facilities(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        geo = row.get("geo_point") or {}
        out.append(
            {
                "batch_id": row.get("batch_id"),
                "facility_id": row.get("facility_id"),
                "facility_type": row.get("facility_type"),
                "region_id": row.get("region_id"),
                "geo_lat": geo.get("lat"),
                "geo_lon": geo.get("lon"),
                "throughput_z": row.get("throughput_z"),
                "status_flag": row.get("status_flag"),
            }
        )
    return out


def pick(rows: List[Dict[str, Any]], keys: List[str]) -> List[Dict[str, Any]]:
    return [{key: row.get(key) for key in keys} for row in rows]


def neo4j_driver():
    uri = req_env("NEO4J_URI")
    user = req_env("NEO4J_USERNAME")
    password = req_env("NEO4J_PASSWORD")
    return GraphDatabase.driver(uri, auth=(user, password))


def upsert_data_to_neo4j(*, data_dir: str | Path, batch_id: str = "") -> Dict[str, int]:
    """Push a generic batch of seed-generated JSONL data into Neo4j."""
    data_path = Path(data_dir).expanduser().resolve()
    now = iso(utc_now())

    facilities_raw = read_jsonl(data_path / "facilities.jsonl")
    carriers_raw = read_jsonl(data_path / "carriers.jsonl")
    partners_raw = read_jsonl(data_path / "partners.jsonl")
    routes_raw = read_jsonl(data_path / "routes.jsonl")
    orders_raw = read_jsonl(data_path / "orders.jsonl")
    shipments_raw = read_jsonl(data_path / "shipments.jsonl")
    events_raw = read_jsonl(data_path / "events.jsonl")

    facilities = filter_batch(norm_facilities(facilities_raw), batch_id)
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
        batch_id,
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
        batch_id,
    )
    routes = filter_batch(
        pick(routes_raw, ["batch_id", "route_id", "origin_id", "dest_id", "risk_score"]),
        batch_id,
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
        batch_id,
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
        batch_id,
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
        batch_id,
    )

    driver = neo4j_driver()
    try:
        with driver.session() as session:
            for statement in CONSTRAINTS:
                session.run(statement)

            def upsert_rows(query: str, rows: List[Dict[str, Any]], label: str) -> None:
                for chunk in chunked(rows, 1000):
                    session.run(query, rows=chunk, now=now)
                print(f"✅ upserted {label}: {len(rows)}")

            upsert_rows(CYPHER_UPSERT_FACILITIES, facilities, "Facility (+ Region)")
            upsert_rows(CYPHER_UPSERT_CARRIERS, carriers, "Carrier")
            upsert_rows(CYPHER_UPSERT_PARTNERS, partners, "DeliveryPartner")
            upsert_rows(CYPHER_UPSERT_ROUTES, routes, "Route")
            upsert_rows(CYPHER_UPSERT_ORDERS, orders, "Order")
            upsert_rows(CYPHER_UPSERT_SHIPMENTS, shipments, "Shipment")
            upsert_rows(CYPHER_UPSERT_SHIPMENT_RELS, shipments, "Shipment relationships")

            order_ids = sorted({row.get("order_id") for row in orders if row.get("order_id")})
            if order_ids:
                session.run(CYPHER_WIPE_TIMELINE_FOR_ORDERS, order_ids=order_ids).consume()
                print(f"🗑  wiped prior events for {len(order_ids)} order_ids")

            upsert_rows(CYPHER_UPSERT_EVENTS, events, "Event")
            upsert_rows(CYPHER_UPSERT_EVENT_RELS, events, "Event relationships")

            session.run(
                CYPHER_ENRICH_FACILITY_NAMES,
                bid=batch_id or "",
                dc_names=REAL_DC_NAMES,
                port_names=REAL_PORT_NAMES,
                crossdock_names=REAL_CROSSDOCK_NAMES,
            )
            session.run(
                CYPHER_ENRICH_PARTNER_NAMES,
                bid=batch_id or "",
                partner_names=REAL_PARTNER_NAMES,
            )
            session.run(CYPHER_ENRICH_ROUTE_NAMES, bid=batch_id or "")

            rec = session.run(CYPHER_UPSERT_STAGE_BASELINES, now=now).single()
            print(
                "✅ StageBaseline upserted:",
                int((rec and rec.get("n_baselines")) or 0),
                "groups",
            )
    finally:
        driver.close()

    return {
        "facilities": len(facilities),
        "carriers": len(carriers),
        "partners": len(partners),
        "routes": len(routes),
        "orders": len(orders),
        "shipments": len(shipments),
        "events": len(events),
    }


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Generalized pipeline: generate canonical seed data and push it to Neo4j "
            "without tying the workflow to Shippo or Odoo-specific adapters."
        )
    )
    ap.add_argument("--mode", choices=["full", "generate", "upsert"], default="full")
    ap.add_argument("--scenario", default="S1_BASELINE_MOSTLY_ONTIME")
    ap.add_argument("--data_dir", default="data")
    ap.add_argument("--batch_id", default="")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", default="data")
    ap.add_argument("--erp_source", default="ORACLE")
    ap.add_argument("--n_facilities", type=int, default=DEFAULT_N_FACILITIES)
    ap.add_argument("--n_partners", type=int, default=DEFAULT_N_PARTNERS)
    ap.add_argument("--n_routes", type=int, default=60)
    ap.add_argument("--min_events", type=int, default=1)
    ap.add_argument("--max_events", type=int, default=6)
    ap.add_argument("--pct_delivered_late", type=float, default=0.20)
    args = ap.parse_args()

    if args.mode in {"full", "generate"}:
        result = build_seed_batch(
            scenario_key=args.scenario,
            out_dir=args.out,
            seed=args.seed,
            batch_id=args.batch_id,
            erp_source=args.erp_source,
            n_facilities=args.n_facilities,
            n_partners=args.n_partners,
            n_routes=args.n_routes,
            min_events=args.min_events,
            max_events=args.max_events,
            pct_delivered_late=args.pct_delivered_late,
        )
        print("✅ Seed data generated")
        print("batch_id:", result["batch_id"])
        print("output_dir:", result["output_dir"])
        print("counts:", result["manifest"]["counts"])

    if args.mode in {"full", "upsert"}:
        target_dir = args.out if args.mode == "full" else args.data_dir
        if args.mode == "full":
            batch = (args.batch_id or "").strip() or (
                json.loads((Path(args.out) / "run_manifest.json").read_text(encoding="utf-8"))
                .get("batch_id", "")
            )
        else:
            batch = args.batch_id
        print("📦 Upserting batch to Neo4j...")
        counts = upsert_data_to_neo4j(data_dir=target_dir, batch_id=batch)
        print("Neo4j load counts:", counts)


if __name__ == "__main__":
    main()
