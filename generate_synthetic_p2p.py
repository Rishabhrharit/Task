"""Generate synthetic P2P (Procure-to-Pay) cases and load them into Neo4j.

There is no real P2P source system yet, so this script fabricates whole
cases (PR -> PO -> ASN -> GR -> INVOICE -> PAYMENT) directly in the shape
DT_p2p_schema.json documents, and MERGEs them into Neo4j using the same
label/property conventions the analytical Cypher in o2c_opec.json /
o2c_patterns.json / o2c_anomalies.json already relies on:

    (:Entity:CASE_ROOT {case_id, process, status, opened_at, last_event_time})
    (:Entity:BUSINESS_OBJECT:<TYPE> {case_id, process, entity_id, ...flattened attrs})
    (:EVENT {process, case_id, node_id, event_type, event_time})

A configurable fraction of cases get one randomly-inflated stage duration so
z-score / baseline anomaly queries have something to find.

Usage:
    python generate_synthetic_p2p.py --cases 200 --process P2P
    python generate_synthetic_p2p.py --cases 50 --anomaly-rate 0.2 --wipe
"""

from __future__ import annotations

import argparse
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from neo4j import GraphDatabase

from config import load_project_env
import os

load_project_env()

SUPPLIERS = [f"SUP-{n}" for n in range(100, 130)]
MATERIALS = [f"MAT-{n}" for n in range(500, 540)]
BUYERS = [f"BUYER-{n}" for n in range(10, 40)]
COMPANY_CODES = ["US01", "US02", "EU01"]
PLANTS = ["COLUMBUS", "DALLAS", "BERLIN"]

# (stage_name, minutes_low, minutes_high) baseline durations, in minutes
STAGES = {
    "REQUISITION_APPROVAL": (60, 60 * 24 * 3),      # PR submitted -> approved
    "PROCUREMENT_LEAD_TIME": (60 * 6, 60 * 24 * 5),  # PO issued -> GR received
    "INVOICE_MATCH_CYCLE": (60, 60 * 24 * 2),        # GR received -> invoice posted
    "BILLING_SETTLEMENT": (60 * 24, 60 * 24 * 60),   # invoice posted -> payment cleared
}


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _minutes(mn: int, mx: int, inflate: bool) -> int:
    value = random.randint(mn, mx)
    return value * random.randint(4, 8) if inflate else value


@dataclass
class Case:
    case_id: str
    process: str
    nodes: List[Dict[str, Any]] = field(default_factory=list)
    edges: List[Dict[str, Any]] = field(default_factory=list)
    events: List[Dict[str, Any]] = field(default_factory=list)
    opened_at: Optional[str] = None
    last_event_time: Optional[str] = None
    status: str = "CLOSED"


def _add_node(case: Case, entity_type: str, entity_id: str, status: str, attrs: Dict[str, Any]) -> None:
    node = {
        "case_id": case.case_id,
        "process": case.process,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "status": status,
    }
    node.update(attrs)
    case.nodes.append(node)
    for key, value in attrs.items():
        if key.endswith("_at") or key.endswith("_date") and value:
            case.events.append(
                {
                    "process": case.process,
                    "case_id": case.case_id,
                    "node_id": f"{case.case_id}::{entity_type}::{entity_id}",
                    "event_type": f"{entity_type}_{key.upper()}",
                    "event_time": value,
                }
            )


def build_case(index: int, process: str, anomaly_rate: float, base_time: datetime) -> Case:
    case_id = f"{process}::PR-{2000 + index}"
    case = Case(case_id=case_id, process=process)
    inflate_stage = random.random() < anomaly_rate
    inflated = random.choice(list(STAGES)) if inflate_stage else None

    supplier = random.choice(SUPPLIERS)
    material = random.choice(MATERIALS)
    company = random.choice(COMPANY_CODES)
    plant = random.choice(PLANTS)
    amount = round(random.uniform(500, 120_000), 2)

    t = base_time + timedelta(days=random.randint(0, 250), hours=random.randint(0, 23))
    case.opened_at = _iso(t)

    pr_id = f"PR-{2000 + index}"
    submit_gap = _minutes(30, 60 * 8, False)
    approve_gap = _minutes(*STAGES["REQUISITION_APPROVAL"], inflated == "REQUISITION_APPROVAL")
    pr_submitted = t + timedelta(minutes=submit_gap)
    pr_approved = pr_submitted + timedelta(minutes=approve_gap)
    _add_node(
        case, "PR", pr_id, "APPROVED",
        {
            "created_at": _iso(t), "submitted_at": _iso(pr_submitted), "approved_at": _iso(pr_approved),
            "company_code": company, "plant_id": plant, "supplier_id": supplier, "material_id": material,
            "amount": amount, "currency": "USD",
        },
    )

    po_id = f"PO-{4500000 + index}"
    po_created = pr_approved + timedelta(minutes=_minutes(15, 120, False))
    po_issued = po_created + timedelta(minutes=_minutes(15, 90, False))
    lead_gap = _minutes(*STAGES["PROCUREMENT_LEAD_TIME"], inflated == "PROCUREMENT_LEAD_TIME")
    gr_received = po_issued + timedelta(minutes=lead_gap)
    _add_node(
        case, "PO", po_id, "ISSUED",
        {
            "created_at": _iso(po_created), "issued_at": _iso(po_issued),
            "company_code": company, "plant_id": plant, "supplier_id": supplier, "material_id": material,
            "amount": amount, "currency": "USD",
        },
    )
    case.edges.append({"relationship_type": "GENERATED_FROM", "from_id": po_id, "to_id": pr_id})

    asn_id = f"ASN-{8800 + index}"
    asn_created = po_issued + timedelta(minutes=_minutes(60, 60 * 12, False))
    _add_node(
        case, "ASN", asn_id, "IN_TRANSIT",
        {"created_at": _iso(asn_created), "company_code": company, "plant_id": plant, "supplier_id": supplier},
    )
    case.edges.append({"relationship_type": "FOLLOWS", "from_id": asn_id, "to_id": po_id})

    gr_id = f"GR-{70000 + index}"
    _add_node(
        case, "GR", gr_id, "RECEIVED",
        {
            "received_at": _iso(gr_received), "company_code": company, "plant_id": plant,
            "supplier_id": supplier, "material_id": material,
        },
    )
    case.edges.append({"relationship_type": "FOLLOWS", "from_id": gr_id, "to_id": po_id})
    case.edges.append({"relationship_type": "RECEIVED_AGAINST", "from_id": gr_id, "to_id": asn_id})

    match_gap = _minutes(*STAGES["INVOICE_MATCH_CYCLE"], inflated == "INVOICE_MATCH_CYCLE")
    inv_id = f"INV-{9900 + index}"
    invoice_date = gr_received + timedelta(minutes=_minutes(30, 60 * 6, False))
    posted_at = invoice_date + timedelta(minutes=match_gap)
    _add_node(
        case, "INVOICE", inv_id, "POSTED",
        {
            "invoice_date": _iso(invoice_date), "posted_at": _iso(posted_at),
            "company_code": company, "plant_id": plant, "supplier_id": supplier, "amount": amount,
            "currency": "USD",
        },
    )
    case.edges.append({"relationship_type": "MATCHED_TO", "from_id": inv_id, "to_id": po_id})
    case.edges.append({"relationship_type": "MATCHED_TO", "from_id": inv_id, "to_id": gr_id})

    settle_gap = _minutes(*STAGES["BILLING_SETTLEMENT"], inflated == "BILLING_SETTLEMENT")
    pay_id = f"PMT-{330000 + index}"
    cleared_at = posted_at + timedelta(minutes=settle_gap)
    _add_node(
        case, "PAYMENT", pay_id, "CLEARED",
        {
            "payment_date": _iso(posted_at), "cleared_at": _iso(cleared_at),
            "supplier_id": supplier, "amount": amount, "currency": "USD",
        },
    )
    case.edges.append({"relationship_type": "SETTLES", "from_id": pay_id, "to_id": inv_id})
    case.edges.append({"relationship_type": "REFERENCES", "from_id": pay_id, "to_id": po_id})

    case.last_event_time = _iso(cleared_at)
    return case


def generate_cases(n: int, process: str, anomaly_rate: float) -> List[Case]:
    base_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [build_case(i, process, anomaly_rate, base_time) for i in range(1, n + 1)]


def _neo4j_driver():
    uri = os.getenv("NEO4J_URI") or os.getenv("NEO4J_URL")
    user = os.getenv("NEO4J_USERNAME") or os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")
    if not uri or not user or not password:
        raise RuntimeError("Missing NEO4J_URI, NEO4J_USERNAME, or NEO4J_PASSWORD.")
    return GraphDatabase.driver(uri, auth=(user, password))


def load_cases(cases: List[Case], wipe: bool) -> None:
    driver = _neo4j_driver()
    now = _iso(datetime.now(timezone.utc))
    try:
        with driver.session() as session:
            if wipe:
                session.run(
                    "MATCH (n) WHERE n.process = $process DETACH DELETE n",
                    process=cases[0].process,
                )

            roots = [
                {
                    "case_id": c.case_id, "process": c.process, "status": c.status,
                    "opened_at": c.opened_at, "last_event_time": c.last_event_time,
                }
                for c in cases
            ]
            session.run(
                """
                UNWIND $rows AS row
                MERGE (r:Entity:CASE_ROOT {case_id: row.case_id})
                SET r += row, r.updated_at = $now
                """,
                rows=roots, now=now,
            )

            nodes_by_type: Dict[str, List[Dict[str, Any]]] = {}
            for c in cases:
                for node in c.nodes:
                    nodes_by_type.setdefault(node["entity_type"], []).append(node)
            for entity_type, rows in nodes_by_type.items():
                session.run(
                    f"""
                    UNWIND $rows AS row
                    MERGE (n:Entity:BUSINESS_OBJECT:{entity_type} {{case_id: row.case_id, entity_id: row.entity_id}})
                    SET n += row, n.updated_at = $now
                    """,
                    rows=rows, now=now,
                )
                session.run(
                    """
                    UNWIND $rows AS row
                    MATCH (r:Entity:CASE_ROOT {case_id: row.case_id})
                    MATCH (n:Entity:BUSINESS_OBJECT {case_id: row.case_id, entity_id: row.entity_id})
                    MERGE (r)-[:CONTAINS]->(n)
                    """,
                    rows=rows,
                )

            edges_by_type: Dict[str, List[Dict[str, Any]]] = {}
            for c in cases:
                for edge in c.edges:
                    edge = dict(edge)
                    edge["case_id"] = c.case_id
                    edges_by_type.setdefault(edge["relationship_type"], []).append(edge)
            for rel_type, rows in edges_by_type.items():
                session.run(
                    f"""
                    UNWIND $rows AS row
                    MATCH (a:Entity:BUSINESS_OBJECT {{case_id: row.case_id, entity_id: row.from_id}})
                    MATCH (b:Entity:BUSINESS_OBJECT {{case_id: row.case_id, entity_id: row.to_id}})
                    MERGE (a)-[:{rel_type}]->(b)
                    """,
                    rows=rows,
                )

            events = [e for c in cases for e in c.events]
            for i in range(0, len(events), 2000):
                session.run(
                    """
                    UNWIND $rows AS row
                    CREATE (ev:EVENT)
                    SET ev = row
                    """,
                    rows=events[i:i + 2000],
                )
    finally:
        driver.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", type=int, default=100, help="Number of synthetic P2P cases to generate")
    parser.add_argument("--process", default="P2P", help="Value stored in the process/process_type property")
    parser.add_argument("--anomaly-rate", type=float, default=0.15, help="Fraction of cases with one inflated stage")
    parser.add_argument("--wipe", action="store_true", help="Delete existing nodes with this process before loading")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducible runs")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    cases = generate_cases(args.cases, args.process, args.anomaly_rate)
    load_cases(cases, args.wipe)
    print(f"Loaded {len(cases)} synthetic {args.process} cases into Neo4j.")


if __name__ == "__main__":
    main()
