"""Generate synthetic P2P source records and store them locally as JSON.

This produces raw, ERP-shaped payloads for the 6 P2P business objects
(PR, PO, ASN, GR, INVOICE, PAYMENT), written under exports/synthetic/p2p/.
These files are served over HTTP by source_api/app/p2p_main.py so the real
ingestion pipeline (ingestion.generic_rest) can pull them exactly like it
would pull from a live ERP -- this is only a stand-in source, not a
replacement for API ingestion.

Usage:
    python generate_p2p_synthetic_source.py --cases 100 --anomaly-rate 0.15
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

OUTPUT_DIR = Path(__file__).resolve().parent / "exports" / "synthetic" / "p2p"

SUPPLIERS = [f"SUP-{n}" for n in range(100, 130)]
MATERIALS = [f"MAT-{n}" for n in range(500, 540)]
COMPANY_CODES = ["US01", "US02", "EU01"]
PLANTS = ["COLUMBUS", "DALLAS", "BERLIN"]

# (stage_name, minutes_low, minutes_high)
STAGES = {
    "REQUISITION_APPROVAL": (60, 60 * 24 * 3),
    "PROCUREMENT_LEAD_TIME": (60 * 6, 60 * 24 * 5),
    "INVOICE_MATCH_CYCLE": (60, 60 * 24 * 2),
    "BILLING_SETTLEMENT": (60 * 24, 60 * 24 * 60),
}

# Fields that are safe to null out per entity: never the record's own id field
# (ingestion hard-fails without it) and never a *_ref field another entity's
# mapping needs to resolve a relationship endpoint.
DROPPABLE_FIELDS = {
    "pr": ["case_ref", "plant", "company"],
    "po": ["plant", "currency"],
    "asn": ["plant", "company"],
    "gr": ["plant", "material_code"],
    "invoice": ["plant", "currency"],
    "payment": ["currency"],
}

# String fields eligible for whitespace corruption.
WHITESPACE_FIELDS = {
    "pr": ["supplier_code", "material_code", "company", "plant"],
    "po": ["supplier_code", "material_code", "company", "plant"],
    "asn": ["supplier_code", "company", "plant"],
    "gr": ["material_code", "company", "plant"],
    "invoice": ["company", "plant", "currency"],
    "payment": ["currency"],
}


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _pad_whitespace(value: str) -> str:
    pad = random.choice([" ", "  ", "\t", " \t "])
    side = random.choice(["left", "right", "both"])
    if side == "left":
        return f"{pad}{value}"
    if side == "right":
        return f"{value}{pad}"
    return f"{pad}{value}{pad}"


def dirty_record(entity: str, record: Dict[str, Any]) -> Dict[str, Any]:
    """Apply exactly one data-quality issue to a record: a missing (dropped)
    optional field, whitespace-padded string, or an unmapped nested object
    tacked onto the payload. Used to exercise normalize.py's clean_values
    (recursive strip) and the mapper's graceful handling of absent fields."""
    record = dict(record)
    mutation = random.choice(["missing", "whitespace", "nested"])

    if mutation == "missing":
        candidates = [f for f in DROPPABLE_FIELDS.get(entity, []) if f in record]
        if candidates:
            record.pop(random.choice(candidates))
            return record
        mutation = "whitespace"

    if mutation == "whitespace":
        candidates = [f for f in WHITESPACE_FIELDS.get(entity, []) if isinstance(record.get(f), str)]
        if candidates:
            field = random.choice(candidates)
            record[field] = _pad_whitespace(record[field])
            return record
        mutation = "nested"

    # Extra nested metadata the mapping never references; exercises
    # clean_values' recursion into nested dicts and untrimmed nested strings.
    record["audit_meta"] = {
        "entered_by": _pad_whitespace(f"user{random.randint(1, 99)}"),
        "review": {
            "flag": random.choice(["MANUAL", "AUTO", None]),
            "notes": _pad_whitespace("needs check") if random.random() < 0.5 else None,
        },
    }
    return record


def _minutes(mn: int, mx: int, inflate: bool) -> int:
    value = random.randint(mn, mx)
    return value * random.randint(4, 8) if inflate else value


def build_case(index: int, process: str, anomaly_rate: float, base_time: datetime) -> Dict[str, List[Dict[str, Any]]]:
    case_id = f"{process}::PR-{2000 + index}"
    inflated = random.choice(list(STAGES)) if random.random() < anomaly_rate else None

    supplier = random.choice(SUPPLIERS)
    material = random.choice(MATERIALS)
    company = random.choice(COMPANY_CODES)
    plant = random.choice(PLANTS)
    amount = round(random.uniform(500, 120_000), 2)

    t = base_time + timedelta(days=random.randint(0, 250), hours=random.randint(0, 23))

    pr_id = f"PR-{2000 + index}"
    pr_submitted = t + timedelta(minutes=_minutes(30, 60 * 8, False))
    pr_approved = pr_submitted + timedelta(minutes=_minutes(*STAGES["REQUISITION_APPROVAL"], inflated == "REQUISITION_APPROVAL"))
    pr = {
        "requisition_no": pr_id, "case_ref": case_id, "process_type": process,
        "req_date": _iso(t), "submit_ts": _iso(pr_submitted), "approve_ts": _iso(pr_approved),
        "supplier_code": supplier, "material_code": material, "amount_usd": amount,
        "currency": "USD", "company": company, "plant": plant,
    }

    po_id = f"PO-{4500000 + index}"
    po_created = pr_approved + timedelta(minutes=_minutes(15, 120, False))
    po_issued = po_created + timedelta(minutes=_minutes(15, 90, False))
    po = {
        "po_no": po_id, "case_ref": case_id, "process_type": process, "pr_ref": pr_id,
        "po_created_ts": _iso(po_created), "po_issued_ts": _iso(po_issued),
        "supplier_code": supplier, "material_code": material, "amount_usd": amount,
        "currency": "USD", "company": company, "plant": plant,
    }

    gr_received = po_issued + timedelta(minutes=_minutes(*STAGES["PROCUREMENT_LEAD_TIME"], inflated == "PROCUREMENT_LEAD_TIME"))
    asn_id = f"ASN-{8800 + index}"
    asn_created = po_issued + timedelta(minutes=_minutes(60, 60 * 12, False))
    asn = {
        "asn_no": asn_id, "case_ref": case_id, "process_type": process, "po_ref": po_id,
        "asn_created_ts": _iso(asn_created), "supplier_code": supplier, "company": company, "plant": plant,
    }

    gr_validated = gr_received + timedelta(minutes=_minutes(15, 60 * 4, False))
    gr_id = f"GR-{70000 + index}"
    gr = {
        "gr_no": gr_id, "case_ref": case_id, "process_type": process, "po_ref": po_id, "asn_ref": asn_id,
        "gr_received_ts": _iso(gr_received), "gr_validated_ts": _iso(gr_validated),
        "material_code": material, "company": company, "plant": plant,
    }

    invoice_date = gr_received + timedelta(minutes=_minutes(30, 60 * 6, False))
    posted_at = invoice_date + timedelta(minutes=_minutes(*STAGES["INVOICE_MATCH_CYCLE"], inflated == "INVOICE_MATCH_CYCLE"))
    inv_id = f"INV-{9900 + index}"
    invoice = {
        "invoice_no": inv_id, "case_ref": case_id, "process_type": process, "po_ref": po_id, "gr_ref": gr_id,
        "invoice_dt": _iso(invoice_date), "posted_ts": _iso(posted_at),
        "amount_usd": amount, "currency": "USD", "company": company, "plant": plant,
    }

    cleared_at = posted_at + timedelta(minutes=_minutes(*STAGES["BILLING_SETTLEMENT"], inflated == "BILLING_SETTLEMENT"))
    pay_id = f"PMT-{330000 + index}"
    payment = {
        "payment_no": pay_id, "case_ref": case_id, "process_type": process,
        "invoice_ref": inv_id, "po_ref": po_id,
        "payment_ts": _iso(posted_at), "cleared_ts": _iso(cleared_at),
        "amount_usd": amount, "currency": "USD",
    }

    return {"pr": pr, "po": po, "asn": asn, "gr": gr, "invoice": invoice, "payment": payment}


def generate(
    cases: int,
    process: str,
    anomaly_rate: float,
    seed: int | None,
    dirty_rate: float = 0.15,
) -> Dict[str, List[Dict[str, Any]]]:
    if seed is not None:
        random.seed(seed)
    base_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    by_entity: Dict[str, List[Dict[str, Any]]] = {"pr": [], "po": [], "asn": [], "gr": [], "invoice": [], "payment": []}
    for i in range(1, cases + 1):
        record_set = build_case(i, process, anomaly_rate, base_time)
        for entity, record in record_set.items():
            if random.random() < dirty_rate:
                record = dirty_record(entity, record)
            by_entity[entity].append(record)
    return by_entity


def write_local(by_entity: Dict[str, List[Dict[str, Any]]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for entity, records in by_entity.items():
        (OUTPUT_DIR / f"{entity}.json").write_text(json.dumps(records, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", type=int, default=100)
    parser.add_argument("--process", default="P2P")
    parser.add_argument("--anomaly-rate", type=float, default=0.15)
    parser.add_argument("--dirty-rate", type=float, default=0.15, help="Fraction of records with a missing/whitespace/nested data issue")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    by_entity = generate(args.cases, args.process, args.anomaly_rate, args.seed, args.dirty_rate)
    write_local(by_entity)
    counts = {k: len(v) for k, v in by_entity.items()}
    print(f"Wrote synthetic P2P source records to {OUTPUT_DIR}: {counts}")


if __name__ == "__main__":
    main()
