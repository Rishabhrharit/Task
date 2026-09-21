"""Run the synthetic P2P data through the real ingestion pipeline end to end:

    local FastAPI P2P source (in-process)
        -> ingestion.generic_rest   (raw.api_records)
        -> transformation.preprocess (staging.preprocessed_records)
        -> transformation.normalize  (canonical.otc_records, mapped to DT_p2p_schema.json fields)
        -> upsert_merge              (Neo4j nodes + relationships)

This is a one-off test harness for the P2P mapping while there is no real
P2P source system. The normal path for any live source stays the plain
`python -m ingestion.generic_rest --config ...` flow described in the README.

Usage:
    python run_p2p_pipeline.py --cases 100 --wipe
"""

from __future__ import annotations

import argparse
import os
import threading
import time

import psycopg2
import requests
import uvicorn

from config import load_project_env
from ingestion.generic_rest import ingest_from_config
from transformation.preprocess import preprocess_records
from transformation.normalize import normalize_staged_records
from upsert_merge import upsert_postgres_canonical_to_neo4j
from generate_p2p_synthetic_source import generate, write_local

load_project_env()

HOST = "127.0.0.1"
PORT = 8123
BASE_URL = f"http://{HOST}:{PORT}"

ENTITIES = {
    "pr": "requisition_no",
    "po": "po_no",
    "asn": "asn_no",
    "gr": "gr_no",
    "invoice": "invoice_no",
    "payment": "payment_no",
}

MAPPING_DIR = os.path.join(os.path.dirname(__file__), "transformation", "p2p_mappings")


def _start_local_source_server() -> uvicorn.Server:
    from source_api.app.p2p_main import app

    config = uvicorn.Config(app, host=HOST, port=PORT, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(50):
        try:
            requests.get(f"{BASE_URL}/p2p/pr", timeout=1)
            break
        except requests.exceptions.ConnectionError:
            time.sleep(0.1)
    return server


def run_pipeline(cases: int, process: str, anomaly_rate: float, wipe: bool, seed: int | None) -> None:
    connection_string = os.environ.get("DATABASE_URL")
    if not connection_string:
        raise RuntimeError("Set DATABASE_URL before running the P2P test pipeline.")

    print(f"Generating {cases} synthetic P2P cases locally...")
    by_entity = generate(cases, process, anomaly_rate, seed)
    write_local(by_entity)

    if wipe:
        with psycopg2.connect(connection_string) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM canonical.otc_records c USING staging.preprocessed_records sp "
                    "WHERE c.staging_record_id = sp.id AND sp.source_system = 'P2P_ERP'"
                )
                cursor.execute("DELETE FROM staging.preprocessed_records WHERE source_system = 'P2P_ERP'")
                cursor.execute("DELETE FROM raw.api_records WHERE source_system = 'P2P_ERP'")
            connection.commit()
        print("Wiped previous P2P_ERP rows from raw/staging/canonical.")

    server = _start_local_source_server()
    batch_ids: list[str] = []
    try:
        for entity, id_field in ENTITIES.items():
            mapping_path = os.path.join(MAPPING_DIR, f"{entity}.json")
            os.environ["SOURCE_MAPPING_PATH"] = mapping_path

            api_config = {
                "endpoint": f"{BASE_URL}/p2p/{entity}",
                "source_system": "P2P_ERP",
                "source_entity": entity,
                "record_id_field": id_field,
                "response": {"records_path": ""},
            }
            batch_id = ingest_from_config(config=api_config)
            staged = preprocess_records(connection_string, batch_id=batch_id)
            normalized = normalize_staged_records(connection_string, batch_id=batch_id)
            print(f"[{entity}] batch={batch_id} raw={len(staged)} staged canonical={normalized}")
            batch_ids.append(batch_id)
    finally:
        server.should_exit = True
        os.environ.pop("SOURCE_MAPPING_PATH", None)

    total_nodes = 0
    total_rels = 0
    for batch_id in batch_ids:
        result = upsert_postgres_canonical_to_neo4j(connection_string, batch_id=batch_id)
        total_nodes += result["nodes"]
        total_rels += result["relationships"]
    print(f"Upserted into Neo4j: {total_nodes} nodes, {total_rels} relationships across {len(batch_ids)} batches.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", type=int, default=100)
    parser.add_argument("--process", default="P2P")
    parser.add_argument("--anomaly-rate", type=float, default=0.15)
    parser.add_argument("--wipe", action="store_true", help="Delete previous P2P_ERP rows before this run")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    run_pipeline(args.cases, args.process, args.anomaly_rate, args.wipe, args.seed)


if __name__ == "__main__":
    main()
