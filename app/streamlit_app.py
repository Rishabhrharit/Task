"""Streamlit UI for the Postgres-first Neo4j AuraDB graph pipeline."""

from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import networkx as nx
import streamlit as st
from neo4j import GraphDatabase

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import load_project_env
from orchestration.flows import otc_pipeline_flow

load_project_env()

def ensure_neo4j_config() -> None:
    missing = [
        name
        for name in ("NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD")
        if not os.getenv(name)
    ]
    if missing:
        raise RuntimeError("Missing Neo4j AuraDB settings: " + ", ".join(missing))


def neo4j_driver() -> GraphDatabase.driver:
    ensure_neo4j_config()
    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    return GraphDatabase.driver(uri, auth=(username, password))


def fetch_graph_snapshot(limit: int = 150) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    driver = neo4j_driver()
    query = """
    MATCH (n)
    OPTIONAL MATCH (n)-[r]->(m)
    RETURN
      elementId(n) AS node_id,
      labels(n) AS labels,
      properties(n) AS properties,
      elementId(m) AS target_id,
      type(r) AS rel_type
    LIMIT $limit
    """
    try:
        with driver.session() as session:
            rows = session.run(query, limit=limit).data()
    finally:
        driver.close()

    node_map: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []
    for row in rows:
        node_id = row.get("node_id")
        if node_id is not None:
            identifier = str(node_id)
            node_map.setdefault(
                identifier,
                {
                    "id": identifier,
                    "labels": row.get("labels") or [],
                    "properties": row.get("properties") or {},
                },
            )

        target_id = row.get("target_id")
        rel_type = row.get("rel_type")
        if target_id is not None and rel_type:
            edges.append({
                "source": str(node_id),
                "target": str(target_id),
                "label": rel_type,
            })

    return list(node_map.values()), edges


def fetch_order_chain(order_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    driver = neo4j_driver()
    query = """
    MATCH (o:Order {order_id: $order_id})
    OPTIONAL MATCH (o)-[:FULFILLED_BY]->(s:Shipment)
    OPTIONAL MATCH (s)-[:DEPARTS_FROM]->(f:Facility)
    OPTIONAL MATCH (s)-[:HANDED_OFF_TO]->(c:Carrier)
    OPTIONAL MATCH (s)-[:TRAVELS_VIA]->(r:Route)
    OPTIONAL MATCH (e:Event {order_id: $order_id})-[:LOGGED_FOR]->(s)
    OPTIONAL MATCH (p:DeliveryPartner)-[:SERVICES]->(:Region)<-[:IN_REGION]-(f)
    RETURN DISTINCT
      elementId(o) AS order_id_node,
      labels(o) AS order_labels,
      properties(o) AS order_props,
      elementId(s) AS shipment_id_node,
      labels(s) AS shipment_labels,
      properties(s) AS shipment_props,
      elementId(f) AS facility_id_node,
      labels(f) AS facility_labels,
      properties(f) AS facility_props,
      elementId(c) AS carrier_id_node,
      labels(c) AS carrier_labels,
      properties(c) AS carrier_props,
      elementId(r) AS route_id_node,
      labels(r) AS route_labels,
      properties(r) AS route_props,
      elementId(e) AS event_id_node,
      labels(e) AS event_labels,
      properties(e) AS event_props,
      elementId(p) AS partner_id_node,
      labels(p) AS partner_labels,
      properties(p) AS partner_props
    """
    try:
        with driver.session() as session:
            rows = session.run(query, order_id=order_id).data()
    finally:
        driver.close()

    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []
    for row in rows:
        for key_prefix, label_key, prop_key in [
            ("order", "order_labels", "order_props"),
            ("shipment", "shipment_labels", "shipment_props"),
            ("facility", "facility_labels", "facility_props"),
            ("carrier", "carrier_labels", "carrier_props"),
            ("route", "route_labels", "route_props"),
            ("event", "event_labels", "event_props"),
            ("partner", "partner_labels", "partner_props"),
        ]:
            node_id = row.get(f"{key_prefix}_id_node")
            labels = row.get(label_key) or []
            properties = row.get(prop_key) or {}
            if node_id is not None:
                nodes.setdefault(
                    str(node_id),
                    {
                        "id": str(node_id),
                        "labels": labels,
                        "properties": properties,
                    },
                )

        if row.get("order_id_node") and row.get("shipment_id_node"):
            edges.append({"source": str(row["order_id_node"]), "target": str(row["shipment_id_node"]), "label": "FULFILLED_BY"})
        if row.get("shipment_id_node") and row.get("facility_id_node"):
            edges.append({"source": str(row["shipment_id_node"]), "target": str(row["facility_id_node"]), "label": "DEPARTS_FROM"})
        if row.get("shipment_id_node") and row.get("carrier_id_node"):
            edges.append({"source": str(row["shipment_id_node"]), "target": str(row["carrier_id_node"]), "label": "HANDED_OFF_TO"})
        if row.get("shipment_id_node") and row.get("route_id_node"):
            edges.append({"source": str(row["shipment_id_node"]), "target": str(row["route_id_node"]), "label": "TRAVELS_VIA"})
        if row.get("event_id_node") and row.get("shipment_id_node"):
            edges.append({"source": str(row["event_id_node"]), "target": str(row["shipment_id_node"]), "label": "LOGGED_FOR"})
        if row.get("partner_id_node") and row.get("facility_id_node"):
            edges.append({"source": str(row["partner_id_node"]), "target": str(row["facility_id_node"]), "label": "SERVICES"})

    return list(nodes.values()), edges


def render_network_graph(node_payload: list[dict[str, Any]], edges: list[dict[str, Any]]) -> None:
    graph = nx.DiGraph()
    for node in node_payload:
        graph.add_node(node["id"], label=node["labels"][0] if node["labels"] else "Node", properties=node["properties"])
    for edge in edges:
        graph.add_edge(edge["source"], edge["target"], label=edge["label"])

    fig, ax = plt.subplots(figsize=(12, 8))
    pos = nx.spring_layout(graph, seed=42)
    nx.draw_networkx_nodes(graph, pos, node_color="#4F8BF9", node_size=800, alpha=0.9, ax=ax)
    nx.draw_networkx_edges(graph, pos, edge_color="#8A8A8A", arrows=True, width=1.5, ax=ax)
    nx.draw_networkx_labels(graph, pos, font_size=9, ax=ax)
    ax.set_axis_off()
    st.pyplot(fig)


def run_api_graph_pipeline(
    api_url: str,
    token: str,
    id_field: str,
    mapping_text: str,
    canonical_text: str,
) -> dict[str, Any]:
    """Ingest one REST API through the default MinIO->Parquet->Postgres->Neo4j
    pipeline. source_system/source_entity are derived from the mapping so the
    form only needs to ask for what actually varies between sources."""
    if not os.getenv("DATABASE_URL"):
        raise RuntimeError("DATABASE_URL is required for API ingestion.")

    mapping = json.loads(mapping_text)
    canonical_schema = json.loads(canonical_text)
    if not isinstance(mapping, dict):
        raise ValueError("The mapping must be a JSON object.")
    if not isinstance(canonical_schema, dict):
        raise ValueError("The canonical schema must be a JSON object.")

    source_entity = str(mapping.get("entity_type") or "record").lower()

    previous_mapping_path = os.environ.get("SOURCE_MAPPING_PATH")
    previous_schema_path = os.environ.get("CANONICAL_SCHEMA_PATH")
    with tempfile.TemporaryDirectory(prefix="otc_streamlit_") as temporary_dir:
        mapping_path = Path(temporary_dir) / "source_mapping.json"
        schema_path = Path(temporary_dir) / "canonical_schema.json"
        mapping_path.write_text(json.dumps(mapping), encoding="utf-8")
        schema_path.write_text(json.dumps(canonical_schema), encoding="utf-8")
        os.environ["SOURCE_MAPPING_PATH"] = str(mapping_path)
        os.environ["CANONICAL_SCHEMA_PATH"] = str(schema_path)
        try:
            config: dict[str, Any] = {
                "endpoint": api_url.strip(),
                "source_system": "api",
                "source_entity": source_entity,
                "record_id_field": id_field.strip(),
                "response": {"records_path": ""},
            }
            if token.strip():
                config["auth"] = {
                    "type": "bearer",
                    "token": token.strip(),
                    "header": "Authorization",
                    "prefix": "Bearer",
                }
            # Default pipeline: MinIO landing -> Parquet -> Postgres -> Neo4j, via Prefect.
            return otc_pipeline_flow(config=config)
        finally:
            if previous_mapping_path is None:
                os.environ.pop("SOURCE_MAPPING_PATH", None)
            else:
                os.environ["SOURCE_MAPPING_PATH"] = previous_mapping_path
            if previous_schema_path is None:
                os.environ.pop("CANONICAL_SCHEMA_PATH", None)
            else:
                os.environ["CANONICAL_SCHEMA_PATH"] = previous_schema_path


st.set_page_config(page_title="Neo4j Aura Graph Pipeline", layout="wide")
st.title("Neo4j AuraDB Order Graph Pipeline")
st.caption("REST API -> PostgreSQL raw/staging/canonical -> Neo4j AuraDB final graph")

with st.sidebar:
    st.header("Pipeline controls")

    api_url = st.text_input("API endpoint URL", placeholder="https://api.example.com/orders")
    api_token = st.text_input("API token", type="password")
    id_field = st.text_input("ID field (primary key)", placeholder="id")
    mapping_text = st.text_area(
        "Mapping JSON",
        height=220,
        placeholder='{"source": {"entity_id": "id"}, "entity_type": "Order", "canonical": {"amount": "totals.due"}}',
    )
    canonical_text = st.text_area(
        "Canonical JSON Schema",
        height=220,
        placeholder='{"type": "object", "required": ["schema_version", "metadata", "entity"]}',
    )

    if st.button("Map API -> Postgres -> Neo4j", type="primary"):
        required_values = {
            "API endpoint URL": api_url.strip(),
            "ID field": id_field.strip(),
            "Mapping JSON": mapping_text.strip(),
            "Canonical JSON Schema": canonical_text.strip(),
        }
        missing = [label for label, value in required_values.items() if not value]
        if missing:
            st.error("Provide: " + ", ".join(missing))
        else:
            try:
                with st.spinner("Fetching, mapping, normalizing, and upserting the graph..."):
                    counts = run_api_graph_pipeline(api_url, api_token, id_field, mapping_text, canonical_text)
                st.success(f"Pipeline complete: {counts}")
            except json.JSONDecodeError as exc:
                st.error(f"Mapping or canonical input is not valid JSON: {exc}")
            except Exception as exc:  # noqa: BLE001
                st.error(f"Pipeline failed: {exc}")

with st.expander("Demo: run the synthetic P2P test pipeline"):
    st.caption(
        "Runs the standalone run_p2p_pipeline.py harness (generate synthetic "
        "PR/PO/ASN/GR/Invoice/Payment cases, serve them locally, then ingest each "
        "in dependency order and upsert to Neo4j). This is a fixed test scenario, "
        "kept separate from the generic API form above."
    )
    demo_cases = st.number_input("Cases", min_value=1, max_value=5000, value=120, step=1)
    demo_anomaly_rate = st.slider("Anomaly rate (inflated stage duration)", 0.0, 1.0, 0.15)
    demo_seed = st.number_input("Seed (0 = random)", min_value=0, max_value=999999, value=42, step=1)
    demo_wipe = st.checkbox("Wipe previous P2P_ERP rows first", value=True)

    if st.button("Run P2P pipeline"):
        try:
            from run_p2p_pipeline import run_pipeline

            log = io.StringIO()
            with st.spinner("Running PR -> PO -> ASN -> GR -> Invoice -> Payment..."):
                with contextlib.redirect_stdout(log):
                    run_pipeline(
                        cases=int(demo_cases),
                        process="P2P",
                        anomaly_rate=float(demo_anomaly_rate),
                        wipe=demo_wipe,
                        seed=int(demo_seed) or None,
                    )
            st.success("P2P demo pipeline complete.")
            st.code(log.getvalue())
        except Exception as exc:  # noqa: BLE001
            st.error(f"P2P demo pipeline failed: {exc}")

try:
    node_payload, edges = fetch_graph_snapshot(limit=200)
except Exception as exc:  # noqa: BLE001
    st.warning(f"Unable to read Neo4j graph yet: {exc}")
    node_payload, edges = [], []

if "query_nodes" in st.session_state and "query_edges" in st.session_state:
    node_payload = st.session_state["query_nodes"]
    edges = st.session_state["query_edges"]

col1, col2, col3 = st.columns(3)
col1.metric("Nodes", len(node_payload))
col2.metric("Relationships", len(edges))
col3.metric("Graph status", "Connected" if node_payload or edges else "Waiting")

if node_payload:
    render_network_graph(node_payload, edges)
else:
    st.info("No graph nodes available yet. Ingest through Postgres and then upsert the canonical graph into Neo4j.")

st.subheader("Order-chain details")
if node_payload:
    sample = node_payload[:10]
    st.json({"nodes": sample, "relationships": edges[:10]})
else:
    st.caption("The graph will appear here after the first successful Postgres-to-Neo4j upsert.")
