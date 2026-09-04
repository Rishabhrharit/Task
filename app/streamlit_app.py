"""Streamlit control panel for the OTC preparation pipeline."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
import requests
import streamlit as st

# Streamlit may execute this file with only the app directory on sys.path.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ingestion.shippo_client import ingest_count, reset_pipeline_data
from transformation.normalize import normalize_staged_records
from transformation.preprocess import preprocess_records


REQUEST_FILE = ROOT / "ingestion" / "shipment_request.example.json"


def flatten(value: Any, prefix: str = "") -> dict[str, Any]:
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, nested in value.items():
            result.update(flatten(nested, f"{prefix}.{key}" if prefix else key))
        return result
    if isinstance(value, list):
        return {prefix: json.dumps(value, ensure_ascii=False)}
    return {prefix: value}


def canonical_record_rows(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record_number, record in enumerate(records, start=1):
        attributes = record.get("entity", {}).get("attributes", {})
        row: dict[str, Any] = {"record": record_number}
        for section, values in attributes.items():
            if isinstance(values, dict):
                for attribute_path, value in flatten(values).items():
                    attribute_name = attribute_path.split(".")[-1]
                    if attribute_name in row:
                        attribute_name = f"{section}_{attribute_name}"
                    row[attribute_name] = value
            else:
                row[section] = values
        rows.append(row)
    return rows


def load_table(connection_string: str, table: str) -> list[dict[str, Any]]:
    if table not in {"staging.preprocessed_records", "canonical.otc_records"}:
        raise ValueError("Unsupported table.")
    with psycopg2.connect(connection_string) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT payload FROM {table} ORDER BY id DESC")
            return [row[0] for row in cursor.fetchall()]


st.set_page_config(page_title="OTC Data Pipeline", layout="wide")
st.title("OTC Data Preparation Pipeline")
st.caption("Shippo test API -> raw -> staging -> otc.v1 canonical")

database_url = os.environ.get("DATABASE_URL")
if not database_url:
    st.error("Set DATABASE_URL before starting Streamlit.")
    st.stop()

with st.sidebar:
    st.header("Pipeline controls")
    record_count = st.number_input("Number of test shipments", min_value=1, max_value=100, value=10)
    if st.button("Ingest from Shippo", type="primary"):
        token = os.environ.get("SHIPPO_API_TOKEN")
        if not token:
            st.error("Set SHIPPO_API_TOKEN before ingesting.")
        else:
            try:
                with st.spinner("Creating Shippo test shipments..."):
                    reset_pipeline_data(database_url)
                    ingested = ingest_count(
                        REQUEST_FILE, int(record_count), token, database_url
                    )
                    staged = preprocess_records(database_url)
            except requests.HTTPError as error:
                status_code = error.response.status_code if error.response else "unknown"
                st.error(
                    f"Shippo rejected the request ({status_code}). "
                    "Check that SHIPPO_API_TOKEN is current and belongs to test mode."
                )
            except requests.RequestException as error:
                st.error(f"Shippo request failed: {error}")
            else:
                st.success(f"Ingested {ingested}; staged {len(staged)} records.")
                st.rerun()

    if st.button("Normalize to otc.v1"):
        with st.spinner("Normalizing staged records..."):
            normalized = normalize_staged_records(database_url)
        st.success(f"Normalized {normalized} new records.")
        st.rerun()

try:
    staged_records = load_table(database_url, "staging.preprocessed_records")
    canonical_records = load_table(database_url, "canonical.otc_records")
except psycopg2.Error as error:
    st.error(f"Unable to read PostgreSQL: {error}")
    st.stop()

st.metric("Preprocessed records", len(staged_records))
st.metric("Canonical records", len(canonical_records))

tab_staging, tab_canonical = st.tabs(["Preprocessed data", "Canonical otc.v1"])
with tab_staging:
    if staged_records:
        st.dataframe(pd.DataFrame([flatten(record) for record in staged_records]), use_container_width=True, hide_index=True)
        st.json(staged_records[0])
    else:
        st.info("No staged records yet.")

with tab_canonical:
    if canonical_records:
        st.subheader("One row per canonical record")
        st.dataframe(
            pd.DataFrame(canonical_record_rows(canonical_records)),
            use_container_width=True,
            hide_index=True,
        )
        st.subheader("Complete otc.v1 record")
        st.json(canonical_records[0])
    else:
        st.info("Click 'Normalize to otc.v1' after staging records are available.")
