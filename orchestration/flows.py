"""Prefect flow chaining ingestion -> preprocess -> normalize -> parquet -> Neo4j."""

from __future__ import annotations

import os
from typing import Any

from prefect import flow, get_run_logger, task

from config import load_project_env
from ingestion import generic_rest, raw_parquet
from transformation import normalize as canonical_normalize
from transformation import preprocess
from upsert_merge import upsert_postgres_canonical_to_neo4j

load_project_env()


@task(name="ingest_to_minio", retries=2, retry_delay_seconds=10)
def ingest_task(config: dict[str, Any] | None, max_records: int | None) -> str:
    return generic_rest.ingest_from_config(config=config, max_records=max_records)


@task(name="convert_to_parquet", retries=2, retry_delay_seconds=10)
def convert_parquet_task(batch_id: str) -> list[str]:
    return raw_parquet.convert_landing_batch_to_parquet(batch_id)


@task(name="preprocess_in_postgres", retries=2, retry_delay_seconds=10)
def preprocess_task(connection_string: str, batch_id: str) -> list[int]:
    return preprocess.preprocess_records(connection_string, batch_id=batch_id)


@task(name="normalize_in_postgres", retries=2, retry_delay_seconds=10)
def normalize_task(connection_string: str, batch_id: str) -> int:
    return canonical_normalize.normalize_staged_records(connection_string, batch_id=batch_id)


@task(name="upsert_neo4j", retries=2, retry_delay_seconds=15)
def neo4j_upsert_task(connection_string: str, batch_id: str) -> dict[str, int]:
    return upsert_postgres_canonical_to_neo4j(connection_string, batch_id=batch_id)


@flow(name="otc-ingestion-pipeline", log_prints=True)
def otc_pipeline_flow(
    config: dict[str, Any] | None = None,
    max_records: int | None = None,
) -> dict[str, Any]:
    """Default pipeline: ingest to MinIO -> convert to Parquet -> process/normalize
    in Postgres -> upsert into Neo4j. Each stage is its own retryable Prefect task."""
    logger = get_run_logger()
    connection_string = os.environ["DATABASE_URL"]

    batch_id = ingest_task(config, max_records)
    logger.info("Ingested batch %s into MinIO", batch_id)

    parquet_keys = convert_parquet_task(batch_id)
    logger.info("Converted %d landing object(s) to Parquet", len(parquet_keys))

    staged_ids = preprocess_task(connection_string, batch_id)
    logger.info("Staged %d records in Postgres", len(staged_ids))

    canonical_count = normalize_task(connection_string, batch_id)
    logger.info("Normalized %d canonical records in Postgres", canonical_count)

    graph_summary = neo4j_upsert_task(connection_string, batch_id)
    logger.info("Neo4j upsert summary: %s", graph_summary)

    return {
        "batch_id": batch_id,
        "parquet_keys": parquet_keys,
        "staged_rows": len(staged_ids),
        "canonical_rows": canonical_count,
        "neo4j_rows": graph_summary,
    }


if __name__ == "__main__":
    otc_pipeline_flow()
