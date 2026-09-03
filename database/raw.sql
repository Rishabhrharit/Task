CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.api_records (
    id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    source_system TEXT NOT NULL,
    source_entity TEXT NOT NULL,
    source_record_id TEXT,
    extracted_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    ingestion_status TEXT NOT NULL DEFAULT 'SUCCESS',
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS api_records_source_record_idx
    ON raw.api_records (source_system, source_entity, source_record_id);

CREATE INDEX IF NOT EXISTS api_records_extracted_at_idx
    ON raw.api_records (extracted_at);
