CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.preprocessed_records (
    id BIGSERIAL PRIMARY KEY,
    raw_record_id BIGINT NOT NULL REFERENCES raw.api_records(id),
    batch_id UUID NOT NULL,
    source_system TEXT NOT NULL,
    source_record_id TEXT,
    processed_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS preprocessed_records_source_record_idx
    ON staging.preprocessed_records (source_system, source_record_id);

CREATE INDEX IF NOT EXISTS preprocessed_records_processed_at_idx
    ON staging.preprocessed_records (processed_at);
