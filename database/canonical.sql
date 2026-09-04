CREATE SCHEMA IF NOT EXISTS canonical;

CREATE TABLE IF NOT EXISTS canonical.otc_records (
    id BIGSERIAL PRIMARY KEY,
    staging_record_id BIGINT NOT NULL REFERENCES staging.preprocessed_records(id),
    batch_id UUID NOT NULL,
    source_record_id TEXT,
    processed_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS otc_records_source_record_idx
    ON canonical.otc_records (source_record_id);
