DROP TABLE IF EXISTS custody_position_staging;

CREATE TABLE IF NOT EXISTS custody_position (
    id SERIAL PRIMARY KEY,
    account_id VARCHAR NOT NULL,
    asset_id VARCHAR NOT NULL,
    reference_date DATE NOT NULL,
    quantity NUMERIC(18, 4) NOT NULL,
    amount NUMERIC(18, 2) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, asset_id, reference_date)
);

CREATE TABLE IF NOT EXISTS custody_position_buffer (
    id SERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    source_file VARCHAR NOT NULL,
    row_number INTEGER NOT NULL,
    record_hash VARCHAR NOT NULL,
    account_id VARCHAR NOT NULL,
    asset_id VARCHAR NOT NULL,
    reference_date DATE NOT NULL,
    quantity NUMERIC(18, 4) NOT NULL,
    amount NUMERIC(18, 2) NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'PENDING',
    error_reason TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    merged_at TIMESTAMP,
    UNIQUE (source_file, row_number),
    UNIQUE (source_file, record_hash)
);

CREATE TABLE IF NOT EXISTS custody_position_error (
    id SERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    source_file VARCHAR NOT NULL,
    row_number INTEGER NOT NULL,
    payload JSONB NOT NULL,
    error_reason TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (source_file, row_number)
);
