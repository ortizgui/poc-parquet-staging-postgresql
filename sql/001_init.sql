-- ============================================================================
-- POC: Ingestão Massiva de Dados - Parquet → Staging → Principal
-- ============================================================================

-- Tabela Principal (produção)
-- Contém os dados finais com todas as otimizações (índices, constraints)
CREATE TABLE IF NOT EXISTS custody_position (
    id SERIAL PRIMARY KEY,
    account_id VARCHAR NOT NULL,
    asset_id VARCHAR NOT NULL,
    reference_date DATE NOT NULL,
    quantity NUMERIC(18, 4) NOT NULL,
    amount NUMERIC(18, 2) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, asset_id, reference_date)
);

-- Índices para leitura eficiente
CREATE INDEX IF NOT EXISTS idx_custody_position_account ON custody_position (account_id);
CREATE INDEX IF NOT EXISTS idx_custody_position_date ON custody_position (reference_date);
CREATE INDEX IF NOT EXISTS idx_custody_position_lookup ON custody_position (account_id, asset_id, reference_date);

-- Tabela Staging (landing zone para dados do Parquet)
-- Recebe bulk insert diretamente do parquet
-- Sem índices para maximizar velocidade de INSERT
CREATE TABLE IF NOT EXISTS custody_position_staging (
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
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    -- Unique constraints para idempotência
    UNIQUE (source_file, row_number),
    UNIQUE (source_file, record_hash)
);

-- Tabela de Erros (registros inválidos do parquet)
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
