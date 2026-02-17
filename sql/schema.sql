-- ============================================
-- Firmable - Australian Company Data Pipeline
-- Schema Definition
-- ============================================


-- =====================
-- CREATING SCHEMA
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
-- =====================

-- =====================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- =====================

-- ======================
-- STAGING TABLES (RAW)
-- ======================

CREATE TABLE IF NOT EXISTS staging.abr_raw (
    abn VARCHAR(11),
    entity_name TEXT,
    entity_type TEXT,
    entity_status TEXT,
    address_line TEXT,
    postcode TEXT,
    state TEXT,
    start_date DATE,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.commoncrawl_raw (
    website_url TEXT,
    company_name TEXT,
    industry TEXT,
    loaded_at TIMESTAMP DEFAULT NOW()
);


-- ======================
-- STAGING TABLES (CLEAN)
-- ======================

CREATE TABLE IF NOT EXISTS staging.abr_clean (
    abn VARCHAR(11) PRIMARY KEY,
    entity_name TEXT,
    normalized_name TEXT,
    entity_type TEXT,
    entity_status TEXT,
    address_line TEXT,
    postcode TEXT,
    state TEXT,
    start_date DATE
);

CREATE TABLE IF NOT EXISTS staging.commoncrawl_clean (
    id SERIAL PRIMARY KEY,
    website_url TEXT,
    company_name TEXT,
    normalized_name TEXT,
    industry TEXT
);


-- =====================
-- AI (LLM) Prompt logs
-- =
CREATE TABLE IF NOT EXISTS core.ai_match_log (
    id SERIAL PRIMARY KEY,
    company_a TEXT,
    company_b TEXT,
    fuzzy_score FLOAT,
    prompt TEXT,
    llm_response JSONB,
    decision BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ======================
-- CORE TABLE (UNIFIED)
-- ======================

CREATE TABLE IF NOT EXISTS core.company_master (
    company_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    abn VARCHAR(11),
    website_url TEXT,
    company_name TEXT,
    industry TEXT,
    entity_type TEXT,
    entity_status TEXT,
    state VARCHAR(3),
    postcode VARCHAR(4),
    match_method VARCHAR(20),
    match_confidence NUMERIC(5,2),
    created_at TIMESTAMP DEFAULT NOW()
);


-- ======================
-- INDEXES (Performance)
-- ======================

CREATE INDEX IF NOT EXISTS idx_abr_clean_normalized_name
ON staging.abr_clean(normalized_name);

CREATE INDEX IF NOT EXISTS idx_commoncrawl_clean_normalized_name
ON staging.commoncrawl_clean(normalized_name);

CREATE INDEX IF NOT EXISTS idx_company_master_abn
ON core.company_master(abn);

CREATE INDEX IF NOT EXISTS idx_company_master_name
ON core.company_master(company_name);



-- ======================
-- ROLES & PERMISSIONS
-- ======================

-- Read-only analyst role
CREATE ROLE analyst_readonly;

GRANT CONNECT ON DATABASE firmable_db TO analyst_readonly;

GRANT USAGE ON SCHEMA staging TO analyst_readonly;
GRANT USAGE ON SCHEMA core TO analyst_readonly;

GRANT SELECT ON ALL TABLES IN SCHEMA staging TO analyst_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA core TO analyst_readonly;

-- Future-proof: apply to new tables automatically
ALTER DEFAULT PRIVILEGES IN SCHEMA staging
GRANT SELECT ON TABLES TO analyst_readonly;

ALTER DEFAULT PRIVILEGES IN SCHEMA core
GRANT SELECT ON TABLES TO analyst_readonly;



