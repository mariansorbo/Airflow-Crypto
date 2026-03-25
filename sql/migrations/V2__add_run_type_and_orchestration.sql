-- ============================================================
-- V2 — Add run_type column + orchestration schema
-- Applied: 2026-03-24
-- Description:
--   - run_type column on coin_market_snapshots and coin_dev_metrics
--     to distinguish scheduled / manual / backfill runs
--   - orchestration schema with pipeline_runs control table
-- ============================================================

-- ── run_type on clean tables ─────────────────────────────────────────────────

ALTER TABLE coin_market_snapshots
    ADD COLUMN IF NOT EXISTS run_type VARCHAR(50) DEFAULT 'scheduled';

ALTER TABLE coin_dev_metrics
    ADD COLUMN IF NOT EXISTS run_type VARCHAR(50) DEFAULT 'scheduled';

-- ── run_type on raw tables ───────────────────────────────────────────────────

ALTER TABLE raw.coin_market_responses
    ADD COLUMN IF NOT EXISTS run_type VARCHAR(50) DEFAULT 'scheduled';

ALTER TABLE raw.coin_metadata_responses
    ADD COLUMN IF NOT EXISTS run_type VARCHAR(50) DEFAULT 'scheduled';

ALTER TABLE raw.coin_dev_responses
    ADD COLUMN IF NOT EXISTS run_type VARCHAR(50) DEFAULT 'scheduled';

-- ── orchestration schema ─────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS orchestration;

CREATE TABLE IF NOT EXISTS orchestration.pipeline_runs (
    run_id              VARCHAR(250)  PRIMARY KEY,
    dag_id              VARCHAR(250)  NOT NULL,
    run_type            VARCHAR(50),
    snapshot_ts         TIMESTAMPTZ,
    status              VARCHAR(20)   NOT NULL DEFAULT 'running',
    coins_expected      INTEGER,
    coins_processed     INTEGER,
    raw_rows_inserted   INTEGER,
    clean_rows_inserted INTEGER,
    error_message       TEXT,
    started_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    CONSTRAINT chk_status CHECK (status IN ('running', 'success', 'partial', 'failed'))
);

COMMENT ON TABLE orchestration.pipeline_runs IS
    'One row per DAG run. status: running → success | partial | failed';
