#!/bin/bash
# PostgreSQL init script — runs once on first container start.
# Creates the crypto_data database and all required tables.
set -e

echo ">>> Creating crypto_data database..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE crypto_data;
    GRANT ALL PRIVILEGES ON DATABASE crypto_data TO $POSTGRES_USER;
EOSQL

echo ">>> Creating crypto schema and tables in crypto_data..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "crypto_data" <<-EOSQL

    -- ----------------------------------------------------------------
    -- coins_dim
    -- Dimension table: one row per coin, updated on every pipeline run.
    -- ----------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS coins_dim (
        coin_id       VARCHAR(100)  PRIMARY KEY,
        symbol        VARCHAR(20),
        name          VARCHAR(200)  NOT NULL,
        categories    JSONB,
        website_url   TEXT,
        github_url    TEXT,
        first_seen_at TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        updated_at    TIMESTAMPTZ   NOT NULL
    );

    -- ----------------------------------------------------------------
    -- coin_market_snapshots
    -- One row per (timestamp, coin) — the core fact table.
    -- ----------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS coin_market_snapshots (
        snapshot_ts        TIMESTAMPTZ   NOT NULL,
        coin_id            VARCHAR(100)  NOT NULL,
        price_usd          NUMERIC(24,8),
        market_cap_usd     NUMERIC(24,2),
        volume_24h_usd     NUMERIC(24,2),
        circulating_supply NUMERIC(30,4),
        total_supply       NUMERIC(30,4),
        max_supply         NUMERIC(30,4),
        market_cap_rank    INTEGER,
        PRIMARY KEY (snapshot_ts, coin_id)
    );

    CREATE INDEX IF NOT EXISTS idx_snapshots_coin_ts
        ON coin_market_snapshots (coin_id, snapshot_ts DESC);

    -- ----------------------------------------------------------------
    -- coin_dev_metrics
    -- Developer activity per snapshot.
    -- ----------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS coin_dev_metrics (
        snapshot_ts     TIMESTAMPTZ   NOT NULL,
        coin_id         VARCHAR(100)  NOT NULL,
        github_stars    INTEGER,
        github_forks    INTEGER,
        dev_metric_raw  JSONB,
        fetched_at      TIMESTAMPTZ   NOT NULL,
        PRIMARY KEY (snapshot_ts, coin_id)
    );

    -- ----------------------------------------------------------------
    -- coin_raw_responses
    -- Stores the original JSON payload from every API call.
    -- ----------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS coin_raw_responses (
        id              BIGSERIAL     PRIMARY KEY,
        snapshot_ts     TIMESTAMPTZ   NOT NULL,
        coin_id         VARCHAR(100)  NOT NULL,
        source_endpoint VARCHAR(200)  NOT NULL,
        raw_payload     JSONB         NOT NULL,
        inserted_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        UNIQUE (snapshot_ts, coin_id, source_endpoint)
    );

    CREATE INDEX IF NOT EXISTS idx_raw_coin_ts
        ON coin_raw_responses (coin_id, snapshot_ts DESC);

    -- ----------------------------------------------------------------
    -- coin_daily_summary
    -- Aggregated OHLCV + averages per coin per day.
    -- Rebuilt (upserted) by build_daily_summary on every DAG run.
    -- ----------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS coin_daily_summary (
        date               DATE          NOT NULL,
        coin_id            VARCHAR(100)  NOT NULL,
        open_price_usd     NUMERIC(24,8),
        high_price_usd     NUMERIC(24,8),
        low_price_usd      NUMERIC(24,8),
        close_price_usd    NUMERIC(24,8),
        avg_volume_24h_usd NUMERIC(24,2),
        avg_market_cap_usd NUMERIC(24,2),
        snapshot_count     INTEGER,
        PRIMARY KEY (date, coin_id)
    );

EOSQL

    -- ----------------------------------------------------------------
    -- raw schema
    -- Stores the original API responses as JSONB, partitioned by
    -- endpoint type, before any transformation is applied.
    -- Each table has one row per (run_id, coin_id).
    -- ----------------------------------------------------------------
    CREATE SCHEMA IF NOT EXISTS raw;

    -- /coins/markets — one row per (run_id, coin_id, source_endpoint)
    -- payload_hash: md5 of sorted-key JSON, useful for change detection across runs
    CREATE TABLE IF NOT EXISTS raw.coin_market_responses (
        id              BIGSERIAL     PRIMARY KEY,
        snapshot_ts     TIMESTAMPTZ   NOT NULL,
        run_id          VARCHAR(250)  NOT NULL,
        coin_id         VARCHAR(100)  NOT NULL,
        source_endpoint VARCHAR(200)  NOT NULL DEFAULT '/coins/markets',
        raw_payload     JSONB         NOT NULL,
        payload_hash    TEXT,
        inserted_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_raw_market UNIQUE (run_id, coin_id, source_endpoint)
    );

    CREATE INDEX IF NOT EXISTS idx_raw_market_run
        ON raw.coin_market_responses (run_id, inserted_at DESC);

    -- /coins/{id} full response — one row per (run_id, coin_id, source_endpoint)
    CREATE TABLE IF NOT EXISTS raw.coin_metadata_responses (
        id              BIGSERIAL     PRIMARY KEY,
        snapshot_ts     TIMESTAMPTZ   NOT NULL,
        run_id          VARCHAR(250)  NOT NULL,
        coin_id         VARCHAR(100)  NOT NULL,
        source_endpoint VARCHAR(200)  NOT NULL DEFAULT '/coins/{id}',
        raw_payload     JSONB         NOT NULL,
        payload_hash    TEXT,
        inserted_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_raw_metadata UNIQUE (run_id, coin_id, source_endpoint)
    );

    CREATE INDEX IF NOT EXISTS idx_raw_metadata_run
        ON raw.coin_metadata_responses (run_id, inserted_at DESC);

    -- developer_data block — one row per (run_id, coin_id, source_endpoint)
    CREATE TABLE IF NOT EXISTS raw.coin_dev_responses (
        id              BIGSERIAL     PRIMARY KEY,
        snapshot_ts     TIMESTAMPTZ   NOT NULL,
        run_id          VARCHAR(250)  NOT NULL,
        coin_id         VARCHAR(100)  NOT NULL,
        source_endpoint VARCHAR(200)  NOT NULL DEFAULT '/coins/{id}#developer_data',
        raw_payload     JSONB         NOT NULL,
        payload_hash    TEXT,
        inserted_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_raw_dev UNIQUE (run_id, coin_id, source_endpoint)
    );

    CREATE INDEX IF NOT EXISTS idx_raw_dev_run
        ON raw.coin_dev_responses (run_id, inserted_at DESC);

EOSQL

echo ">>> Creating orchestration schema in crypto_data..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "crypto_data" <<-EOSQL

    -- ----------------------------------------------------------------
    -- orchestration schema
    -- Pipeline audit / control table.
    -- One row per DAG run; tracks status, row counts and errors.
    -- ----------------------------------------------------------------
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
        finished_at         TIMESTAMPTZ
    );

    COMMENT ON TABLE orchestration.pipeline_runs IS
        'One row per DAG run. status: running → success | partial | failed';
    COMMENT ON COLUMN orchestration.pipeline_runs.status IS
        'running=in progress, success=all coins ok, partial=some failed, failed=unhandled exception';

EOSQL

echo ">>> crypto_data initialisation complete."
