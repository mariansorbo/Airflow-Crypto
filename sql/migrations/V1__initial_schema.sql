-- ============================================================
-- V1 — Initial schema
-- Applied: 2026-03-23
-- Description: Base tables for the crypto market snapshots pipeline.
--              public schema (clean layer) + raw schema (raw API responses)
-- ============================================================

-- ── public schema (clean layer) ─────────────────────────────────────────────

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

CREATE TABLE IF NOT EXISTS coin_dev_metrics (
    snapshot_ts     TIMESTAMPTZ   NOT NULL,
    coin_id         VARCHAR(100)  NOT NULL,
    github_stars    INTEGER,
    github_forks    INTEGER,
    dev_metric_raw  JSONB,
    fetched_at      TIMESTAMPTZ   NOT NULL,
    PRIMARY KEY (snapshot_ts, coin_id)
);

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

-- ── raw schema ───────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS raw;

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
