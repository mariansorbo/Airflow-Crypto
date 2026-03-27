-- V3: add orchestration.backfill_queue
-- Stores detected gap slots and their backfill trigger status.

CREATE TABLE IF NOT EXISTS orchestration.backfill_queue (
    id           BIGSERIAL    PRIMARY KEY,
    slot_ts      TIMESTAMPTZ  NOT NULL UNIQUE,
    hora_local   VARCHAR(20)  NOT NULL,
    status       VARCHAR(20)  NOT NULL DEFAULT 'pending',
    detected_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    triggered_at TIMESTAMPTZ,
    CONSTRAINT chk_bq_status CHECK (status IN ('pending', 'triggered', 'skipped'))
);

CREATE INDEX IF NOT EXISTS idx_bq_status ON orchestration.backfill_queue (status, slot_ts);

COMMENT ON TABLE orchestration.backfill_queue IS
    'Gap slots detected by crypto_backfill_manager. status: pending → triggered | skipped';
