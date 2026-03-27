"""
Backfill utilities for crypto_backfill_manager DAG.

Two responsibilities:
    1. detect_gaps()   — queries pipeline_runs, finds missing 30-min slots,
                         inserts them into orchestration.backfill_queue
    2. trigger_runs()  — reads pending rows from backfill_queue,
                         POSTs a dagRun to crypto_market_snapshots for each,
                         marks them as triggered
"""

import logging
import time
from datetime import timezone

import requests
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

CONN_ID      = "crypto_postgres"
TARGET_DAG   = "crypto_market_snapshots"
AIRFLOW_URL  = "http://airflow-webserver:8080"
TRIGGER_DELAY_SECONDS = 5   # wait between successive dagRun POSTs


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_conn():
    return PostgresHook(postgres_conn_id=CONN_ID).get_conn()


def _airflow_session() -> requests.Session:
    """Returns a requests Session authenticated against the Airflow REST API."""
    session = requests.Session()
    session.auth = ("admin", "admin")   # same creds used in the UI
    session.headers.update({"Content-Type": "application/json"})
    return session


# ---------------------------------------------------------------------------
# 1. detect_gaps — find missing 30-min slots and enqueue them
# ---------------------------------------------------------------------------

GAPS_SQL = """
WITH expected AS (
    SELECT generate_series(
        date_trunc('hour', (SELECT MIN(snapshot_ts) FROM orchestration.pipeline_runs))
        + INTERVAL '30 minutes' * FLOOR(
            EXTRACT(MINUTE FROM (SELECT MIN(snapshot_ts) FROM orchestration.pipeline_runs)) / 30
          ),
        NOW(),
        INTERVAL '30 minutes'
    ) AS expected_ts
),
actual AS (
    SELECT
        date_trunc('hour', snapshot_ts)
        + INTERVAL '30 minutes' * FLOOR(EXTRACT(MINUTE FROM snapshot_ts) / 30) AS slot_ts
    FROM orchestration.pipeline_runs
    WHERE status IN ('success', 'partial')
)
SELECT
    e.expected_ts,
    to_char(e.expected_ts AT TIME ZONE 'America/Buenos_Aires', 'YYYY-MM-DD HH24:MI') AS hora_local
FROM expected e
LEFT JOIN actual a ON a.slot_ts = e.expected_ts
WHERE a.slot_ts IS NULL
  AND e.expected_ts < NOW() - INTERVAL '30 minutes'
ORDER BY e.expected_ts;
"""


def detect_gaps() -> int:
    """
    Finds all missing 30-min slots and inserts them into backfill_queue
    with status='pending'. Already-queued slots are skipped (ON CONFLICT).

    Returns the number of new gaps inserted.
    """
    conn = _get_conn()
    inserted = 0
    try:
        with conn.cursor() as cur:
            cur.execute(GAPS_SQL)
            rows = cur.fetchall()

        if not rows:
            log.info("detect_gaps: no missing slots found")
            return 0

        log.info("detect_gaps: %d missing slots detected", len(rows))

        with conn.cursor() as cur:
            for slot_ts, hora_local in rows:
                cur.execute(
                    """
                    INSERT INTO orchestration.backfill_queue (slot_ts, hora_local, status)
                    VALUES (%s, %s, 'pending')
                    ON CONFLICT (slot_ts) DO NOTHING
                    """,
                    (slot_ts, hora_local),
                )
                if cur.rowcount:
                    inserted += 1
                    log.info("  queued: %s (%s)", slot_ts, hora_local)
                else:
                    log.info("  skip (already queued): %s", slot_ts)

        conn.commit()
        log.info("detect_gaps: %d new slots added to backfill_queue", inserted)
        return inserted

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 2. trigger_runs — POST a dagRun for each pending slot
# ---------------------------------------------------------------------------

def trigger_runs() -> int:
    """
    Reads all pending rows from backfill_queue and triggers a dagRun
    of crypto_market_snapshots for each one via the Airflow REST API.

    Marks each row as 'triggered' after a successful POST.
    Returns the number of runs triggered.
    """
    conn    = _get_conn()
    session = _airflow_session()
    url     = f"{AIRFLOW_URL}/api/v1/dags/{TARGET_DAG}/dagRuns"
    triggered = 0

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, slot_ts, hora_local FROM orchestration.backfill_queue "
                "WHERE status = 'pending' ORDER BY slot_ts"
            )
            pending = cur.fetchall()

        if not pending:
            log.info("trigger_runs: no pending slots in backfill_queue")
            return 0

        log.info("trigger_runs: %d pending slots to trigger", len(pending))

        for row_id, slot_ts, hora_local in pending:
            slot_str = slot_ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")

            payload = {
                "conf": {
                    "backfill_slot": slot_str,
                    "triggered_by":  "crypto_backfill_manager",
                },
                "note": f"backfill for slot {hora_local}",
            }

            try:
                resp = session.post(url, json=payload, timeout=10)
                resp.raise_for_status()
                dag_run_id = resp.json().get("dag_run_id", "unknown")
                log.info("  triggered dagRun=%s for slot %s", dag_run_id, hora_local)

                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE orchestration.backfill_queue "
                        "SET status='triggered', triggered_at=NOW() WHERE id=%s",
                        (row_id,),
                    )
                conn.commit()
                triggered += 1

            except requests.HTTPError as exc:
                log.error("  FAILED to trigger slot %s: %s — %s",
                          hora_local, exc, exc.response.text if exc.response else "")

            time.sleep(TRIGGER_DELAY_SECONDS)

        log.info("trigger_runs: %d dagRuns triggered", triggered)
        return triggered

    finally:
        conn.close()
        session.close()
