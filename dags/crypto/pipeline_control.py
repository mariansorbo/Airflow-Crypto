"""
Pipeline control layer — writes run-level metadata to
orchestration.pipeline_runs for monitoring, auditing and debugging.

One row per DAG run. Lifecycle:
    1. start_run()  → INSERT  status='running'   (called in get_coin_universe)
    2. finish_run() → UPDATE  status='success' or 'partial'  (called in finalize_run)
    3. fail_run()   → UPDATE  status='failed'    (called in on_failure_callback)

Status values:
    running  — pipeline started, not yet finished
    success  — all 50 coins processed without errors
    partial  — pipeline finished but some coins failed validation
    failed   — an unhandled exception caused a task to fail
"""

import logging
from typing import Optional

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

def _get_conn(conn_id: str):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=conn_id).get_conn()


# ---------------------------------------------------------------------------
# 1. start_run — INSERT at the beginning of the pipeline
# ---------------------------------------------------------------------------

def start_run(
    conn_id:        str,
    run_id:         str,
    dag_id:         str,
    run_type:       str,
    snapshot_ts:    str,
    coins_expected: int,
) -> None:
    """
    Inserts a new row in orchestration.pipeline_runs with status='running'.
    Called at the start of get_coin_universe so every run is registered
    even if the pipeline fails early.

    Uses ON CONFLICT DO NOTHING so re-triggering the same run_id is safe.
    """
    sql = """
        INSERT INTO orchestration.pipeline_runs
            (run_id, dag_id, run_type, snapshot_ts, status, coins_expected)
        VALUES (%s, %s, %s, %s, 'running', %s)
        ON CONFLICT (run_id) DO NOTHING
    """
    conn = _get_conn(conn_id)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (run_id, dag_id, run_type, snapshot_ts, coins_expected))
        conn.commit()
        log.info("pipeline_runs: run started — run_id=%s run_type=%s", run_id, run_type)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 2. finish_run — UPDATE at the end of the pipeline
# ---------------------------------------------------------------------------

def finish_run(
    conn_id:             str,
    run_id:              str,
    coins_processed:     int,
    coins_expected:      int,
    raw_rows_inserted:   int,
    clean_rows_inserted: int,
) -> None:
    """
    Updates the run row with final counts and sets status to:
      - 'success' if coins_processed == coins_expected
      - 'partial'  if at least one coin was processed but fewer than expected
                   (e.g. some coins failed validation or metadata fetch failed)

    Called in the finalize_run task, which is the last task in the DAG.
    """
    status = "success" if coins_processed >= coins_expected else "partial"

    sql = """
        UPDATE orchestration.pipeline_runs SET
            finished_at         = NOW(),
            status              = %s,
            coins_processed     = %s,
            raw_rows_inserted   = %s,
            clean_rows_inserted = %s
        WHERE run_id = %s
    """
    conn = _get_conn(conn_id)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (status, coins_processed, raw_rows_inserted, clean_rows_inserted, run_id))
        conn.commit()
        log.info(
            "pipeline_runs: run finished — run_id=%s status=%s coins=%d/%d raw=%d clean=%d",
            run_id, status, coins_processed, coins_expected, raw_rows_inserted, clean_rows_inserted,
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 3. fail_run — UPDATE on unhandled exception
# ---------------------------------------------------------------------------

def fail_run(
    conn_id:       str,
    run_id:        str,
    error_message: str,
) -> None:
    """
    Marks the run as failed with the exception message.
    Called from the DAG's on_failure_callback so any task failure
    is automatically recorded.

    Uses ON CONFLICT DO UPDATE so it works even if start_run never ran
    (e.g. failure in get_coin_universe itself).
    """
    sql = """
        INSERT INTO orchestration.pipeline_runs
            (run_id, dag_id, status, error_message)
        VALUES (%s, 'crypto_market_snapshots', 'failed', %s)
        ON CONFLICT (run_id) DO UPDATE SET
            status        = 'failed',
            finished_at   = NOW(),
            error_message = EXCLUDED.error_message
    """
    try:
        conn = _get_conn(conn_id)
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (run_id, error_message[:2000]))
            conn.commit()
            log.info("pipeline_runs: run marked as failed — run_id=%s", run_id)
        finally:
            conn.close()
    except Exception as exc:
        # Never let control-table errors propagate and mask the real error
        log.error("pipeline_runs: could not record failure — %s", exc)
