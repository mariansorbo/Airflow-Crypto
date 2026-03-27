"""
Raw retrieval layer — market snapshots pipeline.

Reads the latest (or a specific) market batch from raw.coin_market_responses
and reconstructs the payload dict the rest of the pipeline expects.

Decoupling chain:
    extract.py          → hits /coins/markets API
    load_raw.py         → persists raw JSON to raw.coin_market_responses
    extract_from_raw.py → retrieves that JSON back (this file)
    validate.py         → validates the retrieved payload
    transform.py        → transforms validated data

Supports two resolution modes:
    run_id      → fetch exact batch for that Airflow run
    target_ts   → fetch the closest batch whose inserted_at <= target_ts
                  (used by the backfill pipeline)
    neither     → fetch the most recent batch available
"""

import logging
from typing import Dict, Optional, Tuple

log = logging.getLogger(__name__)


def _get_conn(conn_id: str):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=conn_id).get_conn()


def get_run_id_for_ts(conn, target_ts: str) -> Optional[str]:
    """
    Returns the run_id whose inserted_at is the closest one <= target_ts.
    Used when doing historical backfill to find the right raw batch.
    """
    sql = """
        SELECT run_id
        FROM   raw.coin_market_responses
        WHERE  inserted_at <= %s
        ORDER  BY inserted_at DESC
        LIMIT  1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (target_ts,))
        row = cur.fetchone()
    return row[0] if row else None


def get_latest_run_id(conn) -> Optional[str]:
    """
    Returns the most recently inserted run_id in raw.coin_market_responses.
    Returns None if the table is empty.
    """
    sql = "SELECT run_id FROM raw.coin_market_responses ORDER BY inserted_at DESC LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    return row[0] if row else None


def fetch_market_batch(conn, run_id: str) -> Dict[str, dict]:
    """
    Retrieves all rows from raw.coin_market_responses for a given run_id.

    Output shape (same as extract.fetch_market_snapshot):
        {
          "bitcoin":  {"id": "bitcoin", "current_price": 87000, ...},
          "ethereum": {"id": "ethereum", "current_price": 2100, ...},
          ...
        }
    """
    sql = """
        SELECT coin_id, raw_payload
        FROM   raw.coin_market_responses
        WHERE  run_id = %s
        ORDER  BY coin_id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_id,))
        rows = cur.fetchall()

    result = {row[0]: row[1] for row in rows}
    log.info("fetch_market_batch: retrieved %d coins (run_id=%s)", len(result), run_id)
    return result


def get_latest_batch(
    conn_id: str,
    run_id: Optional[str] = None,
    target_ts: Optional[str] = None,
) -> Tuple[str, Dict[str, dict]]:
    """
    Retrieves the market payload for the given run_id (or resolves it).

    Resolution priority:
        1. run_id provided      → use it directly
        2. target_ts provided   → find closest run_id with inserted_at <= target_ts
        3. neither              → use the most recent available run_id

    Returns:
        (resolved_run_id, market_data)

    Raises:
        ValueError if raw.coin_market_responses is empty.
    """
    conn = _get_conn(conn_id)
    try:
        if run_id is None:
            if target_ts is not None:
                run_id = get_run_id_for_ts(conn, target_ts)
                log.info("Resolved run_id=%s for target_ts=%s", run_id, target_ts)
            else:
                run_id = get_latest_run_id(conn)
                log.info("No run_id specified — using latest: %s", run_id)

        if run_id is None:
            raise ValueError(
                "raw.coin_market_responses is empty — "
                "run the pipeline at least once before calling extract_from_raw."
            )

        market_data = fetch_market_batch(conn, run_id)
    finally:
        conn.close()

    log.info("get_latest_batch complete — run_id=%s | market=%d", run_id, len(market_data))
    return run_id, market_data
