"""
Raw retrieval layer — reads the latest (or a specific) batch from the
raw schema and reconstructs the three payload dicts that the rest of
the pipeline expects.

This module decouples extraction from transformation:
  - extract.py   → hits CoinGecko API
  - load_raw.py  → persists raw JSON to raw.* tables
  - extract_from_raw.py → retrieves that JSON back (this file)
  - validate.py  → validates the retrieved payloads
  - transform.py → transforms validated payloads

The output dicts of get_latest_batch() have exactly the same shape
as the XCom values produced by the original extract tasks, so validate.py
and transform.py need zero changes.
"""

import logging
from typing import Dict, Optional, Tuple

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

def _get_conn(conn_id: str):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=conn_id).get_conn()


# ---------------------------------------------------------------------------
# Run-ID resolution
# ---------------------------------------------------------------------------

def get_latest_run_id(conn, table: str) -> Optional[str]:
    """
    Returns the most recently inserted run_id in raw.<table>.
    Returns None if the table is empty.

    Used when no specific run_id is provided — i.e., "give me the
    latest batch regardless of which Airflow run produced it."
    """
    sql = f"SELECT run_id FROM raw.{table} ORDER BY inserted_at DESC LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Per-table retrievers
# ---------------------------------------------------------------------------

def fetch_market_batch(conn, run_id: str) -> Dict[str, dict]:
    """
    Retrieves all rows from raw.coin_market_responses for a given run_id
    and returns them as Dict[coin_id → raw payload dict].

    Output shape (same as extract.fetch_market_snapshot):
        {
          "bitcoin":  { "id": "bitcoin", "current_price": 68000.0, ... },
          "ethereum": { "id": "ethereum", "current_price": 2149.0, ... },
          ...
        }
    """
    sql = """
        SELECT coin_id, raw_payload
        FROM   raw.coin_market_responses
        WHERE  run_id = %s
        ORDER BY coin_id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_id,))
        rows = cur.fetchall()

    result = {row[0]: row[1] for row in rows}
    log.info("fetch_market_batch: retrieved %d coins (run_id=%s)", len(result), run_id)
    return result


def fetch_metadata_batch(conn, run_id: str) -> Dict[str, Optional[dict]]:
    """
    Retrieves all rows from raw.coin_metadata_responses for a given run_id.
    Coins that had None responses (API failure) are absent from raw storage,
    so they will not appear here either.

    Output shape (same as extract.fetch_all_details):
        {
          "bitcoin":  { "id": "bitcoin", "symbol": "btc", "links": {...}, ... },
          "ethereum": { ... },
          ...
          # Note: coins that failed API calls are simply missing from this dict
        }
    """
    sql = """
        SELECT coin_id, raw_payload
        FROM   raw.coin_metadata_responses
        WHERE  run_id = %s
        ORDER BY coin_id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_id,))
        rows = cur.fetchall()

    result = {row[0]: row[1] for row in rows}
    log.info("fetch_metadata_batch: retrieved %d coins (run_id=%s)", len(result), run_id)
    return result


def fetch_dev_batch(conn, run_id: str) -> Dict[str, dict]:
    """
    Retrieves all rows from raw.coin_dev_responses for a given run_id.

    Output shape (same as the dev_metrics XCom from extract_dev_metrics task):
        {
          "bitcoin":  { "forks": 36000, "stars": 78000, "commit_count_4_weeks": 45, ... },
          "ethereum": { ... },
          ...
        }
    """
    sql = """
        SELECT coin_id, raw_payload
        FROM   raw.coin_dev_responses
        WHERE  run_id = %s
        ORDER BY coin_id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_id,))
        rows = cur.fetchall()

    result = {row[0]: row[1] for row in rows}
    log.info("fetch_dev_batch: retrieved %d coins (run_id=%s)", len(result), run_id)
    return result


# ---------------------------------------------------------------------------
# Main entry point — retrieves the full batch for one run
# ---------------------------------------------------------------------------

def get_latest_batch(
    conn_id: str,
    run_id: Optional[str] = None,
) -> Tuple[str, Dict[str, dict], Dict[str, Optional[dict]], Dict[str, dict]]:
    """
    Retrieves all three raw payloads for the given run_id (or the latest
    available if run_id is None).

    Returns:
        (run_id, market_data, metadata, dev_metrics)

        run_id       — the run_id that was actually queried (useful when
                       None was passed and we resolved automatically)
        market_data  — Dict[coin_id → /coins/markets payload]
        metadata     — Dict[coin_id → /coins/{id} payload]
        dev_metrics  — Dict[coin_id → developer_data block]

    Raises:
        ValueError if no data exists in raw.coin_market_responses at all.

    Example:
        run_id, market, meta, dev = get_latest_batch("crypto_postgres")
        # market["bitcoin"]["current_price"] → 68000.0
        # meta["bitcoin"]["name"]            → "Bitcoin"
        # dev["bitcoin"]["stars"]            → 78000
    """
    conn = _get_conn(conn_id)
    try:
        # Resolve run_id if not provided
        if run_id is None:
            run_id = get_latest_run_id(conn, "coin_market_responses")
            if run_id is None:
                raise ValueError(
                    "raw.coin_market_responses is empty — "
                    "run the pipeline at least once before calling extract_from_raw."
                )
            log.info("No run_id specified — using latest: %s", run_id)

        market_data = fetch_market_batch(conn, run_id)
        metadata    = fetch_metadata_batch(conn, run_id)
        dev_metrics = fetch_dev_batch(conn, run_id)

    finally:
        conn.close()

    log.info(
        "get_latest_batch complete — run_id=%s | market=%d, metadata=%d, dev=%d",
        run_id, len(market_data), len(metadata), len(dev_metrics),
    )
    return run_id, market_data, metadata, dev_metrics
