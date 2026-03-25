"""
Raw load layer — persists the original API JSON payloads into the
raw schema in PostgreSQL before any transformation takes place.

Tables written:
    raw.coin_market_responses   ← /coins/markets payload (1 row per coin per run)
    raw.coin_metadata_responses ← /coins/{id} full response (1 row per coin per run)
    raw.coin_dev_responses      ← developer_data block extracted from metadata
                                  (1 row per coin per run)

Each table stores:
    snapshot_ts     — canonical timestamp of the DAG run (data_interval_end)
    run_id          — Airflow run_id, unique per DAG execution
    coin_id         — CoinGecko coin identifier
    source_endpoint — which API endpoint produced this payload
    raw_payload     — JSONB, the exact response as received from CoinGecko
    inserted_at     — DB insertion timestamp (set by DEFAULT NOW())

All inserts use ON CONFLICT (run_id, coin_id) DO NOTHING so re-runs are safe.
"""

import hashlib
import json
import logging
from typing import Dict, Optional

from psycopg2.extras import execute_values, Json

log = logging.getLogger(__name__)


def _payload_hash(payload: dict) -> str:
    """
    MD5 of the JSON payload serialized with sorted keys.
    Sorting keys makes the hash deterministic regardless of dict insertion order.
    Useful for detecting whether the same coin data changed between runs.

    Example:
        _payload_hash({"b": 2, "a": 1}) == _payload_hash({"a": 1, "b": 2})  → True
    """
    canonical = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.md5(canonical.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

def _get_conn(conn_id: str):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=conn_id).get_conn()


# ---------------------------------------------------------------------------
# Individual table writers
# ---------------------------------------------------------------------------

def save_market_responses(
    conn,
    snapshot_ts: str,
    run_id: str,
    run_type: str,
    market_data: Dict[str, dict],
) -> None:
    """
    Inserts raw /coins/markets payloads into raw.coin_market_responses.

    Input:
        market_data — Dict[coin_id → raw dict from CoinGecko /coins/markets]
                      Example entry:
                      {
                        "id": "bitcoin",
                        "current_price": 68000.0,
                        "market_cap": 1_340_000_000_000,
                        "total_volume": 38_000_000_000,
                        "circulating_supply": 19_600_000.0,
                        "market_cap_rank": 1,
                        ...
                      }
    """
    if not market_data:
        log.warning("save_market_responses: empty market_data, nothing to save")
        return

    sql = """
        INSERT INTO raw.coin_market_responses
            (snapshot_ts, run_id, run_type, coin_id, source_endpoint, raw_payload, payload_hash)
        VALUES %s
        ON CONFLICT (run_id, coin_id, source_endpoint) DO NOTHING
    """
    values = [
        (snapshot_ts, run_id, run_type, coin_id, "/coins/markets", Json(payload), _payload_hash(payload))
        for coin_id, payload in market_data.items()
        if payload is not None
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()
    log.info("Saved %d rows into raw.coin_market_responses (run_id=%s)", len(values), run_id)


def save_metadata_responses(
    conn,
    snapshot_ts: str,
    run_id: str,
    run_type: str,
    metadata: Dict[str, Optional[dict]],
) -> None:
    """
    Inserts raw /coins/{id} payloads into raw.coin_metadata_responses.
    Coins where the API call returned None (e.g. rate limit) are skipped.

    Input:
        metadata — Dict[coin_id → full /coins/{id} response or None]
                   Example entry:
                   {
                     "id": "bitcoin",
                     "symbol": "btc",
                     "name": "Bitcoin",
                     "categories": ["Cryptocurrency", "Layer 1"],
                     "links": { "homepage": [...], "repos_url": {...} },
                     "developer_data": { "forks": 36000, "stars": 78000, ... },
                     ...
                   }
    """
    if not metadata:
        log.warning("save_metadata_responses: empty metadata, nothing to save")
        return

    sql = """
        INSERT INTO raw.coin_metadata_responses
            (snapshot_ts, run_id, run_type, coin_id, source_endpoint, raw_payload, payload_hash)
        VALUES %s
        ON CONFLICT (run_id, coin_id, source_endpoint) DO NOTHING
    """
    values = [
        (snapshot_ts, run_id, run_type, coin_id, "/coins/{id}", Json(payload), _payload_hash(payload))
        for coin_id, payload in metadata.items()
        if payload is not None          # skip coins where the API call failed
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()
    log.info(
        "Saved %d/%d rows into raw.coin_metadata_responses (run_id=%s)",
        len(values), len(metadata), run_id,
    )


def save_dev_responses(
    conn,
    snapshot_ts: str,
    run_id: str,
    run_type: str,
    dev_metrics: Dict[str, dict],
) -> None:
    """
    Inserts the developer_data block into raw.coin_dev_responses.
    This is NOT a separate API call — it is extracted from the metadata
    response already saved in raw.coin_metadata_responses.
    Stored separately for easier querying.

    Input:
        dev_metrics — Dict[coin_id → developer_data dict]
                      Example entry:
                      {
                        "forks": 36000,
                        "stars": 78000,
                        "subscribers": 4200,
                        "total_issues": 8100,
                        "closed_issues": 7900,
                        "pull_requests_merged": 12400,
                        "commit_count_4_weeks": 45,
                        "code_additions_deletions_4_weeks": {
                          "additions": 2100,
                          "deletions": -900
                        }
                      }
    """
    if not dev_metrics:
        log.warning("save_dev_responses: empty dev_metrics, nothing to save")
        return

    sql = """
        INSERT INTO raw.coin_dev_responses
            (snapshot_ts, run_id, run_type, coin_id, source_endpoint, raw_payload, payload_hash)
        VALUES %s
        ON CONFLICT (run_id, coin_id, source_endpoint) DO NOTHING
    """
    values = [
        (snapshot_ts, run_id, run_type, coin_id, "/coins/{id}#developer_data", Json(dev_data), _payload_hash(dev_data))
        for coin_id, dev_data in dev_metrics.items()
        if dev_data  # skip empty dicts
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()
    log.info("Saved %d rows into raw.coin_dev_responses (run_id=%s)", len(values), run_id)


# ---------------------------------------------------------------------------
# Convenience: save all three raw tables in one call
# ---------------------------------------------------------------------------

def save_all_raw(
    conn_id: str,
    snapshot_ts: str,
    run_id: str,
    run_type: str,
    market_data: Dict[str, dict],
    metadata: Dict[str, Optional[dict]],
    dev_metrics: Dict[str, dict],
) -> None:
    """
    Opens one connection and writes all three raw tables.
    Called from the load_raw_data task in the DAG.

    Load order:
        1. raw.coin_market_responses
        2. raw.coin_metadata_responses
        3. raw.coin_dev_responses
    """
    conn = _get_conn(conn_id)
    try:
        save_market_responses(conn, snapshot_ts, run_id, run_type, market_data)
        save_metadata_responses(conn, snapshot_ts, run_id, run_type, metadata)
        save_dev_responses(conn, snapshot_ts, run_id, run_type, dev_metrics)
    finally:
        conn.close()

    log.info(
        "save_all_raw complete — market=%d, metadata=%d, dev=%d (run_id=%s)",
        len(market_data),
        len(metadata),
        len(dev_metrics),
        run_id,
    )
