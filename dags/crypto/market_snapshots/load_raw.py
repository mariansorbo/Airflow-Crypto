"""
Raw load layer — market snapshots pipeline.

Persists the raw /coins/markets JSON payload into raw.coin_market_responses
before any transformation takes place.

Table written:
    raw.coin_market_responses  — 1 row per coin per run
        snapshot_ts     — canonical timestamp of the DAG run
        run_id          — Airflow run_id (unique per execution)
        run_type        — scheduled / manual / backfill
        coin_id         — CoinGecko coin identifier
        source_endpoint — '/coins/markets'
        raw_payload     — JSONB, exact response from CoinGecko
        payload_hash    — MD5 of the JSON (sorted keys) for change detection
        inserted_at     — DB insertion timestamp (DEFAULT NOW())

Inserts use ON CONFLICT (run_id, coin_id, source_endpoint) DO NOTHING
so re-runs of the same Airflow run_id are safe.
"""

import hashlib
import json
import logging
from typing import Dict

from psycopg2.extras import execute_values, Json

log = logging.getLogger(__name__)


def _payload_hash(payload: dict) -> str:
    """
    MD5 of the JSON payload with sorted keys.
    Sorting guarantees the hash is deterministic regardless of dict order.
    Useful for detecting whether the same coin data changed between runs.
    """
    canonical = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.md5(canonical.encode()).hexdigest()


def _get_conn(conn_id: str):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=conn_id).get_conn()


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
                      e.g. {"bitcoin": {"current_price": 87000, "market_cap": ..., ...}}
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


def save_all_raw(
    conn_id: str,
    snapshot_ts: str,
    run_id: str,
    run_type: str,
    market_data: Dict[str, dict],
) -> None:
    """
    Opens one connection and writes raw.coin_market_responses.
    Called from the load_raw_data task in the DAG.
    """
    conn = _get_conn(conn_id)
    try:
        save_market_responses(conn, snapshot_ts, run_id, run_type, market_data)
    finally:
        conn.close()
    log.info("save_all_raw complete — market=%d (run_id=%s)", len(market_data), run_id)
