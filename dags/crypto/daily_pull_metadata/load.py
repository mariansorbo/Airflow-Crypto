"""
Load layer — daily metadata pull pipeline.

Writes coins_dim and coin_dev_metrics to PostgreSQL.
No raw layer — the daily pull goes directly extract → validate → transform → load.

Table load order:
    1. coins_dim        — UPSERT: ON CONFLICT (coin_id) DO UPDATE
                          Always overwrites with the latest metadata.
    2. coin_dev_metrics — INSERT: ON CONFLICT (snapshot_ts, coin_id) DO NOTHING
                          Each daily run creates one row per coin per day.
"""

import logging
from typing import Dict, List, Optional

from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

CONN_ID = "crypto_postgres"


def _get_conn(conn_id: str = CONN_ID):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=conn_id).get_conn()


# ---------------------------------------------------------------------------
# 1. coins_dim — upsert
# ---------------------------------------------------------------------------

def upsert_coins_dim(conn, dims: List[Dict]) -> None:
    """
    Upserts rows into coins_dim.
    On conflict (same coin_id): overwrites symbol, name, categories,
    website_url, github_url, updated_at with fresh data from CoinGecko.
    first_seen_at is never overwritten.
    """
    if not dims:
        return

    sql = """
        INSERT INTO coins_dim
            (coin_id, symbol, name, categories, website_url, github_url, first_seen_at, updated_at)
        VALUES %s
        ON CONFLICT (coin_id) DO UPDATE SET
            symbol      = EXCLUDED.symbol,
            name        = EXCLUDED.name,
            categories  = EXCLUDED.categories::jsonb,
            website_url = EXCLUDED.website_url,
            github_url  = EXCLUDED.github_url,
            updated_at  = EXCLUDED.updated_at
    """
    values = [
        (
            d["coin_id"],
            d["symbol"],
            d["name"],
            d["categories"],
            d["website_url"],
            d["github_url"],
            d["updated_at"],   # first_seen_at — written only on first insert
            d["updated_at"],   # updated_at    — overwritten on every upsert
        )
        for d in dims
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()
    log.info("Upserted %d rows into coins_dim", len(dims))


# ---------------------------------------------------------------------------
# 2. coin_dev_metrics — insert
# ---------------------------------------------------------------------------

def insert_dev_metrics(conn, dev_rows: List[Dict]) -> None:
    """
    Inserts developer metrics rows into coin_dev_metrics.
    ON CONFLICT DO NOTHING: safe if the daily DAG runs more than once
    on the same day for the same coin.
    """
    if not dev_rows:
        return

    sql = """
        INSERT INTO coin_dev_metrics
            (snapshot_ts, coin_id, github_stars, github_forks,
             dev_metric_raw, fetched_at, run_type)
        VALUES %s
        ON CONFLICT (snapshot_ts, coin_id) DO NOTHING
    """
    values = [
        (
            d["snapshot_ts"],
            d["coin_id"],
            d["github_stars"],
            d["github_forks"],
            d["dev_metric_raw"],
            d["fetched_at"],
            d.get("run_type", "scheduled"),
        )
        for d in dev_rows
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()
    log.info("Inserted %d rows into coin_dev_metrics", len(dev_rows))


# ---------------------------------------------------------------------------
# Convenience: load both tables in one call
# ---------------------------------------------------------------------------

def load_all(
    conn_id: str = CONN_ID,
    dims: Optional[List[Dict]] = None,
    dev_rows: Optional[List[Dict]] = None,
) -> None:
    """
    Opens one connection and writes coins_dim + coin_dev_metrics.
    Called from the load_metadata task in the daily pull DAG.
    """
    dims     = dims     or []
    dev_rows = dev_rows or []

    conn = _get_conn(conn_id)
    try:
        upsert_coins_dim(conn, dims)
        insert_dev_metrics(conn, dev_rows)
    finally:
        conn.close()

    log.info("load_all complete — dims=%d, dev=%d", len(dims), len(dev_rows))
