"""
Load layer — writes transformed rows to PostgreSQL using Airflow's
PostgresHook (connection ID: crypto_postgres).

Table load order respects FK constraints:
    1. coins_dim        (upsert — must exist before snapshots/dev_metrics)
    2. coin_market_snapshots
    3. coin_dev_metrics
    4. coin_raw_responses
    5. coin_daily_summary   (rebuild on demand via build_daily_summary)
"""

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

CONN_ID = "crypto_postgres"


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

def _get_conn(conn_id: str = CONN_ID):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=conn_id).get_conn()


# ---------------------------------------------------------------------------
# 1. coins_dim — upsert
# ---------------------------------------------------------------------------

def upsert_coins_dim(conn, dims: List[Dict]) -> None:
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
            d["updated_at"],   # first_seen_at — only written on first insert
            d["updated_at"],   # updated_at — overwritten on every upsert
        )
        for d in dims
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()
    log.info("Upserted %d rows into coins_dim", len(dims))


# ---------------------------------------------------------------------------
# 2. coin_market_snapshots
# ---------------------------------------------------------------------------

def insert_snapshots(conn, snapshots: List[Dict]) -> None:
    if not snapshots:
        return

    sql = """
        INSERT INTO coin_market_snapshots
            (snapshot_ts, coin_id, price_usd, market_cap_usd, volume_24h_usd,
             circulating_supply, total_supply, max_supply, market_cap_rank,
             run_type, origin_updated_time)
        VALUES %s
        ON CONFLICT (snapshot_ts, coin_id) DO NOTHING
    """
    # Nota: el trigger trg_check_origin_updated_time se dispara BEFORE INSERT.
    # Si origin_updated_time no es más nuevo que el último registrado para ese
    # coin_id, el trigger cancela el insert y lo redirige a
    # coin_market_snapshots_not_updated. El ON CONFLICT nunca llega a evaluarse
    # en ese caso porque el insert ya fue cancelado por el trigger.
    values = [
        (
            s["snapshot_ts"],
            s["coin_id"],
            s["price_usd"],
            s["market_cap_usd"],
            s["volume_24h_usd"],
            s["circulating_supply"],
            s["total_supply"],
            s["max_supply"],
            s["market_cap_rank"],
            s.get("run_type", "scheduled"),
            s.get("origin_updated_time"),
        )
        for s in snapshots
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()
    log.info("Inserted %d rows into coin_market_snapshots", len(snapshots))


# ---------------------------------------------------------------------------
# 3. coin_dev_metrics
# ---------------------------------------------------------------------------

def insert_dev_metrics(conn, dev_rows: List[Dict]) -> None:
    if not dev_rows:
        return

    sql = """
        INSERT INTO coin_dev_metrics
            (snapshot_ts, coin_id, github_stars, github_forks, dev_metric_raw, fetched_at, run_type)
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
# 4. coin_raw_responses
# ---------------------------------------------------------------------------

def insert_raw_responses(conn, raws: List[Dict]) -> None:
    if not raws:
        return

    sql = """
        INSERT INTO coin_raw_responses
            (snapshot_ts, coin_id, source_endpoint, raw_payload, inserted_at)
        VALUES %s
        ON CONFLICT (snapshot_ts, coin_id, source_endpoint) DO NOTHING
    """
    values = [
        (
            r["snapshot_ts"],
            r["coin_id"],
            r["source_endpoint"],
            r["raw_payload"],
            r["inserted_at"],
        )
        for r in raws
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()
    log.info("Inserted %d rows into coin_raw_responses", len(raws))


# ---------------------------------------------------------------------------
# 5. coin_daily_summary — rebuilt on every run for the target date
# ---------------------------------------------------------------------------

_DAILY_SUMMARY_SQL = """
WITH ranked AS (
    SELECT
        coin_id,
        DATE(snapshot_ts AT TIME ZONE 'UTC')  AS dt,
        price_usd,
        volume_24h_usd,
        market_cap_usd,
        ROW_NUMBER() OVER (
            PARTITION BY coin_id, DATE(snapshot_ts AT TIME ZONE 'UTC')
            ORDER BY snapshot_ts ASC
        ) AS rn_asc,
        ROW_NUMBER() OVER (
            PARTITION BY coin_id, DATE(snapshot_ts AT TIME ZONE 'UTC')
            ORDER BY snapshot_ts DESC
        ) AS rn_desc
    FROM coin_market_snapshots
    WHERE DATE(snapshot_ts AT TIME ZONE 'UTC') = %(target_date)s
),
agg AS (
    SELECT
        dt                                                    AS date,
        coin_id,
        MAX(CASE WHEN rn_asc  = 1 THEN price_usd END)        AS open_price_usd,
        MAX(price_usd)                                        AS high_price_usd,
        MIN(price_usd)                                        AS low_price_usd,
        MAX(CASE WHEN rn_desc = 1 THEN price_usd END)         AS close_price_usd,
        AVG(volume_24h_usd)                                   AS avg_volume_24h_usd,
        AVG(market_cap_usd)                                   AS avg_market_cap_usd,
        COUNT(*)                                              AS snapshot_count
    FROM ranked
    GROUP BY dt, coin_id
)
INSERT INTO coin_daily_summary
    (date, coin_id, open_price_usd, high_price_usd, low_price_usd,
     close_price_usd, avg_volume_24h_usd, avg_market_cap_usd, snapshot_count)
SELECT * FROM agg
ON CONFLICT (date, coin_id) DO UPDATE SET
    high_price_usd     = GREATEST(coin_daily_summary.high_price_usd,  EXCLUDED.high_price_usd),
    low_price_usd      = LEAST(coin_daily_summary.low_price_usd,      EXCLUDED.low_price_usd),
    close_price_usd    = EXCLUDED.close_price_usd,
    avg_volume_24h_usd = EXCLUDED.avg_volume_24h_usd,
    avg_market_cap_usd = EXCLUDED.avg_market_cap_usd,
    snapshot_count     = EXCLUDED.snapshot_count
"""


def build_daily_summary(conn_id: str = CONN_ID, snapshot_ts: Any = None) -> None:
    target_date: date
    if snapshot_ts is None:
        from datetime import timezone
        target_date = datetime.now(tz=timezone.utc).date()
    elif hasattr(snapshot_ts, "date"):
        target_date = snapshot_ts.date()
    else:
        # ISO string fallback
        target_date = datetime.fromisoformat(str(snapshot_ts)).date()

    conn = _get_conn(conn_id)
    try:
        with conn.cursor() as cur:
            cur.execute(_DAILY_SUMMARY_SQL, {"target_date": target_date})
        conn.commit()
        log.info("Built/updated daily summary for %s", target_date)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Stub helper — guarantees every snapshot coin has a coins_dim row
# ---------------------------------------------------------------------------

def _ensure_dim_stubs(conn, snapshots: List[Dict], dims: List[Dict]) -> None:
    """
    For every coin_id that appears in snapshots but is absent from dims
    (i.e. metadata fetch failed for that coin), insert a minimal stub row
    so downstream inserts don't fail.
    Stubs are upserted with ON CONFLICT DO NOTHING on the metadata fields,
    so they get filled in properly the next time metadata succeeds.
    """
    dim_ids = {d["coin_id"] for d in dims}
    missing = [s["coin_id"] for s in snapshots if s["coin_id"] not in dim_ids]

    if not missing:
        return

    log.warning(
        "%d coins have no metadata — inserting stub dim rows: %s",
        len(missing),
        missing,
    )

    now_str = snapshots[0]["snapshot_ts"] if snapshots else datetime.utcnow().isoformat()
    stubs = [
        {
            "coin_id":    coin_id,
            "symbol":     coin_id[:20].upper(),
            "name":       coin_id,
            "categories": "[]",
            "website_url": None,
            "github_url":  None,
            "updated_at":  now_str,
        }
        for coin_id in missing
    ]
    upsert_coins_dim(conn, stubs)


# ---------------------------------------------------------------------------
# Convenience: load all tables in one call
# ---------------------------------------------------------------------------

def load_all(
    conn_id: str = CONN_ID,
    snapshots: Optional[List[Dict]] = None,
    dims: Optional[List[Dict]] = None,
    dev_rows: Optional[List[Dict]] = None,
    raws: Optional[List[Dict]] = None,
) -> None:
    snapshots = snapshots or []
    dims      = dims      or []
    dev_rows  = dev_rows  or []
    raws      = raws      or []

    conn = _get_conn(conn_id)
    try:
        upsert_coins_dim(conn, dims)
        _ensure_dim_stubs(conn, snapshots, dims)   # fill gaps if metadata failed
        insert_snapshots(conn, snapshots)
        insert_dev_metrics(conn, dev_rows)
        insert_raw_responses(conn, raws)
    finally:
        conn.close()

    log.info(
        "load_all complete — dims=%d (+stubs), snapshots=%d, dev=%d, raw=%d",
        len(dims),
        len(snapshots),
        len(dev_rows),
        len(raws),
    )
