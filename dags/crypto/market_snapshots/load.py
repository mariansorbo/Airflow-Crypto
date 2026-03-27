"""
Load layer — market snapshots pipeline.

Writes transformed market snapshot rows to PostgreSQL.

Table load order:
    1. coins_dim stubs      — ensures every snapshot coin has a parent dim row
                              (real dim data is populated by the daily pull DAG)
    2. coin_market_snapshots — fact table, one row per (snapshot_ts, coin_id)
    3. coin_daily_summary   — rebuilt via CTE on demand (build_daily_summary)

Note: coins_dim and coin_dev_metrics are NOT updated here.
That is the responsibility of the crypto_daily_pull_metadata DAG.
"""

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

CONN_ID = "crypto_postgres"


def _get_conn(conn_id: str = CONN_ID):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=conn_id).get_conn()


# ---------------------------------------------------------------------------
# Stub helper — guarantees every snapshot coin has a coins_dim row
# ---------------------------------------------------------------------------

def _ensure_dim_stubs(conn, snapshots: List[Dict]) -> None:
    """
    For every coin_id in snapshots that has no row in coins_dim,
    inserts a minimal stub so the FK constraint on coin_market_snapshots
    is satisfied.

    Stubs use ON CONFLICT DO NOTHING so they never overwrite real data
    inserted by the daily pull DAG.
    """
    if not snapshots:
        return

    coin_ids = list({s["coin_id"] for s in snapshots})

    sql_check = """
        SELECT coin_id FROM coins_dim WHERE coin_id = ANY(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql_check, (coin_ids,))
        existing = {row[0] for row in cur.fetchall()}

    missing = [cid for cid in coin_ids if cid not in existing]
    if not missing:
        return

    log.warning("Inserting %d stub dim rows for coins with no metadata yet: %s", len(missing), missing)

    now_str = snapshots[0]["snapshot_ts"] if snapshots else datetime.utcnow().isoformat()
    sql_stub = """
        INSERT INTO coins_dim
            (coin_id, symbol, name, categories, website_url, github_url, first_seen_at, updated_at)
        VALUES %s
        ON CONFLICT (coin_id) DO NOTHING
    """
    values = [
        (cid, cid[:20].upper(), cid, "[]", None, None, now_str, now_str)
        for cid in missing
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql_stub, values)
    conn.commit()
    log.info("Stub dim rows inserted: %d", len(missing))


# ---------------------------------------------------------------------------
# coin_market_snapshots
# ---------------------------------------------------------------------------

def insert_snapshots(conn, snapshots: List[Dict]) -> None:
    """
    Bulk insert into coin_market_snapshots.

    ON CONFLICT DO NOTHING: safe for re-runs of the same (snapshot_ts, coin_id).

    The trigger trg_check_origin_updated_time fires BEFORE INSERT and
    redirects rows whose origin_updated_time is not newer than the last
    recorded value for that coin into coin_market_snapshots_not_updated.
    """
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
# coin_daily_summary — rebuilt on every run
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
    """Recalculates OHLCV summary for the target date. Idempotent."""
    if snapshot_ts is None:
        from datetime import timezone
        target_date = datetime.now(tz=timezone.utc).date()
    elif hasattr(snapshot_ts, "date"):
        target_date = snapshot_ts.date()
    else:
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
# Convenience: load snapshots in one call
# ---------------------------------------------------------------------------

def load_all(
    conn_id: str = CONN_ID,
    snapshots: Optional[List[Dict]] = None,
) -> None:
    """
    Opens one connection and writes coin_market_snapshots.
    Inserts stub coins_dim rows for any coin with no metadata yet.
    dims and dev_metrics are handled by the daily pull DAG — not here.
    """
    snapshots = snapshots or []

    conn = _get_conn(conn_id)
    try:
        _ensure_dim_stubs(conn, snapshots)
        insert_snapshots(conn, snapshots)
    finally:
        conn.close()

    log.info("load_all complete — snapshots=%d", len(snapshots))
