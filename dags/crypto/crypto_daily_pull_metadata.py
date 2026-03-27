"""
DAG: crypto_daily_pull_metadata
Schedule: once per day at 06:00 UTC
Source: CoinGecko GET /coins/{id} (1 call per coin, 50 calls total)
Target: PostgreSQL — coins_dim, coin_dev_metrics

Fetches metadata and developer metrics for all 50 coins once per day.
Separated from the 30-min market snapshot pipeline to avoid exhausting
CoinGecko's free plan quota (10,000 calls/month).

Quota math:
    50 calls/day × 30 days = 1,500 calls/month   ✅ well within 10k limit
    (vs 50 calls × 48 runs/day × 30 days = 72,000 if run every 30 min ❌)

No raw layer — data goes directly extract → validate → transform → load.
Metadata doesn't need point-in-time replay like market prices do.

Task graph:

    get_coin_universe
          │
          ▼
    extract_metadata          ← GET /coins/{id} × 50 (3s delay between calls)
          │
          ▼
    validate_metadata         ← id present, name present, response not null
          │
          ▼
    transform_metadata        ← build coins_dim + coin_dev_metrics rows
          │
          ▼
    load_metadata             ← UPSERT coins_dim, INSERT coin_dev_metrics
"""

import logging
from datetime import datetime, timezone, UTC

from airflow import DAG
from airflow.operators.python import PythonOperator

from crypto.coins import COIN_UNIVERSE
from crypto.daily_pull_metadata import extract, validate, transform, load

log = logging.getLogger(__name__)


with DAG(
    dag_id="crypto_daily_pull_metadata",
    description="Fetches coin metadata + dev metrics once per day from CoinGecko",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "coingecko", "metadata", "daily"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
    },
) as dag:

    # -----------------------------------------------------------------------
    # Task 1 — get_coin_universe
    #
    # Reads the static list of 50 coin IDs and captures the run timestamp.
    #
    # Output (XCom):
    #   coin_ids    → List[str]
    #   snapshot_ts → str (ISO-8601) — used as the timestamp for dev metrics rows
    # -----------------------------------------------------------------------
    def _get_coin_universe(**ctx):
        ti          = ctx["ti"]
        snapshot_ts = datetime.now(UTC).isoformat()

        ti.xcom_push(key="coin_ids",    value=COIN_UNIVERSE)
        ti.xcom_push(key="snapshot_ts", value=snapshot_ts)

        log.info("Daily pull — %d coins | snapshot_ts=%s", len(COIN_UNIVERSE), snapshot_ts)
        return f"Universe: {len(COIN_UNIVERSE)} coins | ts={snapshot_ts}"

    # -----------------------------------------------------------------------
    # Task 2 — extract_metadata
    #
    # Calls GET /coins/{id} for each of the 50 coins with a 3-second delay
    # between requests. On 429: waits 65s and retries up to 2 times.
    # Coins where the call fails are stored as None and filtered out in validate.
    #
    # Input:  coin_ids (XCom)
    # Output: metadata → Dict[coin_id, dict | None]
    # -----------------------------------------------------------------------
    def _extract_metadata(**ctx):
        ti       = ctx["ti"]
        coin_ids = ti.xcom_pull(task_ids="get_coin_universe", key="coin_ids")

        metadata = extract.fetch_all_details(coin_ids)
        ti.xcom_push(key="metadata", value=metadata)

        fetched = sum(1 for v in metadata.values() if v is not None)
        log.info("Metadata fetched: %d/%d coins", fetched, len(coin_ids))
        return f"Fetched metadata: {fetched}/{len(coin_ids)} coins"

    # -----------------------------------------------------------------------
    # Task 3 — validate_metadata
    #
    # Filters out coins where the API call returned None, or where the
    # response is missing required fields (id, name).
    # Pipeline continues with valid coins only — no exception raised.
    #
    # Input:  metadata (XCom)
    # Output: valid_metadata → Dict[coin_id, dict]
    # -----------------------------------------------------------------------
    def _validate_metadata(**ctx):
        ti       = ctx["ti"]
        metadata = ti.xcom_pull(task_ids="extract_metadata", key="metadata")

        valid_metadata, errors = validate.validate_metadata(metadata)
        ti.xcom_push(key="valid_metadata", value=valid_metadata)

        msg = f"Validated — {len(valid_metadata)}/{len(metadata)} ok | errors: {len(errors)}"
        log.info(msg)
        return msg

    # -----------------------------------------------------------------------
    # Task 4 — transform_metadata
    #
    # Converts validated metadata into two sets of typed rows:
    #   dims     → coins_dim rows (coin_id, symbol, name, categories, urls)
    #   dev_rows → coin_dev_metrics rows (github_stars, github_forks, full JSON)
    # Coins without developer_data are excluded from dev_rows.
    #
    # Input:  valid_metadata (XCom), snapshot_ts (XCom)
    # Output: dims → List[dict], dev_rows → List[dict]
    # -----------------------------------------------------------------------
    def _transform_metadata(**ctx):
        ti             = ctx["ti"]
        snapshot_ts    = ti.xcom_pull(task_ids="get_coin_universe", key="snapshot_ts")
        valid_metadata = ti.xcom_pull(task_ids="validate_metadata", key="valid_metadata")

        dims     = transform.build_dims(valid_metadata, snapshot_ts)
        dev_rows = transform.build_dev_metrics(valid_metadata, snapshot_ts)

        ti.xcom_push(key="dims",     value=dims)
        ti.xcom_push(key="dev_rows", value=dev_rows)

        msg = f"Transformed — dims: {len(dims)}, dev_rows: {len(dev_rows)}"
        log.info(msg)
        return msg

    # -----------------------------------------------------------------------
    # Task 5 — load_metadata
    #
    # Writes both tables in one DB connection:
    #   coins_dim        → UPSERT (ON CONFLICT DO UPDATE) — always fresh metadata
    #   coin_dev_metrics → INSERT ON CONFLICT DO NOTHING  — one row per coin per day
    #
    # Input:  dims, dev_rows (XCom)
    # Tables: coins_dim (upsert), coin_dev_metrics (insert)
    # -----------------------------------------------------------------------
    def _load_metadata(**ctx):
        ti       = ctx["ti"]
        dims     = ti.xcom_pull(task_ids="transform_metadata", key="dims")
        dev_rows = ti.xcom_pull(task_ids="transform_metadata", key="dev_rows")

        load.load_all(conn_id="crypto_postgres", dims=dims, dev_rows=dev_rows)

        msg = f"Loaded — dims: {len(dims)}, dev: {len(dev_rows)}"
        log.info(msg)
        return msg

    # -----------------------------------------------------------------------
    # Instantiate operators
    # -----------------------------------------------------------------------
    t_universe  = PythonOperator(task_id="get_coin_universe",  python_callable=_get_coin_universe)
    t_extract   = PythonOperator(task_id="extract_metadata",   python_callable=_extract_metadata)
    t_validate  = PythonOperator(task_id="validate_metadata",  python_callable=_validate_metadata)
    t_transform = PythonOperator(task_id="transform_metadata", python_callable=_transform_metadata)
    t_load      = PythonOperator(task_id="load_metadata",      python_callable=_load_metadata)

    # -----------------------------------------------------------------------
    # Task dependencies — fully linear
    # -----------------------------------------------------------------------
    t_universe >> t_extract >> t_validate >> t_transform >> t_load
