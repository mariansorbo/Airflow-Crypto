"""
DAG: crypto_market_snapshots
Schedule: every 30 minutes
Source: CoinGecko GET /coins/markets (1 API call for all 50 coins)
Target: PostgreSQL — coin_market_snapshots, coin_daily_summary, raw.coin_market_responses

Task graph (linear — single API call, no parallel branches needed):

    get_coin_universe
          │
          ▼
    extract_current_snapshot      ← GET /coins/markets (1 call, 50 coins)
          │
          ▼
    load_raw_data                 ← persist raw JSON to raw.coin_market_responses
          │
          ▼
    extract_from_raw              ← read back from raw layer
          │
          ▼
    validate_raw_data             ← price > 0, market_cap >= 0, id present
          │
          ▼
    transform_and_normalize       ← build snapshot rows
          │
          ▼
    load_to_postgres              ← insert into coin_market_snapshots
          │                          (trigger handles duplicate origin_updated_time)
          ▼
    build_daily_summary           ← UPSERT OHLCV into coin_daily_summary
          │
          ▼
    finalize_run                  ← close orchestration.pipeline_runs record

Note: coins_dim and coin_dev_metrics are updated by crypto_daily_pull_metadata DAG,
not here. Stub dim rows are auto-inserted if a coin has no metadata yet.
"""

import logging
from datetime import datetime, timezone, UTC

from airflow import DAG
from airflow.operators.python import PythonOperator

from crypto.coins import COIN_UNIVERSE
from crypto.market_snapshots import extract, validate, transform, load, load_raw, extract_from_raw
from crypto import pipeline_control

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Failure callback — fires when any task fails
# ---------------------------------------------------------------------------

def _on_failure_callback(context):
    """Marks the run as failed in orchestration.pipeline_runs."""
    run_id    = context.get("run_id", "unknown")
    task_id   = context["task_instance"].task_id
    exception = context.get("exception", "unknown error")
    pipeline_control.fail_run("crypto_postgres", run_id, f"[{task_id}] {exception}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="crypto_market_snapshots",
    description="Captures crypto market snapshots every 30 min — 1 API call for all 50 coins",
    schedule="*/30 * * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "coingecko", "market-data"],
    on_failure_callback=_on_failure_callback,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
    },
) as dag:

    # -----------------------------------------------------------------------
    # Task 1 — get_coin_universe
    #
    # Reads the static list of 50 coin IDs from crypto/coins.py.
    # Captures the real execution timestamp (datetime.now(UTC)) so all
    # downstream tasks use the same consistent snapshot_ts.
    # Registers the run start in orchestration.pipeline_runs.
    #
    # Output (XCom):
    #   coin_ids    → List[str]
    #   snapshot_ts → str (ISO-8601)
    #   run_type    → str ("scheduled" | "manual" | "backfill")
    # -----------------------------------------------------------------------
    def _get_coin_universe(**ctx):
        ti          = ctx["ti"]
        run_id      = ctx["run_id"]
        dag_id      = ctx["dag"].dag_id
        run_type    = ctx["dag_run"].run_type
        snapshot_ts = datetime.now(UTC).isoformat()

        # Check if backfill_slot was passed via DAG conf (from backfill manager)
        backfill_slot = ctx["dag_run"].conf.get("backfill_slot") if ctx["dag_run"].conf else None
        if backfill_slot:
            snapshot_ts = backfill_slot
            run_type    = "backfill"

        ti.xcom_push(key="coin_ids",    value=COIN_UNIVERSE)
        ti.xcom_push(key="snapshot_ts", value=snapshot_ts)
        ti.xcom_push(key="run_type",    value=run_type)

        pipeline_control.start_run(
            conn_id        = "crypto_postgres",
            run_id         = run_id,
            dag_id         = dag_id,
            run_type       = run_type,
            snapshot_ts    = snapshot_ts,
            coins_expected = len(COIN_UNIVERSE),
        )

        log.info("Coin universe: %d coins | snapshot_ts=%s | run_type=%s",
                 len(COIN_UNIVERSE), snapshot_ts, run_type)
        return f"Universe: {len(COIN_UNIVERSE)} coins | ts={snapshot_ts} | type={run_type}"

    # -----------------------------------------------------------------------
    # Task 2 — extract_current_snapshot
    #
    # Single GET /coins/markets call returning price, market cap, volume,
    # supply, rank, and last_updated for all 50 coins.
    #
    # Input:  coin_ids (XCom)
    # Output: market_data → Dict[coin_id, dict]
    # -----------------------------------------------------------------------
    def _extract_current_snapshot(**ctx):
        ti       = ctx["ti"]
        coin_ids = ti.xcom_pull(task_ids="get_coin_universe", key="coin_ids")

        market_data = extract.fetch_market_snapshot(coin_ids)
        ti.xcom_push(key="market_data", value=market_data)

        log.info("Market snapshot fetched for %d coins", len(market_data))
        return f"Fetched market data: {len(market_data)} coins"

    # -----------------------------------------------------------------------
    # Task 3 — load_raw_data
    #
    # Persists the raw /coins/markets payload into raw.coin_market_responses
    # before any validation or transformation occurs.
    # Guarantees the original API response is always stored, even if
    # downstream tasks fail.
    #
    # Input:  market_data (XCom)
    # Output: run_id, snapshot_ts, run_type (XCom)
    # Tables: raw.coin_market_responses (50 rows, 1 per coin)
    # -----------------------------------------------------------------------
    def _load_raw_data(**ctx):
        ti              = ctx["ti"]
        run_id          = ctx["run_id"]
        snapshot_ts_str = ti.xcom_pull(task_ids="get_coin_universe", key="snapshot_ts")
        run_type        = ti.xcom_pull(task_ids="get_coin_universe", key="run_type")
        market_data     = ti.xcom_pull(task_ids="extract_current_snapshot", key="market_data")

        load_raw.save_all_raw(
            conn_id     = "crypto_postgres",
            snapshot_ts = snapshot_ts_str,
            run_id      = run_id,
            run_type    = run_type,
            market_data = market_data,
        )

        ti.xcom_push(key="run_id",      value=run_id)
        ti.xcom_push(key="snapshot_ts", value=snapshot_ts_str)
        ti.xcom_push(key="run_type",    value=run_type)

        msg = f"Raw saved — market={len(market_data)} | run_id={run_id}"
        log.info(msg)
        return msg

    # -----------------------------------------------------------------------
    # Task 4 — extract_from_raw
    #
    # Reads the just-saved batch back from raw.coin_market_responses.
    # From this point forward the pipeline works exclusively with data
    # retrieved from the DB, not from XComs of the extract task.
    # This decoupling allows backfill runs to inject historical data
    # through the same pipeline without changing any downstream logic.
    #
    # Input:  run_id (XCom from load_raw_data)
    # Output: market_data → Dict[coin_id, dict], run_id (XCom)
    # -----------------------------------------------------------------------
    def _extract_from_raw(**ctx):
        ti     = ctx["ti"]
        run_id = ti.xcom_pull(task_ids="load_raw_data", key="run_id")

        resolved_run_id, market_data = extract_from_raw.get_latest_batch(
            conn_id = "crypto_postgres",
            run_id  = run_id,
        )

        ti.xcom_push(key="market_data", value=market_data)
        ti.xcom_push(key="run_id",      value=resolved_run_id)

        msg = f"Retrieved from raw — market={len(market_data)} | run_id={resolved_run_id}"
        log.info(msg)
        return msg

    # -----------------------------------------------------------------------
    # Task 5 — validate_raw_data
    #
    # Applies business rules to the raw market data:
    #   - coin_id present and non-empty
    #   - current_price > 0
    #   - market_cap >= 0
    # Coins that fail are discarded with a WARNING log. The pipeline
    # continues with the valid subset — no exception is raised.
    #
    # Input:  market_data (XCom from extract_from_raw)
    # Output: valid_market → Dict[coin_id, dict], coins_processed → int
    # -----------------------------------------------------------------------
    def _validate_raw_data(**ctx):
        ti          = ctx["ti"]
        market_data = ti.xcom_pull(task_ids="extract_from_raw", key="market_data")

        valid_market, market_errors = validate.validate_snapshot(market_data)

        ti.xcom_push(key="valid_market",    value=valid_market)
        ti.xcom_push(key="coins_processed", value=len(valid_market))

        msg = (
            f"Validated — market: {len(valid_market)}/{len(market_data)} ok, "
            f"errors: {len(market_errors)}"
        )
        log.info(msg)
        return msg

    # -----------------------------------------------------------------------
    # Task 6 — transform_and_normalize
    #
    # Converts validated raw dicts into typed rows for coin_market_snapshots.
    # Each row includes: snapshot_ts, coin_id, price, market_cap, volume,
    # supply fields, rank, run_type, and origin_updated_time (CoinGecko's
    # last_updated timestamp, used by the dedup trigger).
    #
    # Input:  valid_market (XCom), snapshot_ts, run_type
    # Output: snapshots → List[dict]
    # -----------------------------------------------------------------------
    def _transform_and_normalize(**ctx):
        ti          = ctx["ti"]
        snapshot_ts = ti.xcom_pull(task_ids="get_coin_universe", key="snapshot_ts")
        run_type    = ti.xcom_pull(task_ids="get_coin_universe", key="run_type")
        valid_market = ti.xcom_pull(task_ids="validate_raw_data", key="valid_market")

        snapshots = transform.build_snapshots(valid_market, snapshot_ts, run_type)

        ti.xcom_push(key="snapshots", value=snapshots)

        msg = f"Transformed — snapshots: {len(snapshots)}"
        log.info(msg)
        return msg

    # -----------------------------------------------------------------------
    # Task 7 — load_to_postgres
    #
    # Writes snapshot rows into coin_market_snapshots.
    # Before inserting snapshots, auto-inserts minimal stub rows in coins_dim
    # for any coin that doesn't have metadata yet (avoids FK violations until
    # the daily pull DAG runs for the first time).
    #
    # The trigger trg_check_origin_updated_time intercepts each row:
    #   - If origin_updated_time > last recorded → insert proceeds normally
    #   - If not newer → insert is cancelled, row goes to
    #     coin_market_snapshots_not_updated instead
    #
    # Input:  snapshots (XCom)
    # Output: clean_rows_inserted, raw_rows_inserted (XCom for finalize_run)
    # -----------------------------------------------------------------------
    def _load_to_postgres(**ctx):
        ti        = ctx["ti"]
        snapshots = ti.xcom_pull(task_ids="transform_and_normalize", key="snapshots")

        load.load_all(conn_id="crypto_postgres", snapshots=snapshots)

        ti.xcom_push(key="clean_rows_inserted", value=len(snapshots))
        ti.xcom_push(key="raw_rows_inserted",   value=len(snapshots))

        msg = f"Loaded — snapshots: {len(snapshots)}"
        log.info(msg)
        return msg

    # -----------------------------------------------------------------------
    # Task 8 — build_daily_summary
    #
    # Runs a CTE over coin_market_snapshots to compute OHLCV aggregates
    # for today and upserts them into coin_daily_summary. Idempotent:
    # can be re-run as many times as needed throughout the day.
    #
    # Input:  snapshot_ts (XCom — determines which date to aggregate)
    # Tables: coin_daily_summary (UPSERT)
    # -----------------------------------------------------------------------
    def _build_daily_summary(**ctx):
        ti          = ctx["ti"]
        snapshot_ts = ti.xcom_pull(task_ids="get_coin_universe", key="snapshot_ts")
        load.build_daily_summary(conn_id="crypto_postgres", snapshot_ts=snapshot_ts)
        log.info("Daily summary built for %s", snapshot_ts)
        return f"Daily summary updated for {snapshot_ts}"

    # -----------------------------------------------------------------------
    # Task 9 — finalize_run
    #
    # Closes the orchestration.pipeline_runs record with final counts and
    # status: 'success' if all 50 coins processed, 'partial' if fewer.
    #
    # Input:  coins_processed, clean_rows_inserted, raw_rows_inserted (XCom)
    # Tables: orchestration.pipeline_runs (UPDATE)
    # -----------------------------------------------------------------------
    def _finalize_run(**ctx):
        ti                  = ctx["ti"]
        run_id              = ctx["run_id"]
        coins_processed     = ti.xcom_pull(task_ids="validate_raw_data",      key="coins_processed")
        clean_rows_inserted = ti.xcom_pull(task_ids="load_to_postgres",       key="clean_rows_inserted")
        raw_rows_inserted   = ti.xcom_pull(task_ids="load_to_postgres",       key="raw_rows_inserted")

        pipeline_control.finish_run(
            conn_id             = "crypto_postgres",
            run_id              = run_id,
            coins_processed     = coins_processed     or 0,
            coins_expected      = len(COIN_UNIVERSE),
            raw_rows_inserted   = raw_rows_inserted   or 0,
            clean_rows_inserted = clean_rows_inserted or 0,
        )
        log.info("Run finalized — run_id=%s coins=%s", run_id, coins_processed)
        return f"Run {run_id} finalized"

    # -----------------------------------------------------------------------
    # Instantiate operators
    # -----------------------------------------------------------------------
    t_universe    = PythonOperator(task_id="get_coin_universe",        python_callable=_get_coin_universe)
    t_snapshot    = PythonOperator(task_id="extract_current_snapshot", python_callable=_extract_current_snapshot)
    t_load_raw    = PythonOperator(task_id="load_raw_data",            python_callable=_load_raw_data)
    t_extract_raw = PythonOperator(task_id="extract_from_raw",         python_callable=_extract_from_raw)
    t_validate    = PythonOperator(task_id="validate_raw_data",        python_callable=_validate_raw_data)
    t_transform   = PythonOperator(task_id="transform_and_normalize",  python_callable=_transform_and_normalize)
    t_load        = PythonOperator(task_id="load_to_postgres",         python_callable=_load_to_postgres)
    t_summary     = PythonOperator(task_id="build_daily_summary",      python_callable=_build_daily_summary)
    t_finalize    = PythonOperator(task_id="finalize_run",             python_callable=_finalize_run)

    # -----------------------------------------------------------------------
    # Task dependencies — fully linear (single API call, no parallel branches)
    # -----------------------------------------------------------------------
    t_universe >> t_snapshot >> t_load_raw >> t_extract_raw >> t_validate >> t_transform >> t_load >> t_summary >> t_finalize
