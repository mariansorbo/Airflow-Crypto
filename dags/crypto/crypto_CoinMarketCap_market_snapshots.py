"""
DAG: crypto_CoinMarketCap_market_snapshots
Schedule: None — self-triggering (each run schedules the next one)
Source: CoinMarketCap GET /v1/cryptocurrency/quotes/latest (1 API call for all 50 coins)
Target: PostgreSQL — coin_market_snapshots, coin_daily_summary, raw.coin_market_responses

Fallback pipeline for crypto_market_snapshots. Shares all tasks except extraction:
the CoinMarketCap API replaces CoinGecko as the data source, but the response is
normalized to the same field names so validate / transform / load run unchanged.

Task graph (identical structure to crypto_market_snapshots):

    get_coin_universe
          │
          ▼
    extract_current_snapshot      ← GET /v1/cryptocurrency/quotes/latest (1 call, 50 coins)
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
          │
          ▼
    build_daily_summary           ← UPSERT OHLCV into coin_daily_summary
          │
          ▼
    finalize_run                  ← close orchestration.pipeline_runs record
          │
          ▼
    should_self_trigger           ← ShortCircuit: skips next task if backfill run
          │
          ▼
    trigger_next_run              ← TriggerDagRunOperator → fires a new run of this DAG

Environment variable required:
    CMC_API_KEY — CoinMarketCap API key
"""

import logging
from datetime import datetime, timezone, UTC

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from crypto.coins import COIN_UNIVERSE
from crypto.market_snapshots import validate, transform, load, load_raw, extract_from_raw
from crypto.market_snapshots import CoinMarketCap_extract as cmc_extract
from crypto import pipeline_control

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Failure callback
# ---------------------------------------------------------------------------

def _on_failure_callback(context):
    run_id    = context.get("run_id", "unknown")
    task_id   = context["task_instance"].task_id
    exception = context.get("exception", "unknown error")
    pipeline_control.fail_run("crypto_postgres", run_id, f"[{task_id}] {exception}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="crypto_CoinMarketCap_market_snapshots",
    description="Fallback market snapshots via CoinMarketCap — self-triggering, 1 API call for all 50 coins",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "coinmarketcap", "market-data", "fallback"],
    on_failure_callback=_on_failure_callback,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
    },
) as dag:

    # -----------------------------------------------------------------------
    # Task 1 — get_coin_universe
    #
    # Reads the static list from crypto/coins.py.
    # Pushes coin_pairs (full tuples) for the CMC extract task, and
    # coin_ids (IDs only) for coins_expected count.
    #
    # Output (XCom):
    #   coin_pairs  → List[Tuple[str, str]]  — (coingecko_id, symbol)
    #   coin_ids    → List[str]              — coingecko_ids only
    #   snapshot_ts → str (ISO-8601)
    #   run_type    → str ("scheduled" | "manual" | "backfill")
    # -----------------------------------------------------------------------
    def _get_coin_universe(**ctx):
        ti          = ctx["ti"]
        run_id      = ctx["run_id"]
        dag_id      = ctx["dag"].dag_id
        run_type    = ctx["dag_run"].run_type
        snapshot_ts = datetime.now(UTC).isoformat()

        backfill_slot = ctx["dag_run"].conf.get("backfill_slot") if ctx["dag_run"].conf else None
        if backfill_slot:
            snapshot_ts = backfill_slot
            run_type    = "backfill"

        coin_ids = [cgid for cgid, _ in COIN_UNIVERSE]

        ti.xcom_push(key="coin_pairs",  value=COIN_UNIVERSE)
        ti.xcom_push(key="coin_ids",    value=coin_ids)
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
    # Single GET /v1/cryptocurrency/quotes/latest call via CoinMarketCap.
    # Response is normalized to CoinGecko-compatible field names before
    # being pushed to XCom so all downstream tasks run unchanged.
    #
    # Input:  coin_pairs (XCom)
    # Output: market_data → Dict[coingecko_id, normalized_dict]
    # -----------------------------------------------------------------------
    def _extract_current_snapshot(**ctx):
        ti         = ctx["ti"]
        coin_pairs = ti.xcom_pull(task_ids="get_coin_universe", key="coin_pairs")

        market_data = cmc_extract.fetch_market_snapshot(coin_pairs)
        ti.xcom_push(key="market_data", value=market_data)

        log.info("CMC market snapshot fetched for %d coins", len(market_data))
        return f"Fetched market data: {len(market_data)} coins"

    # -----------------------------------------------------------------------
    # Task 3 — load_raw_data
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
    # -----------------------------------------------------------------------
    def _transform_and_normalize(**ctx):
        ti           = ctx["ti"]
        snapshot_ts  = ti.xcom_pull(task_ids="get_coin_universe", key="snapshot_ts")
        run_type     = ti.xcom_pull(task_ids="get_coin_universe", key="run_type")
        valid_market = ti.xcom_pull(task_ids="validate_raw_data", key="valid_market")

        snapshots = transform.build_snapshots(valid_market, snapshot_ts, run_type)

        ti.xcom_push(key="snapshots", value=snapshots)

        msg = f"Transformed — snapshots: {len(snapshots)}"
        log.info(msg)
        return msg

    # -----------------------------------------------------------------------
    # Task 7 — load_to_postgres
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
    # -----------------------------------------------------------------------
    def _build_daily_summary(**ctx):
        ti          = ctx["ti"]
        snapshot_ts = ti.xcom_pull(task_ids="get_coin_universe", key="snapshot_ts")
        load.build_daily_summary(conn_id="crypto_postgres", snapshot_ts=snapshot_ts)
        log.info("Daily summary built for %s", snapshot_ts)
        return f"Daily summary updated for {snapshot_ts}"

    # -----------------------------------------------------------------------
    # Task 9 — finalize_run
    # -----------------------------------------------------------------------
    def _finalize_run(**ctx):
        ti                  = ctx["ti"]
        run_id              = ctx["run_id"]
        coins_processed     = ti.xcom_pull(task_ids="validate_raw_data", key="coins_processed")
        clean_rows_inserted = ti.xcom_pull(task_ids="load_to_postgres",  key="clean_rows_inserted")
        raw_rows_inserted   = ti.xcom_pull(task_ids="load_to_postgres",  key="raw_rows_inserted")

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
    # Task 10 — should_self_trigger
    # -----------------------------------------------------------------------
    def _should_self_trigger(**ctx):
        run_type = ctx["ti"].xcom_pull(task_ids="get_coin_universe", key="run_type")
        if run_type == "backfill":
            log.info("Backfill run — skipping self-trigger")
            return False
        return True

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

    t_should_trigger = ShortCircuitOperator(
        task_id="should_self_trigger",
        python_callable=_should_self_trigger,
    )
    t_trigger = TriggerDagRunOperator(
        task_id="trigger_next_run",
        trigger_dag_id="crypto_CoinMarketCap_market_snapshots",
        wait_for_completion=False,
    )

    (
        t_universe
        >> t_snapshot
        >> t_load_raw
        >> t_extract_raw
        >> t_validate
        >> t_transform
        >> t_load
        >> t_summary
        >> t_finalize
        >> t_should_trigger
        >> t_trigger
    )
