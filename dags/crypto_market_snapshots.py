"""
DAG: crypto_market_snapshots
Schedule: every 30 minutes
Source: CoinGecko public API
Target: PostgreSQL (crypto_data database)

Task graph:

    get_coin_universe
         ├──────────────────────────────┐
         ▼                              ▼
  extract_current_snapshot      extract_coin_metadata
         │                              │
         │                              ▼
         │                    extract_dev_metrics
         │                              │
         └──────────────┬───────────────┘
                        ▼
                 load_raw_data          ← persists raw JSON to raw.* tables
                        │
                        ▼
               extract_from_raw         ← reads back from raw.* tables
                        │
                        ▼
                validate_raw_data
                        │
                        ▼
             transform_and_normalize
                        │
                        ▼
                 load_to_postgres
                        │
                        ▼
               build_daily_summary
"""

import logging
from datetime import datetime, timezone, UTC

from airflow import DAG
from airflow.operators.python import PythonOperator

from crypto.coins import COIN_UNIVERSE
from crypto import extract, validate, transform, load, load_raw, extract_from_raw, pipeline_control

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Failure callback — fires when any task fails
# ---------------------------------------------------------------------------

def _on_failure_callback(context):
    """
    Automatically marks the run as failed in orchestration.pipeline_runs.
    Captures the exception message and the task that caused the failure.
    """
    run_id    = context.get("run_id", "unknown")
    task_id   = context["task_instance"].task_id
    exception = context.get("exception", "unknown error")
    error_msg = f"[{task_id}] {exception}"
    pipeline_control.fail_run("crypto_postgres", run_id, error_msg)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="crypto_market_snapshots",
    description="Captures crypto market snapshots every 30 min from CoinGecko",
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
    # Qué hace:
    #   Devuelve la lista estática de 50 coin IDs definida en crypto/coins.py
    #   y la guarda en XCom para que las tareas de extracción la consuman.
    #
    # Input:  ninguno (lista hardcodeada en el código)
    # Output (XCom):
    #   key="coin_ids" → List[str]
    #   Ejemplo: ["bitcoin", "ethereum", "solana", ...]
    # -----------------------------------------------------------------------
    def _get_coin_universe(**ctx):
        ti = ctx["ti"]
        ti.xcom_push(key="coin_ids", value=COIN_UNIVERSE)

        # Capturamos el timestamp real de ejecución acá, una sola vez,
        # para que todas las tasks usen el mismo valor consistente.
        # Usamos now() en lugar de data_interval_end para evitar que
        # Airflow redondee al límite del intervalo programado.
        snapshot_ts = datetime.now(UTC).isoformat()
        run_type    = ctx["dag_run"].run_type   # "scheduled" | "manual" | "backfill"
        run_id      = ctx["run_id"]
        dag_id      = ctx["dag"].dag_id
        ti.xcom_push(key="snapshot_ts", value=snapshot_ts)
        ti.xcom_push(key="run_type",    value=run_type)

        # Registrar el inicio de la corrida en la tabla de control
        pipeline_control.start_run(
            conn_id        = "crypto_postgres",
            run_id         = run_id,
            dag_id         = dag_id,
            run_type       = run_type,
            snapshot_ts    = snapshot_ts,
            coins_expected = len(COIN_UNIVERSE),
        )

        log.info("Coin universe loaded: %d coins | snapshot_ts=%s | run_type=%s",
                 len(COIN_UNIVERSE), snapshot_ts, run_type)
        return f"Universe: {len(COIN_UNIVERSE)} coins | ts={snapshot_ts} | type={run_type}"

    # -----------------------------------------------------------------------
    # Task 2 — extract_current_snapshot
    #
    # Qué hace:
    #   Llama a GET /coins/markets una sola vez con los 50 IDs.
    #   CoinGecko devuelve precio, market cap, volumen, supply, rank, etc.
    #   para todos en una sola respuesta. Convierte la lista en un dict
    #   indexado por coin_id para facilitar el acceso posterior.
    #
    # Input (XCom):
    #   ← get_coin_universe / coin_ids → List[str]
    #
    # Output (XCom):
    #   key="market_data" → Dict[str, dict]
    #   Ejemplo:
    #   {
    #     "bitcoin": {
    #       "id": "bitcoin",
    #       "current_price": 68000.0,
    #       "market_cap": 1_340_000_000_000,
    #       "total_volume": 38_000_000_000,
    #       "circulating_supply": 19_600_000.0,
    #       "market_cap_rank": 1,
    #       ...
    #     },
    #     "ethereum": { ... },
    #     ...
    #   }
    # -----------------------------------------------------------------------
    def _extract_current_snapshot(**ctx):
        ti = ctx["ti"]
        coin_ids = ti.xcom_pull(task_ids="get_coin_universe", key="coin_ids")

        market_data = extract.fetch_market_snapshot(coin_ids)
        ti.xcom_push(key="market_data", value=market_data)
        log.info("Market snapshot fetched for %d coins", len(market_data))
        return f"Fetched market data: {len(market_data)} coins"

    # -----------------------------------------------------------------------
    # Task 3 — extract_coin_metadata
    #
    # Qué hace:
    #   Llama a GET /coins/{id} una vez por cada moneda (50 llamadas en total)
    #   con un delay de ~2 segundos entre cada una para respetar el rate limit
    #   de CoinGecko. Trae información estática/semi-estática de cada moneda:
    #   categorías, links, sitio web, repositorio de GitHub, etc.
    #   También incluye el bloque "developer_data" que se reutiliza en Task 4.
    #   Si una llamada falla (timeout, rate limit), guarda None para esa moneda.
    #
    # Input (XCom):
    #   ← get_coin_universe / coin_ids → List[str]
    #
    # Output (XCom):
    #   key="metadata" → Dict[str, dict | None]
    #   Ejemplo:
    #   {
    #     "bitcoin": {
    #       "id": "bitcoin",
    #       "symbol": "btc",
    #       "name": "Bitcoin",
    #       "categories": ["Cryptocurrency", "Layer 1"],
    #       "links": {
    #         "homepage": ["https://bitcoin.org"],
    #         "repos_url": { "github": ["https://github.com/bitcoin/bitcoin"] }
    #       },
    #       "developer_data": {
    #         "forks": 36000,
    #         "stars": 78000,
    #         "commit_count_4_weeks": 45,
    #         ...
    #       },
    #       ...
    #     },
    #     "solana": None,   ← si la llamada falló
    #     ...
    #   }
    # -----------------------------------------------------------------------
    def _extract_coin_metadata(**ctx):
        ti = ctx["ti"]
        coin_ids = ti.xcom_pull(task_ids="get_coin_universe", key="coin_ids")

        metadata = extract.fetch_all_details(coin_ids)
        ti.xcom_push(key="metadata", value=metadata)

        fetched = sum(1 for v in metadata.values() if v is not None)
        log.info("Metadata fetched: %d/%d coins", fetched, len(coin_ids))
        return f"Fetched metadata: {fetched}/{len(coin_ids)} coins"

    # -----------------------------------------------------------------------
    # Task 4 — extract_dev_metrics
    #
    # Qué hace:
    #   No hace llamadas a la API. Toma el XCom de extract_coin_metadata y
    #   extrae únicamente el bloque "developer_data" de cada moneda.
    #   Separa esta información en su propio XCom para que el pipeline
    #   trate las métricas de desarrollo como una capa independiente.
    #   Las monedas con metadata None son omitidas silenciosamente.
    #
    # Input (XCom):
    #   ← extract_coin_metadata / metadata → Dict[str, dict | None]
    #
    # Output (XCom):
    #   key="dev_metrics" → Dict[str, dict]
    #   Ejemplo:
    #   {
    #     "bitcoin": {
    #       "forks": 36000,
    #       "stars": 78000,
    #       "subscribers": 4200,
    #       "total_issues": 8100,
    #       "closed_issues": 7900,
    #       "pull_requests_merged": 12400,
    #       "commit_count_4_weeks": 45,
    #       "code_additions_deletions_4_weeks": { "additions": 2100, "deletions": -900 }
    #     },
    #     "ethereum": { ... },
    #     ...
    #   }
    # -----------------------------------------------------------------------
    def _extract_dev_metrics(**ctx):
        ti = ctx["ti"]
        metadata = ti.xcom_pull(task_ids="extract_coin_metadata", key="metadata")

        dev_metrics = {
            coin_id: (detail.get("developer_data") or {})
            for coin_id, detail in metadata.items()
            if detail is not None
        }
        ti.xcom_push(key="dev_metrics", value=dev_metrics)
        log.info("Dev metrics extracted for %d coins", len(dev_metrics))
        return f"Dev metrics extracted: {len(dev_metrics)} coins"

    # -----------------------------------------------------------------------
    # Task 5 — load_raw_data
    #
    # Qué hace:
    #   Persiste los tres payloads crudos en el schema raw de PostgreSQL
    #   antes de que se aplique cualquier transformación o validación.
    #   Esto garantiza que siempre tengamos el JSON original de la API
    #   guardado, independientemente de si la validación o transformación
    #   fallan en pasos posteriores.
    #   Cada fila se identifica por (run_id, coin_id) — único por ejecución.
    #
    # Input (XCom):
    #   ← extract_current_snapshot / market_data  → Dict[str, dict]
    #   ← extract_coin_metadata    / metadata     → Dict[str, dict | None]
    #   ← extract_dev_metrics      / dev_metrics  → Dict[str, dict]
    #
    # Output (XCom):
    #   key="run_id"       → str  (el run_id de Airflow, ej: "scheduled__2026-03-24T21:30:00+00:00")
    #   key="snapshot_ts"  → str  (ISO timestamp del data_interval_end)
    #
    # Tablas escritas:
    #   raw.coin_market_responses   (50 filas)
    #   raw.coin_metadata_responses (hasta 50 filas, menos si hubo errores de API)
    #   raw.coin_dev_responses      (hasta 50 filas)
    # -----------------------------------------------------------------------
    def _load_raw_data(**ctx):
        ti          = ctx["ti"]
        run_id      = ctx["run_id"]
        snapshot_ts_str = ti.xcom_pull(task_ids="get_coin_universe", key="snapshot_ts")
        run_type        = ti.xcom_pull(task_ids="get_coin_universe", key="run_type")

        market_data = ti.xcom_pull(task_ids="extract_current_snapshot", key="market_data")
        metadata    = ti.xcom_pull(task_ids="extract_coin_metadata",    key="metadata")
        dev_metrics = ti.xcom_pull(task_ids="extract_dev_metrics",      key="dev_metrics")

        load_raw.save_all_raw(
            conn_id="crypto_postgres",
            snapshot_ts=snapshot_ts_str,
            run_id=run_id,
            run_type=run_type,
            market_data=market_data,
            metadata=metadata,
            dev_metrics=dev_metrics,
        )

        ti.xcom_push(key="run_id",      value=run_id)
        ti.xcom_push(key="snapshot_ts", value=snapshot_ts_str)
        ti.xcom_push(key="run_type",    value=run_type)

        msg = (
            f"Raw saved — market={len(market_data)}, "
            f"metadata={sum(1 for v in metadata.values() if v)}, "
            f"dev={len(dev_metrics)} | run_id={run_id}"
        )
        log.info(msg)
        return msg

    # -----------------------------------------------------------------------
    # Task 6 — extract_from_raw
    #
    # Qué hace:
    #   Lee de vuelta el batch más reciente desde las tablas raw.* usando el
    #   run_id guardado en el paso anterior. Reconstruye los tres dicts de
    #   payloads con exactamente la misma estructura que producían las tasks
    #   de extracción originales. A partir de este punto, el pipeline trabaja
    #   exclusivamente con datos leídos desde la base de datos — no desde
    #   XComs de las tasks de extracción.
    #
    # Input (XCom):
    #   ← load_raw_data / run_id → str
    #
    # Output (XCom):
    #   key="market_data"  → Dict[str, dict]       (misma estructura que extract_current_snapshot)
    #   key="metadata"     → Dict[str, dict | None] (misma estructura que extract_coin_metadata)
    #   key="dev_metrics"  → Dict[str, dict]        (misma estructura que extract_dev_metrics)
    #   key="run_id"       → str
    # -----------------------------------------------------------------------
    def _extract_from_raw(**ctx):
        ti     = ctx["ti"]
        run_id = ti.xcom_pull(task_ids="load_raw_data", key="run_id")

        resolved_run_id, market_data, metadata, dev_metrics = extract_from_raw.get_latest_batch(
            conn_id="crypto_postgres",
            run_id=run_id,
        )

        ti.xcom_push(key="market_data", value=market_data)
        ti.xcom_push(key="metadata",    value=metadata)
        ti.xcom_push(key="dev_metrics", value=dev_metrics)
        ti.xcom_push(key="run_id",      value=resolved_run_id)

        msg = (
            f"Retrieved from raw — market={len(market_data)}, "
            f"metadata={len(metadata)}, dev={len(dev_metrics)} | run_id={resolved_run_id}"
        )
        log.info(msg)
        return msg

    # -----------------------------------------------------------------------
    # Task 7 — validate_raw_data
    #
    # Qué hace:
    #   Aplica validaciones básicas sobre los datos crudos de mercado y metadata.
    #   Las monedas que no pasan la validación se descartan y se loguean como
    #   warnings. No lanza excepciones: el pipeline continúa con las monedas
    #   válidas.
    #
    #   Validaciones sobre market_data:
    #     - coin_id no es nulo
    #     - price_usd > 0
    #     - market_cap >= 0
    #
    #   Validaciones sobre metadata:
    #     - respuesta no es None (la llamada a la API no falló)
    #     - campo "id" presente
    #     - campo "name" presente
    #
    # Input (XCom):
    #   ← extract_current_snapshot / market_data → Dict[str, dict]
    #   ← extract_coin_metadata    / metadata    → Dict[str, dict | None]
    #
    # Output (XCom):
    #   key="valid_market"   → Dict[str, dict]  (solo monedas que pasaron)
    #   key="valid_metadata" → Dict[str, dict]  (solo monedas que pasaron)
    # -----------------------------------------------------------------------
    def _validate_raw_data(**ctx):
        ti = ctx["ti"]
        # Reads from extract_from_raw (raw layer) — NOT from the original extract tasks
        market_data = ti.xcom_pull(task_ids="extract_from_raw", key="market_data")
        metadata    = ti.xcom_pull(task_ids="extract_from_raw", key="metadata")

        valid_market, market_errors = validate.validate_snapshot(market_data)
        valid_metadata, meta_errors = validate.validate_metadata(metadata)

        ti.xcom_push(key="valid_market",    value=valid_market)
        ti.xcom_push(key="valid_metadata",  value=valid_metadata)
        ti.xcom_push(key="coins_processed", value=len(valid_market))

        total_errors = len(market_errors) + len(meta_errors)
        msg = (
            f"Validated — market: {len(valid_market)}/{len(market_data)} ok, "
            f"metadata: {len(valid_metadata)}/{len(metadata)} ok, "
            f"errors: {total_errors}"
        )
        log.info(msg)
        return msg

    # -----------------------------------------------------------------------
    # Task 6 — transform_and_normalize
    #
    # Qué hace:
    #   Convierte los dicts crudos validados en listas de dicts tipados,
    #   listos para inserción masiva en PostgreSQL. También construye las
    #   filas de raw_responses guardando el JSON original de la API.
    #   Todas las fechas se normalizan al timestamp canónico del run
    #   (data_interval_end) para garantizar consistencia entre tablas.
    #
    #   Produce 4 colecciones:
    #
    #   snapshots  → coin_market_snapshots
    #     { snapshot_ts, coin_id, price_usd, market_cap_usd,
    #       volume_24h_usd, circulating_supply, total_supply,
    #       max_supply, market_cap_rank }
    #
    #   dims       → coins_dim  (upsert)
    #     { coin_id, symbol, name, categories, website_url,
    #       github_url, updated_at }
    #
    #   dev_rows   → coin_dev_metrics
    #     { snapshot_ts, coin_id, github_stars, github_forks,
    #       dev_metric_raw (JSON completo), fetched_at }
    #
    #   raws       → coin_raw_responses
    #     { snapshot_ts, coin_id, source_endpoint, raw_payload (JSON), inserted_at }
    #     (una fila por moneda para /coins/markets + una por /coins/{id})
    #
    # Input (XCom):
    #   ← validate_raw_data        / valid_market   → Dict[str, dict]
    #   ← validate_raw_data        / valid_metadata → Dict[str, dict]
    #   ← extract_dev_metrics      / dev_metrics    → Dict[str, dict]
    #   ← extract_current_snapshot / market_data    → Dict[str, dict]  (para raw)
    #   ← extract_coin_metadata    / metadata       → Dict[str, dict]  (para raw)
    #
    # Output (XCom):
    #   key="snapshots" → List[dict]
    #   key="dims"      → List[dict]
    #   key="dev_rows"  → List[dict]
    #   key="raws"      → List[dict]
    # -----------------------------------------------------------------------
    def _transform_and_normalize(**ctx):
        ti          = ctx["ti"]
        snapshot_ts = ti.xcom_pull(task_ids="get_coin_universe", key="snapshot_ts")
        run_type    = ti.xcom_pull(task_ids="get_coin_universe", key="run_type")

        valid_market   = ti.xcom_pull(task_ids="validate_raw_data", key="valid_market")
        valid_metadata = ti.xcom_pull(task_ids="validate_raw_data", key="valid_metadata")
        # dev_metrics y raw payloads vienen de la capa raw, no de los extract tasks originales
        dev_metrics = ti.xcom_pull(task_ids="extract_from_raw", key="dev_metrics")
        market_raw  = ti.xcom_pull(task_ids="extract_from_raw", key="market_data")
        meta_raw    = ti.xcom_pull(task_ids="extract_from_raw", key="metadata")

        snapshots = transform.build_snapshots(valid_market, snapshot_ts, run_type)
        dims      = transform.build_dims(valid_metadata, snapshot_ts)
        dev_rows  = transform.build_dev_metrics(dev_metrics, snapshot_ts, run_type)
        raws      = transform.build_raw_responses(market_raw, meta_raw, snapshot_ts)

        ti.xcom_push(key="snapshots", value=snapshots)
        ti.xcom_push(key="dims",      value=dims)
        ti.xcom_push(key="dev_rows",  value=dev_rows)
        ti.xcom_push(key="raws",      value=raws)

        msg = (
            f"Transformed — snapshots: {len(snapshots)}, dims: {len(dims)}, "
            f"dev: {len(dev_rows)}, raw: {len(raws)}"
        )
        log.info(msg)
        return msg

    # -----------------------------------------------------------------------
    # Task 7 — load_to_postgres
    #
    # Qué hace:
    #   Carga las 4 colecciones transformadas en PostgreSQL usando una sola
    #   conexión y bulk inserts (execute_values de psycopg2).
    #   El orden de carga es importante por las dependencias:
    #     1. coins_dim       → UPSERT (ON CONFLICT DO UPDATE) — tabla padre
    #     2. coin_market_snapshots → INSERT ON CONFLICT DO NOTHING
    #     3. coin_dev_metrics      → INSERT ON CONFLICT DO NOTHING
    #     4. coin_raw_responses    → INSERT ON CONFLICT DO NOTHING
    #   Si coins_dim falla parcialmente, se insertan stubs mínimos para las
    #   monedas faltantes (coin_id + nombre placeholder) para evitar errores
    #   de FK en la tabla de snapshots.
    #
    # Input (XCom):
    #   ← transform_and_normalize / snapshots → List[dict]
    #   ← transform_and_normalize / dims      → List[dict]
    #   ← transform_and_normalize / dev_rows  → List[dict]
    #   ← transform_and_normalize / raws      → List[dict]
    #
    # Output: ninguno (escribe directamente en PostgreSQL)
    # -----------------------------------------------------------------------
    def _load_to_postgres(**ctx):
        ti = ctx["ti"]

        snapshots = ti.xcom_pull(task_ids="transform_and_normalize", key="snapshots")
        dims      = ti.xcom_pull(task_ids="transform_and_normalize", key="dims")
        dev_rows  = ti.xcom_pull(task_ids="transform_and_normalize", key="dev_rows")
        raws      = ti.xcom_pull(task_ids="transform_and_normalize", key="raws")

        load.load_all(
            conn_id="crypto_postgres",
            snapshots=snapshots,
            dims=dims,
            dev_rows=dev_rows,
            raws=raws,
        )

        # Exponer contadores para la task de control al final del pipeline
        raw_total = len(snapshots) + len(dims) + len(dev_rows) + len(raws)
        ti.xcom_push(key="clean_rows_inserted", value=len(snapshots))
        ti.xcom_push(key="raw_rows_inserted",   value=raw_total)

        msg = (
            f"Loaded — dims: {len(dims)}, snapshots: {len(snapshots)}, "
            f"dev: {len(dev_rows)}, raw: {len(raws)}"
        )
        log.info(msg)
        return msg

    # -----------------------------------------------------------------------
    # Task 8 — build_daily_summary
    #
    # Qué hace:
    #   Ejecuta un único SQL con CTE sobre coin_market_snapshots para calcular
    #   las métricas agregadas del día actual y hace un UPSERT en
    #   coin_daily_summary. Es idempotente: se puede ejecutar N veces por día
    #   y siempre actualiza los valores con los datos más recientes.
    #
    #   Campos calculados:
    #     open_price_usd    → precio del primer snapshot del día
    #     high_price_usd    → precio máximo del día
    #     low_price_usd     → precio mínimo del día
    #     close_price_usd   → precio del último snapshot del día
    #     avg_volume_24h    → promedio del campo volume_24h de todos los snapshots
    #     avg_market_cap    → promedio del market cap de todos los snapshots
    #     snapshot_count    → cantidad de snapshots capturados en el día
    #
    #   En caso de re-ejecución (ON CONFLICT DO UPDATE):
    #     - high se actualiza si el nuevo es mayor
    #     - low se actualiza si el nuevo es menor
    #     - close siempre se pisa con el valor más reciente
    #
    # Input:  ctx["data_interval_end"] — determina qué fecha calcular
    # Output: ninguno (escribe en coin_daily_summary)
    # -----------------------------------------------------------------------
    def _build_daily_summary(**ctx):
        ti          = ctx["ti"]
        snapshot_ts = ti.xcom_pull(task_ids="get_coin_universe", key="snapshot_ts")
        load.build_daily_summary(conn_id="crypto_postgres", snapshot_ts=snapshot_ts)
        log.info("Daily summary built for %s", snapshot_ts)
        return f"Daily summary updated for {snapshot_ts}"

    # -----------------------------------------------------------------------
    # 11. finalize_run
    # -----------------------------------------------------------------------
    #   Cierra el registro de la corrida en orchestration.pipeline_runs.
    #
    #   Lee desde XCom:
    #     - validate_raw_data   → coins_processed  (int)
    #     - load_to_postgres    → clean_rows_inserted, raw_rows_inserted (int)
    #
    #   Llama a pipeline_control.finish_run() que actualiza la fila con:
    #     - status          → 'success' si coins_processed == 50, 'partial' si menos
    #     - finished_at     → NOW()
    #     - coins_processed → cantidad de monedas que pasaron validación
    #     - raw_rows_inserted   → filas insertadas en las tablas raw.*
    #     - clean_rows_inserted → filas insertadas en las tablas públicas
    #
    # Input:  XComs de validate_raw_data y load_to_postgres
    # Output: string de confirmación (guardado en XCom por defecto de Airflow)
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
        log.info("Run finalized — run_id=%s coins=%s clean=%s raw=%s",
                 run_id, coins_processed, clean_rows_inserted, raw_rows_inserted)
        return f"Run {run_id} finalized"

    # -----------------------------------------------------------------------
    # Instantiate operators
    # -----------------------------------------------------------------------
    t_universe       = PythonOperator(task_id="get_coin_universe",        python_callable=_get_coin_universe)
    t_snapshot       = PythonOperator(task_id="extract_current_snapshot", python_callable=_extract_current_snapshot)
    t_metadata       = PythonOperator(task_id="extract_coin_metadata",    python_callable=_extract_coin_metadata)
    t_devmet         = PythonOperator(task_id="extract_dev_metrics",      python_callable=_extract_dev_metrics)
    t_load_raw       = PythonOperator(task_id="load_raw_data",            python_callable=_load_raw_data)
    t_extract_raw    = PythonOperator(task_id="extract_from_raw",         python_callable=_extract_from_raw)
    t_validate       = PythonOperator(task_id="validate_raw_data",        python_callable=_validate_raw_data)
    t_transform      = PythonOperator(task_id="transform_and_normalize",  python_callable=_transform_and_normalize)
    t_load           = PythonOperator(task_id="load_to_postgres",         python_callable=_load_to_postgres)
    t_summary        = PythonOperator(task_id="build_daily_summary",      python_callable=_build_daily_summary)
    t_finalize       = PythonOperator(task_id="finalize_run",             python_callable=_finalize_run)

    # -----------------------------------------------------------------------
    # Task dependencies
    # -----------------------------------------------------------------------
    t_universe >> [t_snapshot, t_metadata]
    t_metadata >> t_devmet
    [t_snapshot, t_devmet] >> t_load_raw      # todos los extracts terminaron → guardar raw
    t_load_raw >> t_extract_raw               # raw guardado → leer de vuelta desde DB
    t_extract_raw >> t_validate               # datos desde raw layer → validar
    t_validate >> t_transform >> t_load >> t_summary >> t_finalize
