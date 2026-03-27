# Crypto Market Snapshots — Airflow Pipeline

Pipeline de ingeniería de datos que captura métricas de mercado de 50 criptomonedas cada 30 minutos desde la API pública de CoinGecko. Separa la captura de precios (cada 30 min, 1 llamada a la API) de la captura de metadata y métricas de desarrollo (1 vez por día, 50 llamadas).

---

## Stack

| Componente | Tecnología |
|---|---|
| Orquestación | Apache Airflow 2.9.3 (CeleryExecutor) |
| Base de datos | PostgreSQL 15 |
| Infraestructura | Docker Compose |
| Fuente de datos | CoinGecko API (public / Demo key) |
| Dashboard | Python + Tkinter + Matplotlib |

---

## DAGs

### `crypto_market_snapshots` — cada 30 minutos

Captura precio, market cap, volumen y supply para las 50 monedas con **una sola llamada** a `/coins/markets`.

```
get_coin_universe
      │
      ▼
extract_current_snapshot   ← GET /coins/markets (1 call, 50 coins)
      │
      ▼
load_raw_data              ── raw.coin_market_responses
      │
      ▼
extract_from_raw           ← lee desde raw.*
      │
      ▼
validate_raw_data          ← precio > 0, market_cap ≥ 0, id presente
      │
      ▼
transform_and_normalize    ← build snapshot rows + origin_updated_time
      │
      ▼
load_to_postgres           ── coin_market_snapshots (trigger dedup activo)
      │
      ▼
build_daily_summary        ── coin_daily_summary (OHLCV)
      │
      ▼
finalize_run               ── orchestration.pipeline_runs
```

### `crypto_daily_pull_metadata` — 06:00 UTC una vez por día

Captura metadata (nombre, categorías, links) y métricas de desarrollo (GitHub stars/forks) para las 50 monedas. Sin capa raw — va directo a tablas limpias.

```
get_coin_universe
      │
      ▼
extract_metadata    ← GET /coins/{id} × 50 (3s delay, retry en 429)
      │
      ▼
validate_metadata   ← id presente, name presente, response no nulo
      │
      ▼
transform_metadata  ← build coins_dim rows + coin_dev_metrics rows
      │
      ▼
load_metadata       ── coins_dim (UPSERT), coin_dev_metrics (INSERT)
```

### `crypto_backfill_manager` — manual

Detecta slots de 30 min sin run exitoso y los encola para reejecutar.

---

## Quota de API CoinGecko (plan Demo gratuito: 10.000 calls/mes)

| Pipeline | Calls por mes | Dentro del límite |
|---|---|---|
| market snapshots (1 call × 48 runs/día × 30 días) | 1.440 | ✅ |
| daily pull (50 calls × 1 run/día × 30 días) | 1.500 | ✅ |
| **Total** | **~2.940** | ✅ (vs 73.440 antes del refactor) |

---

## Schemas de la base de datos

La base de datos se llama `crypto_data` y tiene tres schemas.

### `public` — capa limpia

| Tabla | Actualizada por | Descripción |
|---|---|---|
| `coins_dim` | daily pull | Dimensión de monedas: nombre, símbolo, categorías, links. UPSERT en cada pull diario. |
| `coin_market_snapshots` | market snapshots | Fact table. Un registro por (snapshot_ts, coin_id): precio, market cap, volumen, supply, rank. |
| `coin_dev_metrics` | daily pull | Métricas de desarrollo: GitHub stars, forks, JSON completo. Un registro por (snapshot_ts, coin_id). |
| `coin_daily_summary` | market snapshots | Resumen OHLCV diario. Se recalcula con UPSERT en cada corrida de 30 min. |
| `coin_market_snapshots_not_updated` | trigger BD | Registra inserts rechazados por `trg_check_origin_updated_time` — datos cuyo `origin_updated_time` no es más nuevo que el último registrado. Sirve como queue para fuentes alternativas. |

### `raw` — capa raw (solo market data)

| Tabla | Endpoint | Descripción |
|---|---|---|
| `raw.coin_market_responses` | `/coins/markets` | JSON original de cada run de 30 min. 1 fila por (run_id, coin_id). Incluye `payload_hash` MD5 para detección de cambios. |

> La metadata no tiene capa raw — va directo a `coins_dim` y `coin_dev_metrics`.

### `orchestration` — auditoría

| Tabla | Descripción |
|---|---|
| `orchestration.pipeline_runs` | Una fila por DAG run de market snapshots. Registra status, conteos y errores. |
| `orchestration.backfill_queue` | Queue de slots faltantes detectados por el backfill manager. |

**Ciclo de vida de un run:**
```
start_run()  →  status = 'running'   (get_coin_universe)
fail_run()   →  status = 'failed'    (on_failure_callback)
finish_run() →  status = 'success'   (finalize_run, si 50/50 coins ok)
              → status = 'partial'   (finalize_run, si menos de 50 coins ok)
```

---

## Control de datos duplicados — `origin_updated_time`

`coin_market_snapshots` tiene una columna `origin_updated_time` con el timestamp `last_updated` de CoinGecko — cuándo fue actualizado el precio en la fuente, no cuándo lo capturamos.

El trigger `trg_check_origin_updated_time` (BEFORE INSERT) compara ese valor contra el último registrado por coin:

- `origin_updated_time` **mayor** al anterior → insert normal ✅
- `origin_updated_time` **igual o menor** → insert cancelado, fila redirigida a `coin_market_snapshots_not_updated` ⚠️

---

## Estructura del proyecto

```
Airflow/
├── dags/
│   └── crypto/
│       ├── coins.py                        # lista estática de 50 monedas
│       ├── pipeline_control.py             # auditoría en orchestration.*
│       ├── backfill_utils.py               # detección de gaps + trigger REST API
│       ├── crypto_market_snapshots.py      # DAG cada 30 min
│       ├── crypto_daily_pull_metadata.py   # DAG diario 06:00 UTC
│       ├── crypto_backfill_manager.py      # DAG backfill (manual)
│       ├── market_snapshots/               # módulos del pipeline de 30 min
│       │   ├── extract.py                  # fetch_market_snapshot (/coins/markets)
│       │   ├── validate.py                 # validate_snapshot
│       │   ├── transform.py                # build_snapshots
│       │   ├── load.py                     # insert_snapshots + build_daily_summary
│       │   ├── load_raw.py                 # save raw.coin_market_responses
│       │   └── extract_from_raw.py         # get_latest_batch (market only)
│       └── daily_pull_metadata/            # módulos del pipeline diario
│           ├── extract.py                  # fetch_all_details (/coins/{id} × 50)
│           ├── validate.py                 # validate_metadata
│           ├── transform.py                # build_dims + build_dev_metrics
│           └── load.py                     # upsert_coins_dim + insert_dev_metrics
├── scripts/
│   └── test_historical_polygon.py          # exploración de datos históricos
├── sql/
│   ├── init_crypto.sh                      # setup inicial (solo primer arranque)
│   ├── migrate.sh                          # runner de migraciones
│   └── migrations/
│       ├── V1__initial_schema.sql
│       ├── V2__add_run_type_and_orchestration.sql
│       ├── V3__add_backfill_queue.sql
│       ├── V4__origin_updated_time_trigger.sql
│       └── V5__drop_raw_metadata_dev_tables.sql
├── .github/
│   └── workflows/
│       └── ci.yml                          # CI: parse DAGs, syntax, migraciones
├── dashboard.py                            # dashboard Tkinter local
├── docker-compose.yml
├── .env.example
└── .gitattributes
```

---

## Setup local

### Requisitos
- Docker Desktop
- Python 3.10+ (solo para el dashboard)

### Levantar el stack

```bash
cp .env.example .env
docker compose up -d
```

Airflow queda disponible en `http://localhost:8080` (usuario: `admin` / `admin`).
PostgreSQL en `localhost:5432` base de datos `crypto_data`.

### Apagar

```bash
docker compose down
```

Los datos persisten en los volúmenes de Docker.

---

## Migraciones de schema

Cada cambio de estructura vive en `sql/migrations/` como un archivo versionado `V{n}__{descripcion}.sql`.

```bash
bash sql/migrate.sh --dry-run   # ver qué migraciones están pendientes
bash sql/migrate.sh             # aplicar migraciones pendientes
```

Las versiones aplicadas se registran en la tabla `schema_migrations` dentro de `crypto_data`.

---

## CI (GitHub Actions)

Ante cada push a `main` o `feature/**`:

| Job | Qué verifica |
|---|---|
| `lint` | Parsea los DAGs y verifica sintaxis de todos los módulos |
| `test` | Corre `pytest tests/` (cuando exista el directorio) |
| `migrations` | Levanta un Postgres temporal y aplica todas las migraciones desde cero |

---

## Backfill manager

El DAG `crypto_backfill_manager` detecta slots de 30 min sin run exitoso y los triggerlea.

1. `detect_gaps` — compara slots esperados contra `pipeline_runs`. Los faltantes van a `orchestration.backfill_queue` con estado `pending`.
2. `run_backfill` — lee los `pending`, dispara el DAG principal vía la API REST de Airflow pasando `backfill_slot` en el `conf`, marca cada uno como `triggered`.

**Limitación:** CoinGecko en plan gratuito devuelve granularidad horaria para datos históricos. Dos slots del mismo slot horario (ej. `10:00` y `10:30`) van a traer el mismo precio → quedan registrados en `coin_market_snapshots_not_updated`.

---

## Queries útiles

```sql
-- Últimas corridas
SELECT run_id, run_type, status, coins_processed, coins_expected,
       finished_at - started_at AS duration
FROM orchestration.pipeline_runs
ORDER BY started_at DESC
LIMIT 10;

-- Precio actual de las top 10
SELECT coin_id, price_usd, market_cap_usd, market_cap_rank, snapshot_ts
FROM coin_market_snapshots
WHERE snapshot_ts = (SELECT MAX(snapshot_ts) FROM coin_market_snapshots)
ORDER BY market_cap_rank
LIMIT 10;

-- Resumen diario de Bitcoin
SELECT date, open_price_usd, high_price_usd, low_price_usd,
       close_price_usd, snapshot_count
FROM coin_daily_summary
WHERE coin_id = 'bitcoin'
ORDER BY date DESC
LIMIT 7;

-- Monedas con datos sin actualizar (trigger dedup)
SELECT coin_id, COUNT(*) AS veces_sin_actualizar, MAX(snapshot_ts) AS ultima_vez
FROM coin_market_snapshots_not_updated
GROUP BY coin_id
ORDER BY 2 DESC;

-- Slots faltantes en el historial
WITH expected AS (
    SELECT generate_series(
        date_trunc('hour', (SELECT MIN(snapshot_ts) FROM orchestration.pipeline_runs))
        + INTERVAL '30 minutes' * FLOOR(EXTRACT(MINUTE FROM (SELECT MIN(snapshot_ts) FROM orchestration.pipeline_runs)) / 30),
        NOW(),
        INTERVAL '30 minutes'
    ) AS expected_ts
),
actual AS (
    SELECT date_trunc('hour', snapshot_ts)
           + INTERVAL '30 minutes' * FLOOR(EXTRACT(MINUTE FROM snapshot_ts) / 30) AS slot_ts
    FROM orchestration.pipeline_runs
    WHERE status IN ('success', 'partial')
)
SELECT e.expected_ts AS missing_batch_ts
FROM expected e
LEFT JOIN actual a ON a.slot_ts = e.expected_ts
WHERE a.slot_ts IS NULL
  AND e.expected_ts < NOW() - INTERVAL '30 minutes'
ORDER BY e.expected_ts;

-- Monedas que fallaron validación en el último run
SELECT r.coin_id
FROM raw.coin_market_responses r
WHERE r.run_id = (SELECT run_id FROM orchestration.pipeline_runs ORDER BY started_at DESC LIMIT 1)
  AND r.coin_id NOT IN (
      SELECT coin_id FROM coin_market_snapshots
      WHERE snapshot_ts >= NOW() - INTERVAL '1 hour'
  );
```
