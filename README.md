# Crypto Market Snapshots — Airflow Pipeline

Pipeline de ingeniería de datos que captura métricas de mercado de 50 criptomonedas cada 30 minutos desde la API pública de CoinGecko, las persiste en una capa raw, las valida, transforma y carga en PostgreSQL.

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

## Arquitectura general

```
CoinGecko API
      │
      ▼
┌─────────────────────────────────────────────────────┐
│                   Airflow DAG                       │
│  (crypto_market_snapshots — cada 30 minutos)        │
│                                                     │
│  get_coin_universe                                  │
│       ├──────────────────────┐                      │
│       ▼                      ▼                      │
│  extract_current_snapshot   extract_coin_metadata   │
│       │                      │                      │
│       │                      ▼                      │
│       │              extract_dev_metrics            │
│       │                      │                      │
│       └──────────┬───────────┘                      │
│                  ▼                                  │
│           load_raw_data        ──► raw.*            │
│                  │                                  │
│                  ▼                                  │
│          extract_from_raw      ◄── raw.*            │
│                  │                                  │
│                  ▼                                  │
│          validate_raw_data                          │
│                  │                                  │
│                  ▼                                  │
│       transform_and_normalize                       │
│                  │                                  │
│                  ▼                                  │
│          load_to_postgres      ──► public.*         │
│                  │                                  │
│                  ▼                                  │
│        build_daily_summary     ──► public.*         │
│                  │                                  │
│                  ▼                                  │
│            finalize_run        ──► orchestration.*  │
└─────────────────────────────────────────────────────┘
```

---

## Schemas de la base de datos

La base de datos se llama `crypto_data` y tiene tres schemas con responsabilidades bien separadas.

### `public` — capa limpia

| Tabla | Descripción |
|---|---|
| `coins_dim` | Dimensión de monedas: nombre, símbolo, categorías, links. Se actualiza en cada run. |
| `coin_market_snapshots` | Fact table principal. Un registro por (timestamp, moneda): precio, market cap, volumen, supply. |
| `coin_dev_metrics` | Métricas de actividad de desarrollo: stars y forks de GitHub, datos crudos completos en JSONB. |
| `coin_raw_responses` | Respaldo de payloads JSON de la API antes de ser procesados (tabla legacy, reemplazada por `raw.*`). |
| `coin_daily_summary` | Resumen OHLCV diario por moneda. Se recalcula con UPSERT en cada corrida. |

### `raw` — capa raw

Guarda el JSON original de cada endpoint de CoinGecko, sin transformar. Clave de deduplicación: `(run_id, coin_id, source_endpoint)`.

| Tabla | Endpoint origen |
|---|---|
| `raw.coin_market_responses` | `/coins/markets` |
| `raw.coin_metadata_responses` | `/coins/{id}` |
| `raw.coin_dev_responses` | `/coins/{id}` → bloque `developer_data` |

Cada fila incluye `payload_hash` (MD5 del JSON con keys ordenadas) para detectar cambios entre corridas.

### `public` — tablas adicionales de calidad

| Tabla | Descripción |
|---|---|
| `coin_market_snapshots_not_updated` | Registra los casos donde CoinGecko devolvió un dato con el mismo `origin_updated_time` que el último snapshot conocido. El trigger `trg_check_origin_updated_time` lo redirige acá en lugar de insertarlo como duplicado. Puede usarse para triggerear llamadas a fuentes de datos alternativas. |

### `orchestration` — auditoría

| Tabla | Descripción |
|---|---|
| `orchestration.pipeline_runs` | Una fila por DAG run. Registra status, conteos de filas procesadas y errores. |
| `orchestration.backfill_queue` | Queue de slots faltantes detectados por el DAG de backfill. Cada fila es un intervalo de 30 min que no tuvo run exitoso. |

**Ciclo de vida de un run:**

```
start_run()  →  status = 'running'   (tarea get_coin_universe)
fail_run()   →  status = 'failed'    (on_failure_callback, si falla cualquier tarea)
finish_run() →  status = 'success'   (tarea finalize_run, si 50/50 monedas ok)
              → status = 'partial'   (tarea finalize_run, si menos de 50 monedas ok)
```

---

## Detalle de las tareas del DAG

| # | Tarea | Qué hace |
|---|---|---|
| 1 | `get_coin_universe` | Lee la lista fija de 50 coins. Captura el timestamp real (`datetime.now(UTC)`). Registra el inicio del run en `pipeline_runs`. |
| 2 | `extract_current_snapshot` | Llama a `/coins/markets` — un solo request para los 50 coins. Devuelve precio, market cap, volumen, supply y rank. |
| 3 | `extract_coin_metadata` | Llama a `/coins/{id}` para cada moneda (50 requests con delay de 3s). Si recibe 429, espera 65s y reintenta hasta 2 veces antes de descartar la moneda. |
| 4 | `extract_dev_metrics` | Extrae el bloque `developer_data` del XCom de `extract_coin_metadata`. No hace requests adicionales. |
| 5 | `load_raw_data` | Persiste los tres payloads crudos en `raw.*` con `ON CONFLICT DO NOTHING`. Calcula `payload_hash`. |
| 6 | `extract_from_raw` | Recupera los datos del run actual desde `raw.*`. Desacopla la extracción de la transformación. |
| 7 | `validate_raw_data` | Valida cada snapshot (precio > 0, market cap ≥ 0, id presente). Las monedas que no pasan se descartan con warning. |
| 8 | `transform_and_normalize` | Convierte los dicts crudos en filas estructuradas para cada tabla destino. Agrega `run_type` y `snapshot_ts`. |
| 9 | `load_to_postgres` | Hace UPSERT de `coins_dim` y bulk insert de snapshots, dev metrics y raw responses. El trigger `trg_check_origin_updated_time` intercepta cada insert en `coin_market_snapshots`: si el dato de CoinGecko no es más nuevo que el último registrado para esa moneda, lo redirige a `coin_market_snapshots_not_updated` en lugar de insertarlo. |
| 10 | `build_daily_summary` | Recalcula el resumen OHLCV del día actual con un CTE + `ON CONFLICT DO UPDATE`. |
| 11 | `finalize_run` | Cierra el registro en `pipeline_runs` con los conteos finales y el status correcto. |

---

## Universo de monedas

50 criptomonedas fijas definidas en `dags/crypto/coins.py`. Criterio: no-stablecoins, liquidez relevante, IDs de CoinGecko.

Incluye: Bitcoin, Ethereum, Solana, BNB, XRP, Cardano, Dogecoin, Chainlink, Avalanche, Polkadot, Uniswap, Aave, Maker, y otros 37 más.

---

## Estructura del proyecto

```
Airflow/
├── dags/
│   ├── crypto_market_snapshots.py   # DAG principal
│   └── crypto/
│       ├── coins.py                 # lista de 50 monedas
│       ├── extract.py               # llamadas a la API de CoinGecko
│       ├── validate.py              # validación de payloads crudos
│       ├── transform.py             # transformación a filas estructuradas
│       ├── load.py                  # escritura en tablas públicas
│       ├── load_raw.py              # escritura en raw.*
│       ├── extract_from_raw.py      # lectura desde raw.*
│       └── pipeline_control.py      # auditoría en orchestration.*
├── dags/
│   ├── crypto_backfill_manager.py   # DAG de backfill: detecta gaps y los encola
│   └── crypto/
│       └── backfill_utils.py        # detección de gaps + trigger de runs vía API REST
├── scripts/
│   └── test_historical_polygon.py   # exploración de datos históricos de CoinGecko
├── sql/
│   ├── init_crypto.sh               # setup inicial (solo primer arranque)
│   ├── migrate.sh                   # runner de migraciones
│   └── migrations/
│       ├── V1__initial_schema.sql
│       ├── V2__add_run_type_and_orchestration.sql
│       ├── V3__add_backfill_queue.sql
│       └── V4__origin_updated_time_trigger.sql
├── .github/
│   └── workflows/
│       └── ci.yml                   # CI: parse DAG, syntax, migraciones
├── dashboard.py                     # dashboard Tkinter local
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

Cada cambio de estructura de la base de datos vive en `sql/migrations/` como un archivo versionado `V{n}__{descripcion}.sql`.

```bash
# Ver qué migraciones están pendientes
bash sql/migrate.sh --dry-run

# Aplicar migraciones pendientes
bash sql/migrate.sh
```

Las versiones aplicadas se registran en la tabla `schema_migrations` dentro de `crypto_data`.

---

## CI (GitHub Actions)

Ante cada push a `main` o `feature/**` se corren tres jobs:

| Job | Qué verifica |
|---|---|
| `lint` | Parsea el DAG completo y verifica sintaxis de todos los módulos |
| `test` | Corre `pytest tests/` (cuando exista el directorio) |
| `migrations` | Levanta un Postgres temporal y aplica todas las migraciones desde cero |

---

## Backfill manager

Un segundo DAG (`crypto_backfill_manager`) se encarga de detectar y completar slots faltantes.

**Cómo funciona:**

1. `detect_gaps` — compara los slots esperados de 30 min contra `pipeline_runs`. Los faltantes se insertan en `orchestration.backfill_queue` con estado `pending`.
2. `trigger_backfill_runs` — lee los `pending`, dispara el DAG principal vía la API REST de Airflow pasando el `backfill_slot` en el `conf`, y los marca como `triggered`.

**Limitación conocida:** CoinGecko en plan gratuito devuelve granularidad horaria para datos históricos. Dos slots del mismo slot horario (ej. `10:00` y `10:30`) van a traer el mismo precio. Esto queda registrado y es visible en `coin_market_snapshots_not_updated`.

---

## Control de datos duplicados — `origin_updated_time`

`coin_market_snapshots` tiene una columna `origin_updated_time` que registra el timestamp `last_updated` que devuelve CoinGecko para cada precio — es decir, cuándo fue actualizado el dato en la fuente, no cuándo fue capturado por el pipeline.

Un trigger `BEFORE INSERT` (`trg_check_origin_updated_time`) compara este valor contra el último registrado para cada moneda:

- Si `origin_updated_time` es **mayor** al anterior → insert normal ✅
- Si es **igual o menor** (dato sin actualizar) → insert cancelado, se registra en `coin_market_snapshots_not_updated` ⚠️

La tabla `coin_market_snapshots_not_updated` actúa como queue para identificar monedas con baja frecuencia de actualización en CoinGecko y potencialmente consultar fuentes alternativas.

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

-- Monedas con datos sin actualizar (mismo origin_updated_time que el run anterior)
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
