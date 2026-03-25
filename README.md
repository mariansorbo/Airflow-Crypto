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

### `orchestration` — auditoría

| Tabla | Descripción |
|---|---|
| `orchestration.pipeline_runs` | Una fila por DAG run. Registra status, conteos de filas procesadas y errores. |

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
| 3 | `extract_coin_metadata` | Llama a `/coins/{id}` para cada moneda (50 requests con delay de 2s para respetar rate limits). Devuelve metadata completa. |
| 4 | `extract_dev_metrics` | Extrae el bloque `developer_data` del XCom de `extract_coin_metadata`. No hace requests adicionales. |
| 5 | `load_raw_data` | Persiste los tres payloads crudos en `raw.*` con `ON CONFLICT DO NOTHING`. Calcula `payload_hash`. |
| 6 | `extract_from_raw` | Recupera los datos del run actual desde `raw.*`. Desacopla la extracción de la transformación. |
| 7 | `validate_raw_data` | Valida cada snapshot (precio > 0, market cap ≥ 0, id presente). Las monedas que no pasan se descartan con warning. |
| 8 | `transform_and_normalize` | Convierte los dicts crudos en filas estructuradas para cada tabla destino. Agrega `run_type` y `snapshot_ts`. |
| 9 | `load_to_postgres` | Hace UPSERT de `coins_dim` y bulk insert de snapshots, dev metrics y raw responses en las tablas públicas. |
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
├── sql/
│   ├── init_crypto.sh               # setup inicial (solo primer arranque)
│   ├── migrate.sh                   # runner de migraciones
│   └── migrations/
│       ├── V1__initial_schema.sql
│       └── V2__add_run_type_and_orchestration.sql
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

-- Monedas que fallaron validación en el último run
SELECT r.coin_id
FROM raw.coin_market_responses r
WHERE r.run_id = (SELECT run_id FROM orchestration.pipeline_runs ORDER BY started_at DESC LIMIT 1)
  AND r.coin_id NOT IN (
      SELECT coin_id FROM coin_market_snapshots
      WHERE snapshot_ts >= NOW() - INTERVAL '1 hour'
  );
```
