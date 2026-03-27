-- =============================================================================
-- V4 — origin_updated_time + trigger de deduplicación por origen
-- =============================================================================
-- Qué hace este migration:
--   1. Agrega la columna origin_updated_time a coin_market_snapshots
--      Representa el timestamp que devuelve CoinGecko indicando cuándo
--      actualizó por última vez ese dato (campo last_updated de la API).
--
--   2. Crea la tabla coin_market_snapshots_not_updated
--      Recibe los inserts rechazados — casos donde CoinGecko no actualizó
--      el dato entre dos corridas consecutivas del pipeline.
--      Sirve como queue para futuras llamadas a fuentes alternativas.
--
--   3. Crea la función fn_check_origin_updated_time
--      Lógica del trigger: compara el origin_updated_time entrante contra
--      el máximo registrado para ese coin_id. Si no es más nuevo, redirige
--      a coin_market_snapshots_not_updated y cancela el insert original.
--
--   4. Crea el trigger trg_check_origin_updated_time
--      BEFORE INSERT en coin_market_snapshots, llama a la función anterior.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Nueva columna en coin_market_snapshots
-- -----------------------------------------------------------------------------
ALTER TABLE coin_market_snapshots
    ADD COLUMN IF NOT EXISTS origin_updated_time TIMESTAMPTZ;

COMMENT ON COLUMN coin_market_snapshots.origin_updated_time IS
    'Timestamp del ultimo update segun CoinGecko (last_updated en la API). '
    'Permite detectar si el dato cambio entre dos corridas del pipeline.';

-- Indice para acelerar la busqueda del MAX por coin_id dentro del trigger
CREATE INDEX IF NOT EXISTS idx_snapshots_coin_origin_ts
    ON coin_market_snapshots (coin_id, origin_updated_time DESC);

-- -----------------------------------------------------------------------------
-- 2. Tabla de inserts rechazados
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS coin_market_snapshots_not_updated (
    id                  BIGSERIAL    PRIMARY KEY,
    snapshot_ts         TIMESTAMPTZ  NOT NULL,
    latest_update_time  TIMESTAMPTZ,
    coin_id             VARCHAR(100) NOT NULL,
    inserted_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE coin_market_snapshots_not_updated IS
    'Registra cada caso donde el pipeline intentó insertar un snapshot pero '
    'CoinGecko no habia actualizado el dato desde la corrida anterior. '
    'Puede usarse como queue para consultar fuentes de precio alternativas.';

CREATE INDEX IF NOT EXISTS idx_not_updated_coin_ts
    ON coin_market_snapshots_not_updated (coin_id, snapshot_ts DESC);

-- -----------------------------------------------------------------------------
-- 3. Función del trigger
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_check_origin_updated_time()
RETURNS TRIGGER AS $$
DECLARE
    v_last_origin_ts TIMESTAMPTZ;
BEGIN
    -- Si origin_updated_time no viene informado, dejamos pasar sin comparar
    IF NEW.origin_updated_time IS NULL THEN
        RETURN NEW;
    END IF;

    -- Buscar el ultimo origin_updated_time registrado para este coin
    SELECT MAX(origin_updated_time)
      INTO v_last_origin_ts
      FROM coin_market_snapshots
     WHERE coin_id = NEW.coin_id;

    -- Primer registro para este coin: siempre permitir
    IF v_last_origin_ts IS NULL THEN
        RETURN NEW;
    END IF;

    -- El dato de CoinGecko es mas nuevo: permitir el insert
    IF NEW.origin_updated_time > v_last_origin_ts THEN
        RETURN NEW;
    END IF;

    -- El dato no cambio: registrar en la tabla de no-actualizados
    INSERT INTO coin_market_snapshots_not_updated
        (snapshot_ts, latest_update_time, coin_id, inserted_at)
    VALUES
        (NEW.snapshot_ts, NEW.origin_updated_time, NEW.coin_id, NOW());

    -- Cancelar el insert original
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- -----------------------------------------------------------------------------
-- 4. Trigger
-- -----------------------------------------------------------------------------
DROP TRIGGER IF EXISTS trg_check_origin_updated_time ON coin_market_snapshots;

CREATE TRIGGER trg_check_origin_updated_time
    BEFORE INSERT ON coin_market_snapshots
    FOR EACH ROW
    EXECUTE FUNCTION fn_check_origin_updated_time();
