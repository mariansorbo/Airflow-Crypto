-- V5: Drop raw.coin_metadata_responses and raw.coin_dev_responses
--
-- These tables were used when the market snapshot DAG fetched metadata
-- every 30 minutes alongside market data.
--
-- After the refactor, metadata is fetched once per day by the
-- crypto_daily_pull_metadata DAG and loaded directly into coins_dim and
-- coin_dev_metrics without a raw layer.
--
-- Only raw.coin_market_responses remains in the raw schema.

DROP TABLE IF EXISTS raw.coin_metadata_responses;
DROP TABLE IF EXISTS raw.coin_dev_responses;
