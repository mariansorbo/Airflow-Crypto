"""
Extraction layer — CoinMarketCap market snapshots pipeline.

Endpoint:
    GET /v1/cryptocurrency/quotes/latest  → batch query by symbol for all 50 coins

Returns data normalized to CoinGecko-compatible field names so the shared
validate / transform / load pipeline works without modification.

Field mapping:
    CoinGecko field     ←  CoinMarketCap field
    ──────────────────────────────────────────
    id                  ←  (coingecko_id, passed in)
    current_price       ←  quote.USD.price
    market_cap          ←  quote.USD.market_cap
    total_volume        ←  quote.USD.volume_24h
    circulating_supply  ←  circulating_supply
    total_supply        ←  total_supply
    max_supply          ←  max_supply
    market_cap_rank     ←  cmc_rank
    last_updated        ←  quote.USD.last_updated

Environment variable required:
    CMC_API_KEY — CoinMarketCap API key (Basic plan or above)
"""

import logging
import os
import time
from typing import Dict, List, Tuple

import requests

log = logging.getLogger(__name__)

CMC_BASE = "https://pro-api.coinmarketcap.com/v1"
_DEFAULT_TIMEOUT = 30
_BATCH_SIZE = 100  # CMC Basic plan supports up to 100 symbols per request


def _headers() -> Dict:
    api_key = os.getenv("CMC_API_KEY", "")
    if not api_key:
        raise EnvironmentError("CMC_API_KEY environment variable is not set")
    return {
        "X-CMC_PRO_API_KEY": api_key,
        "Accept":            "application/json",
    }


def _get(url: str, params: Dict) -> dict:
    resp = requests.get(url, params=params, headers=_headers(), timeout=_DEFAULT_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _normalize(coingecko_id: str, cmc_entry: dict) -> dict:
    """
    Maps a single CoinMarketCap /quotes/latest entry to CoinGecko-compatible
    field names consumed by the shared validate / transform pipeline.
    """
    usd = cmc_entry.get("quote", {}).get("USD", {})
    return {
        "id":                  coingecko_id,
        "current_price":       usd.get("price"),
        "market_cap":          usd.get("market_cap"),
        "total_volume":        usd.get("volume_24h"),
        "circulating_supply":  cmc_entry.get("circulating_supply"),
        "total_supply":        cmc_entry.get("total_supply"),
        "max_supply":          cmc_entry.get("max_supply"),
        "market_cap_rank":     cmc_entry.get("cmc_rank"),
        "last_updated":        usd.get("last_updated"),
    }


def fetch_market_snapshot(coin_pairs: List[Tuple[str, str]]) -> Dict[str, dict]:
    """
    GET /v1/cryptocurrency/quotes/latest — batch query by symbol.

    coin_pairs: list of (coingecko_id, symbol) from COIN_UNIVERSE
    Returns a dict keyed by coingecko_id with normalized fields matching
    the CoinGecko /coins/markets format expected by the shared pipeline.

    Handles batches of up to 100 symbols per request.
    When CMC returns multiple entries for the same symbol (symbol conflict),
    the one with the lowest cmc_rank (i.e. highest market cap) is used.
    """
    symbol_to_id = {symbol.upper(): cgid for cgid, symbol in coin_pairs}
    symbols = list(symbol_to_id.keys())

    results: Dict[str, dict] = {}

    for i in range(0, len(symbols), _BATCH_SIZE):
        batch = symbols[i : i + _BATCH_SIZE]
        params = {
            "symbol":  ",".join(batch),
            "convert": "USD",
        }
        response = _get(f"{CMC_BASE}/cryptocurrency/quotes/latest", params)
        data = response.get("data", {})

        for symbol, entry in data.items():
            coingecko_id = symbol_to_id.get(symbol.upper())
            if coingecko_id is None:
                log.warning("Unexpected symbol from CMC: %s — skipped", symbol)
                continue

            # CMC may return a list when multiple coins share the same symbol;
            # pick the one with the lowest cmc_rank (highest market cap).
            if isinstance(entry, list):
                entry = min(entry, key=lambda x: x.get("cmc_rank") or 999_999)

            results[coingecko_id] = _normalize(coingecko_id, entry)

        log.info(
            "Fetched CMC snapshot batch %d-%d (%d coins returned)",
            i, i + _BATCH_SIZE, len(data),
        )
        if i + _BATCH_SIZE < len(symbols):
            time.sleep(1.0)

    missing = set(symbol_to_id.values()) - set(results.keys())
    if missing:
        log.warning("CMC returned no data for CoinGecko IDs: %s", sorted(missing))

    return results
