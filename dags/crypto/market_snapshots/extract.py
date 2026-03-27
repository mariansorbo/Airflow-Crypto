"""
Extraction layer — market snapshots pipeline.

Endpoint:
    GET /coins/markets  → single batch call for all 50 coins (price, market cap,
                          volume, supply, rank, last_updated)

One call returns all 50 coins. No per-coin loop, no rate limit issues.
"""

import logging
import os
import time
from typing import Dict, List

import requests

log = logging.getLogger(__name__)

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
_DEFAULT_TIMEOUT = 30


def _headers() -> Dict:
    api_key = os.getenv("COINGECKO_API_KEY", "")
    if api_key:
        return {"x-cg-demo-api-key": api_key}
    return {}


def _get(url: str, params: Dict) -> dict:
    resp = requests.get(url, params=params, headers=_headers(), timeout=_DEFAULT_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_market_snapshot(coin_ids: List[str]) -> Dict[str, dict]:
    """
    GET /coins/markets — single request for all 50 coins.

    Returns a dict keyed by coin_id with raw CoinGecko market fields:
        current_price, market_cap, total_volume, circulating_supply,
        total_supply, max_supply, market_cap_rank, last_updated, ...

    Handles batches of up to 250 IDs per request (CoinGecko limit).
    """
    results: Dict[str, dict] = {}
    batch_size = 250

    for i in range(0, len(coin_ids), batch_size):
        batch = coin_ids[i : i + batch_size]
        params = {
            "vs_currency": "usd",
            "ids":         ",".join(batch),
            "order":       "market_cap_desc",
            "per_page":    len(batch),
            "page":        1,
            "sparkline":   False,
        }
        data = _get(f"{COINGECKO_BASE}/coins/markets", params)
        for coin in data:
            results[coin["id"]] = coin
        log.info("Fetched market snapshot batch %d-%d (%d coins)", i, i + batch_size, len(data))
        if i + batch_size < len(coin_ids):
            time.sleep(1.5)

    return results
