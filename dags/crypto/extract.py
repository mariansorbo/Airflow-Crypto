"""
CoinGecko extraction layer.

Endpoints used:
  - /coins/markets  → batch market snapshot (1 call for all 50 coins)
  - /coins/{id}     → coin detail: metadata + developer_data (1 call per coin)

Free public API rate limit: ~30 req/min.
Set COINGECKO_API_KEY env var for a Demo key (higher limits).
"""

import logging
import os
import time
from typing import Dict, List, Optional

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


# ---------------------------------------------------------------------------
# Market snapshot  — single API call for all coins
# ---------------------------------------------------------------------------

def fetch_market_snapshot(coin_ids: List[str]) -> Dict[str, dict]:
    """
    GET /coins/markets
    Returns a dict keyed by coin_id with raw CoinGecko market fields.
    Handles batches of up to 250 ids per request.
    """
    results: Dict[str, dict] = {}
    batch_size = 250  # CoinGecko max per page

    for i in range(0, len(coin_ids), batch_size):
        batch = coin_ids[i : i + batch_size]
        params = {
            "vs_currency": "usd",
            "ids": ",".join(batch),
            "order": "market_cap_desc",
            "per_page": len(batch),
            "page": 1,
            "sparkline": False,
        }
        data = _get(f"{COINGECKO_BASE}/coins/markets", params)
        for coin in data:
            results[coin["id"]] = coin
        log.info("Fetched market snapshot batch %d-%d (%d coins)", i, i + batch_size, len(data))
        if i + batch_size < len(coin_ids):
            time.sleep(1.5)

    return results


# ---------------------------------------------------------------------------
# Coin metadata + developer metrics  — one call per coin
# ---------------------------------------------------------------------------

def fetch_coin_detail(coin_id: str) -> Optional[dict]:
    """
    GET /coins/{id}
    Returns full coin detail (metadata + developer_data).
    Returns None on non-fatal errors (404, 429 after logging).
    """
    params = {
        "localization": "false",
        "tickers": "false",
        "market_data": "false",
        "community_data": "false",
        "developer_data": "true",
        "sparkline": "false",
    }
    try:
        return _get(f"{COINGECKO_BASE}/coins/{coin_id}", params)
    except requests.HTTPError as exc:
        log.warning("HTTP error fetching detail for %s: %s", coin_id, exc)
        return None
    except requests.RequestException as exc:
        log.warning("Request error fetching detail for %s: %s", coin_id, exc)
        return None


def fetch_all_details(coin_ids: List[str], delay: float = 2.2) -> Dict[str, Optional[dict]]:
    """
    Fetch /coins/{id} for every coin with inter-request delay to stay under
    the free-tier rate limit (~30 req/min → 2 s gap is safe).
    """
    results: Dict[str, Optional[dict]] = {}
    total = len(coin_ids)

    for idx, coin_id in enumerate(coin_ids, start=1):
        results[coin_id] = fetch_coin_detail(coin_id)
        log.info("Fetched detail [%d/%d]: %s", idx, total, coin_id)
        if idx < total:
            time.sleep(delay)

    fetched = sum(1 for v in results.values() if v is not None)
    log.info("Detail fetch complete: %d/%d successful", fetched, total)
    return results
