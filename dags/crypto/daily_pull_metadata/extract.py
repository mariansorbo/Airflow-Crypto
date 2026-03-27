"""
Extraction layer — daily metadata pull pipeline.

Endpoint:
    GET /coins/{id}  — one call per coin, returns metadata + developer_data.

Called once per day (not every 30 min) to avoid hitting CoinGecko's
10,000 calls/month quota on the free/demo plan.

Rate limit handling:
    On 429 Too Many Requests: waits 65 seconds and retries up to 2 times.
    Inter-request delay: 3.0 seconds to stay under 30 req/min.

At 3s delay, 50 coins ≈ 2.5 minutes total. Even with occasional retries
(+65s each) this fits comfortably in a daily schedule.
"""

import logging
import os
import time
from typing import Dict, List, Optional

import requests

log = logging.getLogger(__name__)

COINGECKO_BASE   = "https://api.coingecko.com/api/v3"
_DEFAULT_TIMEOUT = 30
_RETRY_WAIT_429  = 65   # seconds to wait after a 429 before retrying
_MAX_RETRIES     = 2    # number of retry attempts on 429


def _headers() -> Dict:
    api_key = os.getenv("COINGECKO_API_KEY", "")
    if api_key:
        return {"x-cg-demo-api-key": api_key}
    return {}


def _get(url: str, params: Dict) -> dict:
    resp = requests.get(url, params=params, headers=_headers(), timeout=_DEFAULT_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_coin_detail(coin_id: str) -> Optional[dict]:
    """
    GET /coins/{id} — returns full coin detail: metadata + developer_data.

    On 429: waits _RETRY_WAIT_429 seconds and retries up to _MAX_RETRIES times.
    On any other HTTP error (e.g. 404): returns None immediately.
    On success: returns the full JSON response dict.
    """
    params = {
        "localization":   "false",
        "tickers":        "false",
        "market_data":    "false",
        "community_data": "false",
        "developer_data": "true",
        "sparkline":      "false",
    }
    url = f"{COINGECKO_BASE}/coins/{coin_id}"

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            return _get(url, params)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 429:
                if attempt < _MAX_RETRIES:
                    log.warning(
                        "429 for %s (attempt %d/%d) — waiting %ds before retry",
                        coin_id, attempt, _MAX_RETRIES, _RETRY_WAIT_429,
                    )
                    time.sleep(_RETRY_WAIT_429)
                else:
                    log.warning("429 for %s — max retries reached, skipping", coin_id)
                    return None
            else:
                log.warning("HTTP error fetching detail for %s: %s", coin_id, exc)
                return None
        except requests.RequestException as exc:
            log.warning("Request error fetching detail for %s: %s", coin_id, exc)
            return None

    return None


def fetch_all_details(coin_ids: List[str], delay: float = 3.0) -> Dict[str, Optional[dict]]:
    """
    Fetches /coins/{id} for every coin with an inter-request delay.

    Returns Dict[coin_id → full response dict | None].
    Coins where the API call failed have None as value.
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
