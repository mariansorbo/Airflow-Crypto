"""
Validation layer — market snapshots pipeline.

Validates raw /coins/markets payloads before transformation:
    - coin_id must be present and non-empty
    - current_price must be a positive number
    - market_cap must be >= 0
"""

import logging
from typing import Dict, List, Tuple

log = logging.getLogger(__name__)


def validate_snapshot(
    raw_snapshot: Dict[str, dict],
) -> Tuple[Dict[str, dict], List[str]]:
    """
    Validates raw market data from /coins/markets.

    Returns:
        valid   — dict of coin_id → raw data that passed all checks
        errors  — list of human-readable error strings for failed coins
    """
    valid: Dict[str, dict] = {}
    errors: List[str] = []

    for coin_id, data in raw_snapshot.items():
        if data is None:
            errors.append(f"{coin_id}: null response")
            continue
        if not data.get("id"):
            errors.append(f"{coin_id}: missing 'id' field")
            continue
        price = data.get("current_price")
        if price is None:
            errors.append(f"{coin_id}: 'current_price' is null")
            continue
        if not isinstance(price, (int, float)) or price <= 0:
            errors.append(f"{coin_id}: invalid price={price}")
            continue
        market_cap = data.get("market_cap")
        if market_cap is None:
            errors.append(f"{coin_id}: 'market_cap' is null")
            continue
        if not isinstance(market_cap, (int, float)) or market_cap < 0:
            errors.append(f"{coin_id}: invalid market_cap={market_cap}")
            continue
        valid[coin_id] = data

    if errors:
        log.warning(
            "Validation [snapshot]: %d/%d passed | %d errors | first 3: %s",
            len(valid), len(raw_snapshot), len(errors), errors[:3],
        )
    else:
        log.info("Validation [snapshot]: all %d coins passed", len(valid))

    return valid, errors
