"""
Validation layer — checks structure and business rules on raw API responses.

Rules applied to market snapshots:
  - coin_id must be present and non-empty
  - price_usd must be a positive number
  - market_cap_usd must be >= 0

Rules applied to metadata:
  - coin_id must be present
  - name must be present
"""

import logging
from typing import Dict, List, Tuple

log = logging.getLogger(__name__)


def validate_snapshot(
    raw_snapshot: Dict[str, dict],
) -> Tuple[Dict[str, dict], List[str]]:
    """
    Validate raw market snapshot data.

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

        # coin_id present
        if not data.get("id"):
            errors.append(f"{coin_id}: missing 'id' field")
            continue

        # price positive
        price = data.get("current_price")
        if price is None:
            errors.append(f"{coin_id}: 'current_price' is null")
            continue
        if not isinstance(price, (int, float)) or price <= 0:
            errors.append(f"{coin_id}: invalid price={price}")
            continue

        # market_cap non-negative
        market_cap = data.get("market_cap")
        if market_cap is None:
            errors.append(f"{coin_id}: 'market_cap' is null")
            continue
        if not isinstance(market_cap, (int, float)) or market_cap < 0:
            errors.append(f"{coin_id}: invalid market_cap={market_cap}")
            continue

        valid[coin_id] = data

    _log_summary("snapshot", len(raw_snapshot), len(valid), errors)
    return valid, errors


def validate_metadata(
    raw_metadata: Dict[str, dict],
) -> Tuple[Dict[str, dict], List[str]]:
    """
    Validate raw coin metadata.

    Returns:
        valid   — dict of coin_id → metadata that passed all checks
        errors  — list of human-readable error strings for failed coins
    """
    valid: Dict[str, dict] = {}
    errors: List[str] = []

    for coin_id, data in raw_metadata.items():
        if data is None:
            errors.append(f"{coin_id}: null metadata response")
            continue
        if not data.get("id"):
            errors.append(f"{coin_id}: missing 'id' in metadata")
            continue
        if not data.get("name"):
            errors.append(f"{coin_id}: missing 'name' in metadata")
            continue
        valid[coin_id] = data

    _log_summary("metadata", len(raw_metadata), len(valid), errors)
    return valid, errors


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _log_summary(label: str, total: int, valid: int, errors: List[str]) -> None:
    if errors:
        log.warning(
            "Validation [%s]: %d/%d passed | %d errors | first 3: %s",
            label,
            valid,
            total,
            len(errors),
            errors[:3],
        )
    else:
        log.info("Validation [%s]: all %d coins passed", label, valid)
