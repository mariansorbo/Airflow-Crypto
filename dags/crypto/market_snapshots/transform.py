"""
Transformation layer — market snapshots pipeline.

Converts raw /coins/markets dicts into flat, typed rows
ready for insertion into coin_market_snapshots.
"""

import logging
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)


def _dt(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def transform_snapshot(
    coin_id: str,
    raw: dict,
    snapshot_ts: Any,
    run_type: str = "scheduled",
) -> Dict:
    """
    Maps a /coins/markets entry to a coin_market_snapshots row.

    origin_updated_time: timestamp from CoinGecko's last_updated field —
    the moment the source data was last refreshed by CoinGecko, not when
    we fetched it. The trigger trg_check_origin_updated_time compares this
    value against the last recorded one for that coin: if it is not newer,
    the insert is cancelled and redirected to coin_market_snapshots_not_updated.
    """
    return {
        "snapshot_ts":         _dt(snapshot_ts),
        "coin_id":             coin_id,
        "price_usd":           _safe_float(raw.get("current_price")),
        "market_cap_usd":      _safe_float(raw.get("market_cap")),
        "volume_24h_usd":      _safe_float(raw.get("total_volume")),
        "circulating_supply":  _safe_float(raw.get("circulating_supply")),
        "total_supply":        _safe_float(raw.get("total_supply")),
        "max_supply":          _safe_float(raw.get("max_supply")),
        "market_cap_rank":     _safe_int(raw.get("market_cap_rank")),
        "run_type":            run_type,
        "origin_updated_time": _dt(raw.get("last_updated")),
    }


def build_snapshots(
    valid_market: Dict[str, dict],
    snapshot_ts: Any,
    run_type: str = "scheduled",
) -> List[Dict]:
    """Transforms the full validated market dict into a list of snapshot rows."""
    rows = [
        transform_snapshot(cid, raw, snapshot_ts, run_type)
        for cid, raw in valid_market.items()
    ]
    log.info("Transformed %d market snapshot rows", len(rows))
    return rows
