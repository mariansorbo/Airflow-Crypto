"""
Transformation layer — daily metadata pull pipeline.

Converts raw /coins/{id} responses into flat, typed rows
ready for insertion into coins_dim and coin_dev_metrics.
"""

import json
import logging
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)


def _dt(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# coins_dim row
# ---------------------------------------------------------------------------

def transform_coin_dim(coin_id: str, raw: dict, snapshot_ts: Any) -> Dict:
    """
    Maps a /coins/{id} response to a coins_dim row.

    Extracts: symbol, name, categories, homepage URL, GitHub repo URL.
    All fields are optional — missing data is stored as None.
    """
    links        = raw.get("links", {})
    homepage     = links.get("homepage") or []
    github_repos = (links.get("repos_url") or {}).get("github") or []
    categories   = raw.get("categories") or []

    return {
        "coin_id":     coin_id,
        "symbol":      (raw.get("symbol") or "").upper(),
        "name":        raw.get("name", ""),
        "categories":  json.dumps([c for c in categories if c]),
        "website_url": next((u for u in homepage if u), None),
        "github_url":  next((u for u in github_repos if u), None),
        "updated_at":  _dt(snapshot_ts),
    }


# ---------------------------------------------------------------------------
# coin_dev_metrics row
# ---------------------------------------------------------------------------

def transform_dev_metrics(
    coin_id: str,
    dev_data: dict,
    snapshot_ts: Any,
) -> Dict:
    """
    Maps the developer_data block of /coins/{id} to a coin_dev_metrics row.

    dev_data fields used:
        stars                — GitHub stars
        forks                — GitHub forks
        (full block stored as JSONB in dev_metric_raw for future use)
    """
    return {
        "snapshot_ts":    _dt(snapshot_ts),
        "coin_id":        coin_id,
        "github_stars":   _safe_int(dev_data.get("stars")),
        "github_forks":   _safe_int(dev_data.get("forks")),
        "dev_metric_raw": json.dumps(dev_data),
        "fetched_at":     _dt(snapshot_ts),
        "run_type":       "scheduled",
    }


# ---------------------------------------------------------------------------
# Batch helpers called from the DAG
# ---------------------------------------------------------------------------

def build_dims(valid_metadata: Dict[str, dict], snapshot_ts: Any) -> List[Dict]:
    """Transforms the full validated metadata dict into a list of coins_dim rows."""
    rows = [transform_coin_dim(cid, raw, snapshot_ts) for cid, raw in valid_metadata.items()]
    log.info("Transformed %d coins_dim rows", len(rows))
    return rows


def build_dev_metrics(
    valid_metadata: Dict[str, dict],
    snapshot_ts: Any,
) -> List[Dict]:
    """
    Extracts developer_data from each metadata response and transforms
    into coin_dev_metrics rows. Coins with empty developer_data are skipped.
    """
    rows = []
    for cid, raw in valid_metadata.items():
        dev_data = raw.get("developer_data") or {}
        if dev_data:
            rows.append(transform_dev_metrics(cid, dev_data, snapshot_ts))
    log.info("Transformed %d coin_dev_metrics rows", len(rows))
    return rows
