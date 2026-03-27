"""
Transformation layer — converts raw API dicts into flat, typed rows
ready to be inserted into PostgreSQL.

All datetimes are returned as ISO-8601 strings so they survive
Airflow XCom JSON serialization.
"""

import json
import logging
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dt(value: Any) -> Optional[str]:
    """Convert a datetime / Pendulum / string to an ISO-8601 string, or None."""
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


# ---------------------------------------------------------------------------
# Market snapshot row
# ---------------------------------------------------------------------------

def transform_snapshot(coin_id: str, raw: dict, snapshot_ts: Any, run_type: str = "scheduled") -> Dict:
    """
    Maps a /coins/markets entry to a coin_market_snapshots row.

    origin_updated_time: timestamp que devuelve CoinGecko en el campo last_updated,
    indica cuándo fue la última vez que la fuente actualizó el precio de ese coin.
    El trigger trg_check_origin_updated_time compara este valor contra el último
    registrado — si no es más nuevo, el insert se cancela y se redirige a
    coin_market_snapshots_not_updated.
    """
    return {
        "snapshot_ts":          _dt(snapshot_ts),
        "coin_id":              coin_id,
        "price_usd":            _safe_float(raw.get("current_price")),
        "market_cap_usd":       _safe_float(raw.get("market_cap")),
        "volume_24h_usd":       _safe_float(raw.get("total_volume")),
        "circulating_supply":   _safe_float(raw.get("circulating_supply")),
        "total_supply":         _safe_float(raw.get("total_supply")),
        "max_supply":           _safe_float(raw.get("max_supply")),
        "market_cap_rank":      _safe_int(raw.get("market_cap_rank")),
        "run_type":             run_type,
        "origin_updated_time":  _dt(raw.get("last_updated")),
    }


# ---------------------------------------------------------------------------
# Coin dimension row
# ---------------------------------------------------------------------------

def transform_coin_dim(coin_id: str, raw: dict, snapshot_ts: Any) -> Dict:
    """
    Maps a /coins/{id} response to a coins_dim row.
    """
    links      = raw.get("links", {})
    homepage   = links.get("homepage") or []
    github_repos = (links.get("repos_url") or {}).get("github") or []
    categories = raw.get("categories") or []

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
# Developer metrics row
# ---------------------------------------------------------------------------

def transform_dev_metrics(coin_id: str, dev_data: dict, snapshot_ts: Any, run_type: str = "scheduled") -> Dict:
    """
    Maps the developer_data block of /coins/{id} to a coin_dev_metrics row.
    """
    return {
        "snapshot_ts":    _dt(snapshot_ts),
        "coin_id":        coin_id,
        "github_stars":   _safe_int(dev_data.get("stars")),
        "github_forks":   _safe_int(dev_data.get("forks")),
        "dev_metric_raw": json.dumps(dev_data),
        "fetched_at":     _dt(snapshot_ts),
        "run_type":       run_type,
    }


# ---------------------------------------------------------------------------
# Raw response row
# ---------------------------------------------------------------------------

def build_raw_response(coin_id: str, endpoint: str, payload: Any, snapshot_ts: Any) -> Dict:
    """
    Wraps an arbitrary API payload in a coin_raw_responses row.
    """
    return {
        "snapshot_ts":     _dt(snapshot_ts),
        "coin_id":         coin_id,
        "source_endpoint": endpoint,
        "raw_payload":     json.dumps(payload, default=str),
        "inserted_at":     _dt(snapshot_ts),
    }


# ---------------------------------------------------------------------------
# Batch helpers called from the DAG
# ---------------------------------------------------------------------------

def build_snapshots(valid_market: Dict[str, dict], snapshot_ts: Any, run_type: str = "scheduled") -> List[Dict]:
    rows = [transform_snapshot(cid, raw, snapshot_ts, run_type) for cid, raw in valid_market.items()]
    log.info("Transformed %d market snapshot rows", len(rows))
    return rows


def build_dims(valid_metadata: Dict[str, dict], snapshot_ts: Any) -> List[Dict]:
    rows = [transform_coin_dim(cid, raw, snapshot_ts) for cid, raw in valid_metadata.items()]
    log.info("Transformed %d coin_dim rows", len(rows))
    return rows


def build_dev_metrics(dev_metrics: Dict[str, dict], snapshot_ts: Any, run_type: str = "scheduled") -> List[Dict]:
    rows = [
        transform_dev_metrics(cid, dev_data, snapshot_ts, run_type)
        for cid, dev_data in dev_metrics.items()
        if dev_data  # skip empty dicts
    ]
    log.info("Transformed %d dev_metric rows", len(rows))
    return rows


def build_raw_responses(
    market_raw: Dict[str, dict],
    metadata_raw: Dict[str, dict],
    snapshot_ts: Any,
) -> List[Dict]:
    rows: List[Dict] = []
    for cid, payload in market_raw.items():
        rows.append(build_raw_response(cid, "/coins/markets", payload, snapshot_ts))
    for cid, payload in metadata_raw.items():
        if payload:
            rows.append(build_raw_response(cid, "/coins/{id}", payload, snapshot_ts))
    log.info("Built %d raw_response rows", len(rows))
    return rows
