"""
Validation layer — daily metadata pull pipeline.

Validates raw /coins/{id} payloads before transformation:
    - response must not be None (API call did not fail)
    - 'id' field must be present
    - 'name' field must be present
"""

import logging
from typing import Dict, List, Tuple

log = logging.getLogger(__name__)


def validate_metadata(
    raw_metadata: Dict[str, dict],
) -> Tuple[Dict[str, dict], List[str]]:
    """
    Validates raw coin metadata from /coins/{id}.

    Returns:
        valid   — dict of coin_id → metadata that passed all checks
        errors  — list of human-readable error strings for failed coins
    """
    valid: Dict[str, dict] = {}
    errors: List[str] = []

    for coin_id, data in raw_metadata.items():
        if data is None:
            errors.append(f"{coin_id}: null metadata response (API call failed)")
            continue
        if not data.get("id"):
            errors.append(f"{coin_id}: missing 'id' in metadata")
            continue
        if not data.get("name"):
            errors.append(f"{coin_id}: missing 'name' in metadata")
            continue
        valid[coin_id] = data

    if errors:
        log.warning(
            "Validation [metadata]: %d/%d passed | %d errors | first 3: %s",
            len(valid), len(raw_metadata), len(errors), errors[:3],
        )
    else:
        log.info("Validation [metadata]: all %d coins passed", len(valid))

    return valid, errors
