"""Exchange eligibility snapshot, independent from broker borrow inventory."""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
SNAPSHOT_PATH = Path(__file__).resolve().parents[2] / "short_eligibility.json"


def get_short_eligibility(hk_code: str, a_code: str) -> dict | None:
    """Return an exact pair match; absent or invalid data means unknown."""
    try:
        snapshot = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
        for row in snapshot["pairs"]:
            if row["hk_code"] == hk_code and row["a_code"] == a_code:
                if not all(isinstance(row[key], bool) for key in ("a_eligible", "h_eligible")):
                    return None
                if not all(row.get(key) for key in ("a_source_date", "h_source_date")):
                    return None
                return dict(row)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        logger.warning("Short eligibility snapshot unavailable: %s", exc)
    return None
