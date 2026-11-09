"""Dated exchange-listed stock option metadata, not live option quotes."""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
SNAPSHOT_PATH = Path(__file__).resolve().parents[2] / "option_info.json"


def get_option_info(hk_code: str, a_code: str) -> dict | None:
    """Absent, mismatched or malformed data is unknown, never a negative."""
    try:
        snapshot = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
        if not snapshot.get("h_source_date") or not snapshot.get("checked_on"):
            return None
        if snapshot.get("a_market_status") != "no_exchange_listed_single_stock_options":
            return None
        for row in snapshot["pairs"]:
            if row["hk_code"] != hk_code or row["a_code"] != a_code:
                continue
            if not all(isinstance(row.get(key), bool) for key in ("h_listed", "weekly")):
                return None
            if row["h_listed"]:
                size = row.get("contract_size")
                if type(size) is not int or size <= 0 or not row.get("hkats"):
                    return None
            elif row["weekly"]:
                return None
            return dict(row, source_date=snapshot["h_source_date"],
                        checked_on=snapshot["checked_on"])
    except (OSError, ValueError, TypeError, KeyError) as exc:
        logger.warning("Option metadata unavailable: %s", exc)
    return None
