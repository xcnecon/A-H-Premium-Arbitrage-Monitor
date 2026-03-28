"""A/H dual-listed stock pair mapping lookup utilities."""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Load the mapping once at module level
_PAIRS_FILE = Path(__file__).parent / "ah_pairs.json"

try:
    with open(_PAIRS_FILE, encoding="utf-8") as f:
        _AH_PAIRS: dict[str, dict[str, str]] = json.load(f)
    logger.info("Loaded %d A/H pairs from %s", len(_AH_PAIRS), _PAIRS_FILE)
except FileNotFoundError:
    logger.error("A/H pairs file not found: %s", _PAIRS_FILE)
    _AH_PAIRS = {}
except json.JSONDecodeError as e:
    logger.error("Failed to parse A/H pairs JSON: %s", e)
    _AH_PAIRS = {}

# Build reverse mapping (A code -> HK code) at module level
_REVERSE_MAP: dict[str, str] = {}
for _hk, _info in _AH_PAIRS.items():
    _a = _info.get("a_code", "")
    if _a and _a not in _REVERSE_MAP:
        _REVERSE_MAP[_a] = _hk


def _normalize_hk_code(hk_code: str) -> str:
    """Normalize HK code to 5-digit zero-padded format."""
    if not hk_code:
        return ""
    code = hk_code.replace("HK.", "").strip()
    return code.zfill(5)


def get_a_code(hk_code: str) -> str | None:
    """Look up the A-share code for a given HK code.

    Args:
        hk_code: HK stock code, e.g. "00939" or "939".

    Returns:
        A-share code string, or None if not found.
    """
    normalized = _normalize_hk_code(hk_code)
    pair = _AH_PAIRS.get(normalized)
    if pair:
        return pair["a_code"]
    # Also try the raw input in case it already matches
    pair = _AH_PAIRS.get(hk_code)
    if pair:
        return pair["a_code"]
    logger.debug("No A-share mapping found for HK code: %s", hk_code)
    return None


def get_hk_code(a_code: str) -> str | None:
    """Reverse-look up the HK code for a given A-share code.

    Args:
        a_code: A-share code, e.g. "601939".

    Returns:
        HK code string (5-digit, zero-padded), or None if not found.
    """
    hk = _REVERSE_MAP.get(a_code)
    if hk is None:
        logger.debug("No HK mapping found for A-share code: %s", a_code)
    return hk


def get_all_pairs() -> dict[str, dict[str, str]]:
    """Return the full A/H pair mapping dictionary.

    Returns:
        Dict mapping HK codes to {"a_code": ..., "name": ...}.
    """
    return dict(_AH_PAIRS)


def get_pair_name(hk_code: str) -> str | None:
    """Get the Chinese name for an A/H pair by HK code.

    Args:
        hk_code: HK stock code, e.g. "00939" or "939".

    Returns:
        Chinese company name, or None if not found.
    """
    normalized = _normalize_hk_code(hk_code)
    pair = _AH_PAIRS.get(normalized) or _AH_PAIRS.get(hk_code)
    if pair:
        return pair.get("name")
    logger.debug("No name found for HK code: %s", hk_code)
    return None
