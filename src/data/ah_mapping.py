"""A/H dual-listed stock pair registry — single source of truth.

The pair registry lives at ``ah_pairs.csv`` in the project root. Schema:

    hk_code, a_code, name, status, is_red_chip, source, first_seen

The CSV is read into an in-memory cache at import time and kept fresh by
``refresh_pairs_cache()`` after the daily discovery job mutates the file.

Mutation API (``add_pair``, ``mark_pairs_delisted``) is used
by the background discovery worker. All writes are serialised by a module
lock and use ``os.replace()`` so concurrent readers never observe a torn
file. Pair rows are sorted by ``hk_code`` on every write to keep git diffs
diff-friendly.

Test isolation: set ``AH_ARB_PAIRS_CSV`` env var to redirect to a tmp file.
"""

import csv
import logging
import os
import pathlib
import tempfile
import threading
from datetime import date

logger = logging.getLogger(__name__)

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
_DEFAULT_PAIRS_FILE = _REPO_ROOT / "ah_pairs.csv"

_FIELDNAMES = [
    "hk_code",
    "a_code",
    "name",
    "status",
    "is_red_chip",
    "is_restricted",
    "source",
    "first_seen",
]
_lock = threading.Lock()


def _csv_path() -> pathlib.Path:
    override = os.environ.get("AH_ARB_PAIRS_CSV")
    return pathlib.Path(override) if override else _DEFAULT_PAIRS_FILE


def _load_csv() -> list[dict[str, str]]:
    path = _csv_path()
    if not path.exists():
        logger.warning("Pairs CSV missing: %s", path)
        return []
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(rows: list[dict[str, str]]) -> None:
    """Atomic write — sorts by hk_code for stable git diffs."""
    path = _csv_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(rows, key=lambda r: r["hk_code"])
    fd, tmp = tempfile.mkstemp(suffix=".tmp", prefix="ah_pairs_", dir=path.parent)
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=_FIELDNAMES)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in _FIELDNAMES})
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def _build_indexes(
    rows: list[dict[str, str]],
) -> tuple[dict[str, dict[str, str]], dict[str, str]]:
    """Build hk→info and a→hk maps from active rows only."""
    pairs: dict[str, dict[str, str]] = {}
    reverse: dict[str, str] = {}
    for r in rows:
        if r.get("status", "active") != "active":
            continue
        hk = r.get("hk_code", "")
        a = r.get("a_code", "")
        if not hk or not a:
            continue
        pairs[hk] = {
            "a_code": a,
            "name": r.get("name", ""),
            "is_restricted": r.get("is_restricted", "false"),
            "is_red_chip": r.get("is_red_chip", "false"),
        }
        if a not in reverse:
            reverse[a] = hk
    return pairs, reverse


_AH_PAIRS, _REVERSE_MAP = _build_indexes(_load_csv())


def refresh_pairs_cache() -> int:
    """Rebuild in-memory maps from CSV. Returns active pair count."""
    global _AH_PAIRS, _REVERSE_MAP
    with _lock:
        _AH_PAIRS, _REVERSE_MAP = _build_indexes(_load_csv())
        logger.info("Refreshed A/H pair cache: %d active pairs", len(_AH_PAIRS))
        return len(_AH_PAIRS)


def _normalize_hk_code(hk_code: str) -> str:
    return hk_code.replace("HK.", "").strip().zfill(5) if hk_code else ""


# ---------------------------------------------------------------------------
# Read API
# ---------------------------------------------------------------------------


def get_a_code(hk_code: str) -> str | None:
    pair = _AH_PAIRS.get(_normalize_hk_code(hk_code))
    if pair:
        return pair["a_code"]
    logger.debug("No A-share mapping found for HK code: %s", hk_code)
    return None


def get_hk_code(a_code: str) -> str | None:
    hk = _REVERSE_MAP.get(a_code)
    if hk is None:
        logger.debug("No HK mapping found for A-share code: %s", a_code)
    return hk


def get_all_pairs() -> dict[str, dict[str, str]]:
    """Return active pairs as {hk_code: {a_code, name}}."""
    return dict(_AH_PAIRS)


def get_pair_name(hk_code: str) -> str | None:
    pair = _AH_PAIRS.get(_normalize_hk_code(hk_code))
    return pair.get("name") if pair else None


def is_restricted(hk_code: str) -> bool:
    """Whether the A-share is blocked for US investors (NS-CMIC / BIS Entity List)."""
    pair = _AH_PAIRS.get(_normalize_hk_code(hk_code))
    return pair.get("is_restricted") == "true" if pair else False


def is_red_chip(hk_code: str) -> bool:
    """Whether the HK-listed entity is a red-chip (incorporated outside mainland China)."""
    pair = _AH_PAIRS.get(_normalize_hk_code(hk_code))
    return pair.get("is_red_chip") == "true" if pair else False


def get_all_pairs_meta() -> list[dict[str, str]]:
    """Return raw CSV rows including status/is_red_chip — for admin views and discovery."""
    return _load_csv()


# ---------------------------------------------------------------------------
# Mutation API — used by the daily pair_discovery background worker
# ---------------------------------------------------------------------------


def add_pair(
    hk_code: str,
    a_code: str,
    name: str,
    source: str = "hkex",
    is_red_chip: bool = False,
) -> str:
    """Insert a new pair if absent. Returns ``"new"`` or ``"unchanged"``.

    Does NOT reactivate delisted rows — once a pair flips to ``status='delisted'``
    it stays that way until the user edits ah_pairs.csv. Otherwise discovery
    would daily flip back HKEX-listed shells whose A-side has no trading data
    (e.g. 00042 → 000585 东电退), poisoning K-line sync with empty responses.
    """
    hk_code = _normalize_hk_code(hk_code)
    inserted = False
    with _lock:
        rows = _load_csv()
        existing = next((r for r in rows if r.get("hk_code") == hk_code), None)
        if existing is None:
            rows.append({
                "hk_code": hk_code,
                "a_code": a_code,
                "name": name or "",
                "status": "active",
                "is_red_chip": "true" if is_red_chip else "false",
                "is_restricted": "false",
                "source": source,
                "first_seen": date.today().strftime("%Y-%m-%d"),
            })
            _write_csv(rows)
            inserted = True
        elif existing.get("status") != "active":
            logger.info(
                "Pair %s exists as status=%s — not reactivating (edit ah_pairs.csv to re-enable)",
                hk_code,
                existing.get("status"),
            )
    # refresh outside the lock — refresh_pairs_cache() itself acquires _lock
    if inserted:
        refresh_pairs_cache()
        return "new"
    return "unchanged"


def mark_pairs_delisted(hk_codes: list[str]) -> int:
    """Mark active pairs as delisted. Returns count actually changed."""
    if not hk_codes:
        return 0
    target = {_normalize_hk_code(c) for c in hk_codes}
    changed = 0
    with _lock:
        rows = _load_csv()
        for r in rows:
            if r.get("hk_code") in target and r.get("status") == "active":
                r["status"] = "delisted"
                changed += 1
        if changed:
            _write_csv(rows)
    if changed:
        refresh_pairs_cache()
    return changed
