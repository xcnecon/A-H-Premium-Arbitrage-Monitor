"""Daily incremental discovery of new A/H dual-listed pairs.

Flow on each daily run:

1. Download HKEX List-of-Securities xlsx → candidate HK codes
   (Main-Board Equity, ISIN country ∈ {CN, HK, KY, BM, CH, VG}).
2. Diff against ``pair_discovery_scanned`` → only NEW codes are queried.
3. For each new HK code, call HKEX ``getequityquote`` widget.
   ``underlying_ric`` field (e.g. ``"601939.SS"``) gives the A-share code.
   Confirmed A+H pairs are appended to ``ah_pairs.csv`` via ``ah_mapping``.
4. Tencent ``stock_zh_ah_name`` cross-check: any HK code Tencent flags as
   A+H but we do NOT have in our CSV is reported via Telegram so the user
   can manually verify and add. Red-chip pairs (HK-primary listings whose
   A-side was added later) are intentionally NOT auto-resolved — fuzzy
   name matching produces too many cross-industry false positives.
5. Mark anything now missing from both authoritative lists as ``delisted``
   (soft delete — K-line cache is preserved).
"""

import io
import json
import logging
import re
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import Any

import pandas as pd

from src.data import ah_mapping
from src.storage.db import get_scanned_hk_codes, mark_scanned_bulk
from src.storage.kline_cache import get_last_discovery_date, update_discovery_meta

logger = logging.getLogger(__name__)

_HKEX_LOS_URL = (
    "https://www.hkex.com.hk/eng/services/trading/securities/"
    "securitieslists/ListOfSecurities.xlsx"
)
_HKEX_WIDGET_URL = (
    "https://www1.hkex.com.hk/hkexwidget/data/getequityquote"
    "?sym={sym}&token={token}&lang=eng&qid={qid}&callback=jq"
)
# Public widget token — harvested from HKEX A-share Lookup Tools page JS.
# No auth; change if HKEX rotates it.
_HKEX_TOKEN = "evLtsLsBNAUVTPxtGqVeGwKMwvI77sNCNEBw3Jnbquu17u9ouFmyoOydw%2fiUlw49"
_HKEX_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.hkex.com.hk/",
}

# ISINs of PRC-flavoured issuers that commonly have an A-share counterpart.
_CANDIDATE_ISIN_PREFIXES = ("CN", "HK", "KY", "BM", "CH", "VG")

# 16 threads keeps a cold-start scan of ~2400 codes under 3 minutes.
_WIDGET_WORKERS = 16
_WIDGET_TIMEOUT = 15.0

_discovery_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Classify — fast (< 1 s), called on every page load
# ---------------------------------------------------------------------------


def classify() -> dict[str, Any]:
    """Return whether (and how) a discovery pass is needed."""
    today_str = date.today().strftime("%Y-%m-%d")
    last = get_last_discovery_date()
    if last and last >= today_str:
        return {"already_done_today": True, "deferred": 0, "last_run": last}
    return {"already_done_today": False, "deferred": 1, "last_run": last}


# ---------------------------------------------------------------------------
# Background worker
# ---------------------------------------------------------------------------


def discover_background() -> dict[str, Any]:
    """Run one full/incremental discovery pass. Safe for daemon thread.

    Never raises — all errors are logged and surfaced via the return dict.
    """
    if not _discovery_lock.acquire(blocking=False):
        logger.info("Pair discovery already running — skipping duplicate call")
        return {"skipped": True}

    started = time.time()
    result: dict[str, Any] = {
        "scanned": 0,
        "new_pairs": [],
        "delisted": [],
        "alerts": [],
    }
    try:
        today_str = date.today().strftime("%Y-%m-%d")
        last = get_last_discovery_date()
        if last and last >= today_str:
            logger.info("Pair discovery already ran today (%s)", last)
            result["already_done_today"] = True
            return result

        candidates = _load_los_candidates()
        already_scanned = get_scanned_hk_codes()
        to_scan = sorted(candidates - already_scanned)
        logger.info(
            "Pair discovery: %d candidate HK codes, %d already scanned, %d new",
            len(candidates),
            len(already_scanned),
            len(to_scan),
        )

        hkex_pairs = _query_widgets(to_scan) if to_scan else []
        hkex_pairs, dead_a_pairs = _resolve_names_and_a_liveness(hkex_pairs)

        # Mark every queried code as scanned. For dead-A codes also mark
        # scanned (is_ah=False) so we don't re-alert tomorrow for the same one.
        all_classified_hk = {p[0] for p in hkex_pairs} | {p[0] for p in dead_a_pairs}
        scan_log: list[tuple[str, str, bool]] = [
            (hk, today_str, hk in all_classified_hk) for hk in to_scan
        ]
        mark_scanned_bulk(scan_log)

        # Add confirmed A+H pairs to the CSV (delisted rows are sticky — skipped)
        for hk_code, a_code, name in hkex_pairs:
            outcome = ah_mapping.add_pair(hk_code, a_code, name, source="hkex")
            if outcome == "new":
                result["new_pairs"].append(
                    {"hk_code": hk_code, "a_code": a_code, "name": name}
                )

        # Dead-A pairs — Telegram alert + suppress (do NOT add to CSV)
        for hk_code, a_code, name in dead_a_pairs:
            try:
                from src.alerts.telegram import send_alert

                send_alert(_format_dead_a_pair_alert(hk_code, a_code, name))
            except Exception as e:
                logger.warning("Dead-A Telegram alert for %s failed: %s", hk_code, e)
            result["alerts"].append({
                "hk_code": hk_code,
                "a_code": a_code,
                "name": name,
                "kind": "dead_a",
            })
            logger.info(
                "Dead-A pair skipped: %s -> %s (%s)", hk_code, a_code, name
            )

        # Tencent cross-check — alert on unknown red-chip-style A+H pairs
        alerts, tencent_hk = _tencent_alert_unknown(today_str)
        result["alerts"].extend(alerts)

        # Soft-delete pairs no longer present in either authoritative source.
        # Safety: if Tencent failed silently (empty set), skip delisting.
        if tencent_hk:
            authoritative = candidates | tencent_hk
            current_active = {
                r["hk_code"]
                for r in ah_mapping.get_all_pairs_meta()
                if r.get("status") == "active"
            }
            to_delist = sorted(current_active - authoritative)
            if to_delist:
                n = ah_mapping.mark_pairs_delisted(to_delist)
                result["delisted"] = to_delist
                logger.info("Soft-deleted %d delisted pairs: %s", n, to_delist)
        else:
            logger.warning("Tencent HK list empty — skipping soft-delete for safety")

        result["scanned"] = len(to_scan)
        update_discovery_meta(today_str)

        # Refresh in-memory mapping cache so the dashboard sees new pairs
        ah_mapping.refresh_pairs_cache()

        result["elapsed_s"] = round(time.time() - started, 1)
        logger.info(
            "Pair discovery done in %.1fs: +%d new, %d delisted, %d alerts",
            result["elapsed_s"],
            len(result["new_pairs"]),
            len(result["delisted"]),
            len(result["alerts"]),
        )
        return result

    except Exception as e:
        logger.exception("Pair discovery failed")
        result["error"] = str(e)
        return result

    finally:
        _discovery_lock.release()


# ---------------------------------------------------------------------------
# HKEX List of Securities — authoritative HK-side candidate pool
# ---------------------------------------------------------------------------


def _load_los_candidates() -> set[str]:
    """Return set of 5-digit HK codes that are plausible A+H candidates."""
    req = urllib.request.Request(_HKEX_LOS_URL, headers=_HKEX_HEADERS)
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read()
    df = pd.read_excel(io.BytesIO(raw), header=2)
    mask = (
        (df["Category"] == "Equity")
        & df["Sub-Category"].astype(str).str.contains("Main Board", na=False)
        & df["ISIN"].astype(str).str[:2].isin(_CANDIDATE_ISIN_PREFIXES)
    )
    codes = df.loc[mask, "Stock Code"].astype(int).astype(str).str.zfill(5)
    return set(codes.tolist())


# ---------------------------------------------------------------------------
# HKEX getequityquote widget — gives underlying_ric = "<A_CODE>.SS|SZ"
# ---------------------------------------------------------------------------

_RIC_RE = re.compile(r"^(\d{6})\.(SS|SZ)$")
_JSONP_PREFIX_RE = re.compile(r"^[^(]+\(")


def _query_widget(sym: int) -> tuple[str, str, str | None] | None:
    """Query one HKEX widget. Returns (hk_code, a_code, name) if A+H, else None."""
    url = _HKEX_WIDGET_URL.format(
        sym=sym, token=_HKEX_TOKEN, qid=int(time.time() * 1000)
    )
    req = urllib.request.Request(url, headers=_HKEX_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=_WIDGET_TIMEOUT) as r:
            raw = r.read().decode()
    except Exception as e:
        logger.debug("widget query %s failed: %s", sym, e)
        return None
    body = _JSONP_PREFIX_RE.sub("", raw).rstrip(");)")
    try:
        quote = json.loads(body)["data"]["quote"]
    except (ValueError, KeyError):
        return None
    ric = quote.get("underlying_ric") or ""
    m = _RIC_RE.match(ric)
    if not m:
        return None
    hk_code = f"{sym:05d}"
    return (hk_code, m.group(1), quote.get("nm"))


def _query_widgets(syms: list[str]) -> list[tuple[str, str, str | None]]:
    """Concurrently query many HKEX widgets."""
    results: list[tuple[str, str, str | None]] = []
    with ThreadPoolExecutor(max_workers=_WIDGET_WORKERS) as ex:
        for r in ex.map(lambda s: _query_widget(int(s)), syms):
            if r:
                results.append(r)
    return results


def _resolve_names_and_a_liveness(
    pairs: list[tuple[str, str, str | None]],
) -> tuple[
    list[tuple[str, str, str | None]],  # live: A side trades, write to CSV
    list[tuple[str, str, str | None]],  # dead_a: A side delisted, Telegram alert
]:
    """Bulk akshare fetch + per-row Tencent fallback. Returns (live, dead_a).

    Live pairs get HKEX English names replaced with Chinese names where found.
    Dead-A pairs are split off for Telegram alert — HKEX widget often keeps
    pointing at A codes that have already 退市 (e.g. 00042 → 000585 东电退),
    and writing them to the CSV poisons the daily K-line sync (Tencent
    chunks 26 years × 11 s = 55 s of empty responses on every page load).

    Lookup chain for each pair:
      1. ``stock_zh_ah_name`` — HK code → Chinese name (A+H list, bulk)
      2. ``stock_zh_a_spot``  — A code → Chinese name (active A-shares, bulk)
      3. Tencent ``qt.gtimg.cn/q=hk<code>`` per missing row (per-row HTTP)

    A code presence in ``stock_zh_a_spot`` is the liveness signal: that table
    only includes actively trading A-shares; delisted/suspended codes are absent.

    On akshare failure, returns (pairs_unchanged, []) — name enrichment and
    liveness check both degrade gracefully (better to add a pair with English
    name than to drop it on a network blip).
    """
    if not pairs:
        return pairs, []
    hk_to_cn: dict[str, str] = {}
    a_to_cn: dict[str, str] = {}
    spot_loaded = False
    try:
        import akshare as ak

        from src.data.akshare_client import _a_share_proxy_env

        with _a_share_proxy_env():
            ah_df = ak.stock_zh_ah_name()
            a_spot = ak.stock_zh_a_spot()
        ah_df.columns = ["hk_code", "name"]
        ah_df["hk_code"] = ah_df["hk_code"].astype(str).str.zfill(5)
        hk_to_cn = dict(zip(ah_df["hk_code"], ah_df["name"]))
        a_spot["a_code"] = a_spot["代码"].astype(str).str.extract(r"([0-9]{6}$)")
        a_spot = a_spot.dropna(subset=["a_code"])
        a_to_cn = dict(zip(a_spot["a_code"], a_spot["名称"].astype(str).str.strip()))
        spot_loaded = True
    except Exception as e:
        logger.warning("Chinese-name / A-liveness tables unavailable (%s)", e)

    live: list[tuple[str, str, str | None]] = []
    dead_a: list[tuple[str, str, str | None]] = []
    for hk, a, eng in pairs:
        cn = hk_to_cn.get(hk) or a_to_cn.get(a)
        if not cn:
            cn = _tencent_hk_name(hk)
        name = cn or eng
        if cn and cn != eng:
            logger.info("Enriched name %s: %r -> %r", hk, eng, cn)
        # A-side liveness: only enforce when spot table actually loaded
        # (don't drop pairs on transient akshare failure).
        if spot_loaded and a not in a_to_cn:
            dead_a.append((hk, a, name))
        else:
            live.append((hk, a, name))
    return live, dead_a


def _format_dead_a_pair_alert(hk_code: str, a_code: str, name: str | None) -> str:
    """Telegram message for HKEX-classified pairs whose A side is delisted."""
    return (
        "\u26a0\ufe0f <b>A+H pair has dead A side</b>\n\n"
        f"<b>HK code:</b> <code>{hk_code}</code> (still trading on HKEX)\n"
        f"<b>A code:</b> <code>{a_code}</code> (not in A-share spot list — likely 退市)\n"
        f"<b>Name:</b> {name or 'unknown'}\n\n"
        "HKEX widget still points at this A code, but Tencent's active A-share "
        "list does not include it — so daily sync would chunk through years of "
        "empty responses on every page load.\n\n"
        "<b>Skipped — not added to ah_pairs.csv.</b> If you believe this is wrong, "
        "add the row by hand:\n"
        f"<pre>{hk_code},{a_code},{name or '???'},active,false,manual,YYYY-MM-DD</pre>"
    )


_TENCENT_HK_NAME_RE = re.compile(r'v_hk\d+="\d+~([^~]+)~')


def _tencent_hk_name(hk_code: str) -> str | None:
    """Look up a HK ticker's Chinese name via Tencent's free HK quote endpoint."""
    try:
        req = urllib.request.Request(
            f"https://qt.gtimg.cn/q=hk{hk_code}",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            body = r.read().decode("gbk", errors="replace")
        m = _TENCENT_HK_NAME_RE.search(body)
        if m:
            name = m.group(1).strip()
            return name or None
    except Exception as e:
        logger.debug("Tencent HK name lookup failed for %s: %s", hk_code, e)
    return None


# ---------------------------------------------------------------------------
# Tencent cross-check — alert on red-chip-style A+H we don't yet have
# ---------------------------------------------------------------------------


def _tencent_alert_unknown(today_str: str) -> tuple[list[dict], set[str]]:
    """Find Tencent-flagged A+H HK codes missing from our CSV; alert via Telegram.

    Returns ``(alert_entries, tencent_hk_universe)``. The universe is reused
    by the caller for delisting safety. Codes are marked scanned (is_ah=False)
    after alerting to suppress duplicate alerts on subsequent days.
    """
    try:
        import akshare as ak

        ah_df = ak.stock_zh_ah_name()
    except Exception as e:
        logger.warning("Tencent AH list unavailable (%s) — cross-check skipped", e)
        return [], set()
    ah_df.columns = ["hk_code", "name"]
    ah_df["hk_code"] = ah_df["hk_code"].astype(str).str.zfill(5)
    tencent_hk = set(ah_df["hk_code"].tolist())

    known_hk = {r["hk_code"] for r in ah_mapping.get_all_pairs_meta()}
    already_scanned = get_scanned_hk_codes()
    unknown = tencent_hk - known_hk - already_scanned
    if not unknown:
        return [], tencent_hk

    logger.info("Tencent cross-check: %d HK codes unknown to our CSV", len(unknown))
    alerts: list[dict] = []
    for _, row in ah_df[ah_df["hk_code"].isin(unknown)].iterrows():
        hk = row["hk_code"]
        h_name = str(row["name"]).strip()
        try:
            from src.alerts.telegram import send_alert

            send_alert(_format_unknown_pair_alert(hk, h_name))
        except Exception as e:
            logger.warning("Telegram alert for %s failed: %s", hk, e)
        # Suppress duplicate alerts: mark scanned so next-day filter excludes it.
        mark_scanned_bulk([(hk, today_str, False)])
        alerts.append({"hk_code": hk, "name": h_name})
        logger.info("Alerted unknown A+H: %s %s", hk, h_name)
    return alerts, tencent_hk


def _format_unknown_pair_alert(hk_code: str, h_name: str) -> str:
    """HTML message asking the user to verify + manually add to ah_pairs.csv."""
    return (
        "\U0001f50d <b>New A+H pair detected</b>\n\n"
        f"<b>HK code:</b> <code>{hk_code}</code>\n"
        f"<b>Name:</b> {h_name}\n\n"
        "Tencent reports this as A+H but the HKEX widget did not classify it "
        "(typically a red-chip dual-listing).\n\n"
        "If confirmed, add a row to <code>ah_pairs.csv</code>:\n"
        f"<pre>{hk_code},&lt;A_CODE&gt;,{h_name},active,true,manual,YYYY-MM-DD</pre>"
    )
