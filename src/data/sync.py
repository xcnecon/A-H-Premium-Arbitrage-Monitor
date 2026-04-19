"""Batch download orchestrator for all A/H pair historical K-line data.

Two sync modes:
  - **Initial sync**: Uses Futu request_history_kline + AKShare to download
    ALL available history. Consumes historical API quota. Runs once.
  - **Daily update**: Uses get_market_snapshot (already called by screener /
    watchlist) to build today's K-line bar. Zero historical quota consumed.
"""

import contextlib
import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
from futu import RET_OK, AuType, KLType, OpenQuoteContext

from src.config.settings import DEFAULT_FX_RATE, OPEND_HOST, OPEND_PORT, SYNC_H_WORKERS
from src.data.ah_mapping import get_all_pairs
from src.data.akshare_client import get_a_kline
from src.data.fx_client import get_fx_latest, get_fx_range
from src.data.realtime import get_a_snapshots_batch
from src.storage.kline_cache import (
    get_all_sync_meta,
    get_last_sync_date,
    load_kline,
    save_kline,
    save_premium_daily,
    update_sync_meta,
)

logger = logging.getLogger(__name__)

# Module-level lock: prevents concurrent background syncs from overlapping
_bg_sync_lock = threading.Lock()


def _prev_trading_day(d: date) -> date:
    """Return the most recent trading day before *d* (skips weekends).

    Does not account for public holidays — worst case is a harmless
    snapshot sync on a holiday (returns prev-close, no harm done).
    """
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:  # Sat=5, Sun=6
        prev -= timedelta(days=1)
    return prev

# Futu rate limit: max 60 request_history_kline calls per 30 seconds.
# Use a global lock so all worker threads share one throttle.
_FUTU_MIN_INTERVAL: float = 0.55  # ~1.8 req/s, safely under the 2 req/s cap
_futu_lock = threading.Lock()
_futu_last_call: float = 0.0


def _futu_throttle() -> None:
    """Block until enough time has passed since the last Futu kline request."""
    global _futu_last_call
    with _futu_lock:
        now = time.monotonic()
        wait = _FUTU_MIN_INTERVAL - (now - _futu_last_call)
        if wait > 0:
            time.sleep(wait)
        _futu_last_call = time.monotonic()


def sync_all(
    progress_cb: Callable[[str, int, int], None] | None = None,
    *,
    _defer_ok: bool = True,
) -> dict[str, Any]:
    """Download missing K-line data for all A/H pairs and compute premium.

    First run: fetches ALL available history via historical APIs.
    Subsequent runs: uses market snapshots for today (zero quota).

    Args:
        progress_cb: Optional callback(message, current, total) for progress.
        _defer_ok: If True (default), defer gap+today sync to background
            when no first-time full downloads are needed.  Set False when
            running from a background thread to do the actual work.

    Returns:
        Summary dict.
    """
    pairs = get_all_pairs()
    today_str = date.today().strftime("%Y-%m-%d")
    default_start = "2000-01-01"

    t0 = time.time()
    errors: list[str] = []

    # Classify each pair into: first-time / gap-fill / today-only / up-to-date
    need_full_h: list[str] = []  # never synced H → full history download
    need_full_a: list[str] = []  # never synced A → full history download
    need_gap_h: list[str] = []  # synced but missed days → historical delta
    need_gap_a: list[str] = []  # synced but missed days → historical delta
    need_today: list[str] = []  # only missing today → snapshot (zero quota)

    # Gap boundary: 2 trading days ago.  If only 0-1 trading days are missing
    # we can fill them from batch snapshots (today OHLCV + prev_close for
    # yesterday) — 4 HTTP calls total instead of 169 per-stock calls.
    # Only multi-day gaps (2+ trading days) require the historical API.
    today = date.today()
    prev_td = _prev_trading_day(today)
    prev_td_str = prev_td.strftime("%Y-%m-%d")
    prev_prev_td_str = _prev_trading_day(prev_td).strftime("%Y-%m-%d")
    is_weekday = today.weekday() < 5
    logger.info(
        "Sync boundary: today=%s prev_td=%s prev_prev_td=%s is_weekday=%s",
        today_str, prev_td_str, prev_prev_td_str, is_weekday,
    )

    all_meta = get_all_sync_meta()
    for hk, info in pairs.items():
        a_code = info["a_code"]
        last_h = all_meta.get((hk, "H"))
        last_a = all_meta.get((a_code, "A"))

        if last_h and last_h >= today_str and last_a and last_a >= today_str:
            continue  # fully synced

        # H-share classification
        if not last_h:
            need_full_h.append(hk)
        elif last_h < prev_td_str:
            need_gap_h.append(hk)  # prev_td missing → historical API
        elif last_h < today_str and is_weekday:
            need_today.append(hk)  # only today missing → batch snapshot

        # A-share classification
        if not last_a:
            need_full_a.append(hk)
        elif last_a < prev_td_str:
            need_gap_a.append(hk)  # prev_td missing → historical API
        elif last_a < today_str and is_weekday and hk not in need_today:
            need_today.append(hk)  # only today missing → batch snapshot

    skipped = (
        len(pairs)
        - len(need_full_h)
        - len(need_gap_h)
        - len(need_today)
        - len(need_full_a)
        - len(need_gap_a)
    )
    logger.info(
        "Sync classification: full_h=%d gap_h=%d full_a=%d gap_a=%d today=%d skipped≈%d",
        len(need_full_h),
        len(need_gap_h),
        len(need_full_a),
        len(need_gap_a),
        len(need_today),
        skipped,
    )

    # Nothing to do? Exit early.
    if not need_full_h and not need_gap_h and not need_full_a and not need_gap_a and not need_today:
        logger.info("All pairs up to date, nothing to sync")
        return {
            "total_pairs": len(pairs),
            "h_fetched": 0,
            "a_fetched": 0,
            "premium_computed": 0,
            "today_deferred": 0,
            "errors": [],
            "elapsed_s": 0.0,
        }

    # No first-time full downloads needed — defer everything to background.
    # The caller should run sync_background() in a thread.
    if _defer_ok and not need_full_h and not need_full_a:
        gap_count = len(need_gap_h) + len(need_gap_a)
        logger.info(
            "Deferring %d gap + %d today to background sync",
            gap_count,
            len(need_today),
        )
        return {
            "total_pairs": len(pairs),
            "h_fetched": 0,
            "a_fetched": 0,
            "premium_computed": 0,
            "today_deferred": len(need_today),
            "gap_deferred": gap_count,
            "errors": [],
            "elapsed_s": 0.0,
        }

    h_count = 0
    a_count = 0

    all_hist_h = list(set(need_full_h + need_gap_h))
    all_hist_a = list(set(need_full_a + need_gap_a))

    if all_hist_h:
        if progress_cb:
            progress_cb(f"Downloading H-share history ({len(all_hist_h)})...", 0, len(all_hist_h))
        h_count += _sync_h_klines_hist(
            {hk: pairs[hk] for hk in all_hist_h},
            default_start,
            today_str,
            progress_cb,
            errors,
        )

    if all_hist_a:
        if progress_cb:
            progress_cb(f"Downloading A-share history ({len(all_hist_a)})...", 0, len(all_hist_a))
        a_count += _sync_a_klines_hist(
            {hk: pairs[hk] for hk in all_hist_a},
            default_start,
            today_str,
            progress_cb,
            errors,
        )

    # Phase 2: Today-only update via snapshots (zero historical quota)
    if need_today:
        if progress_cb:
            progress_cb("Updating today from snapshots...", 0, 1)
        snap_count = _sync_today_from_snapshots(
            {hk: pairs[hk] for hk in need_today}, today_str, errors
        )
        h_count += snap_count
        a_count += snap_count

    logger.info(
        "Sync classification: %d full-H, %d gap-H, %d full-A, %d gap-A, %d today-only",
        len(need_full_h),
        len(need_gap_h),
        len(need_full_a),
        len(need_gap_a),
        len(need_today),
    )

    # Phase 3: FX rates + recompute premium (only for new data)
    # On first run, recompute all; on delta runs, only last 30 days
    has_new_data = h_count > 0 or a_count > 0
    if has_new_data:
        if need_full_h or need_full_a:
            # First-time stocks: need full premium computation
            prem_start = default_start
        else:
            # Delta only: recompute last 30 days to cover gaps
            prem_start = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        get_fx_range(prem_start, today_str)
        prem_count = _recompute_premium(pairs, prem_start, today_str)
    else:
        prem_count = 0

    elapsed = time.time() - t0
    summary: dict[str, Any] = {
        "total_pairs": len(pairs),
        "history_backfill": len(all_hist_h) + len(all_hist_a),
        "daily_update": len(need_today),
        "h_fetched": h_count,
        "a_fetched": a_count,
        "premium_computed": prem_count,
        "errors": errors,
        "elapsed_s": round(elapsed, 1),
    }
    logger.info("Sync complete: %s", summary)
    return summary


def sync_background() -> dict[str, Any]:
    """Run gap-fill + today sync without deferring.  For background threads.

    Re-runs the full classification and executes any gap or today syncs that
    ``sync_all()`` deferred on the main thread.  Uses a module-level lock so
    only one background sync can run at a time.
    """
    if not _bg_sync_lock.acquire(blocking=False):
        logger.info("Background sync already running, skipping")
        return {"skipped": True}
    try:
        return sync_all(_defer_ok=False)
    except Exception as e:
        logger.error("Background sync failed: %s", e)
        return {"error": str(e)}
    finally:
        _bg_sync_lock.release()


def _snapshot_has_full_ohlcv(s: dict | None) -> bool:
    """True iff the snapshot carries a real OHLCV bar worth caching.

    A snapshot with only ``price`` (and missing open/high/low/volume) used to
    be written as a degenerate bar (OHL=price, vol=0) that then pinned
    ``sync_meta`` and blocked future re-fetches. Gate writes on this check so
    incomplete snapshots leave the day unsynced — the next run's gap-fill
    will pull real data via the historical API.
    """
    if not s:
        return False
    if s.get("price", 0) <= 0:
        return False
    return (
        s.get("open", 0) > 0
        and s.get("high", 0) > 0
        and s.get("low", 0) > 0
        and s.get("volume", 0) > 0
    )


def _sync_today_from_snapshots(
    pairs: dict[str, dict],
    today_str: str,
    errors: list[str],
) -> int:
    """Update today's K-line from batch snapshots.

    Uses batch snapshot APIs — ~4 HTTP calls for all 169 stocks instead of
    169 individual historical API calls. Writes today's bar only if the
    snapshot has full OHLCV; partial snapshots are skipped so the classifier
    can pick up the gap via the historical API on the next run.
    """
    from src.calc.screener import _fetch_all_h_snapshots

    hk_codes = list(pairs.keys())
    a_codes = [pairs[hk]["a_code"] for hk in hk_codes]

    h_snaps = _fetch_all_h_snapshots(hk_codes)
    a_snaps = get_a_snapshots_batch(a_codes)
    fx = get_fx_latest()
    count = 0

    for hk in hk_codes:
        info = pairs[hk]
        a_code = info["a_code"]

        # ── H-share today's bar ──
        h = h_snaps.get(hk)
        if _snapshot_has_full_ohlcv(h):
            save_kline(
                hk,
                "H",
                pd.DataFrame(
                    [
                        {
                            "date": today_str,
                            "open": h["open"],
                            "high": h["high"],
                            "low": h["low"],
                            "close": h["price"],
                            "volume": h["volume"],
                            "turnover": h.get("turnover", 0),
                        }
                    ]
                ),
            )
            update_sync_meta(hk, "H", today_str)

        # ── A-share today's bar ──
        a = a_snaps.get(a_code)
        if _snapshot_has_full_ohlcv(a):
            save_kline(
                a_code,
                "A",
                pd.DataFrame(
                    [
                        {
                            "date": today_str,
                            "open": a["open"],
                            "high": a["high"],
                            "low": a["low"],
                            "close": a["price"],
                            "volume": a["volume"],
                            "turnover": a.get("turnover", 0),
                        }
                    ]
                ),
            )
            update_sync_meta(a_code, "A", today_str)

        # ── Premium (only when both sides had valid bars) ──
        if _snapshot_has_full_ohlcv(h) and _snapshot_has_full_ohlcv(a):
            ratio = (h["price"] * fx) / a["price"]
            h_turnover_cny = h.get("turnover", 0) * fx
            save_premium_daily(
                hk,
                pd.DataFrame(
                    [
                        {
                            "date": today_str,
                            "ratio_close": ratio,
                            "a_turnover": a.get("turnover", 0),
                            "h_turnover": h_turnover_cny,
                            "fx_rate": fx,
                        }
                    ]
                ),
            )
            count += 1

    logger.info("Snapshot daily update: %d pairs updated", count)
    return count


def _sync_h_klines_hist(
    pairs: dict[str, dict[str, str]],
    default_start: str,
    today_str: str,
    progress_cb: Callable[[str, int, int], None] | None,
    errors: list[str],
) -> int:
    """Fetch H-share K-lines via Futu — multithreaded with per-thread connections."""
    # Build work items: (hk_code, start_date)
    tasks: list[tuple[str, str]] = []
    for hk in pairs:
        last = get_last_sync_date(hk, "H")
        if last and last >= today_str:
            continue
        if last and last > default_start:
            start = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            start = default_start
        if start <= today_str:
            tasks.append((hk, start))

    if not tasks:
        return 0

    count = 0
    done = 0
    lock = threading.Lock()

    # Each thread gets its own OpenQuoteContext; track them for cleanup
    _local = threading.local()
    _contexts: list[OpenQuoteContext] = []

    def _get_ctx() -> OpenQuoteContext:
        if not hasattr(_local, "ctx"):
            ctx = OpenQuoteContext(host=OPEND_HOST, port=OPEND_PORT)
            _local.ctx = ctx
            with lock:
                _contexts.append(ctx)
        return _local.ctx

    def _fetch_one(hk: str, start: str) -> int:
        nonlocal done
        ctx = _get_ctx()
        try:
            futu_code = f"HK.{hk}"
            all_data: list[pd.DataFrame] = []
            page_req_key = None
            while True:
                _futu_throttle()
                ret, data, page_req_key = ctx.request_history_kline(
                    futu_code,
                    start=start,
                    end=today_str,
                    ktype=KLType.K_DAY,
                    autype=AuType.NONE,
                    max_count=500,
                    page_req_key=page_req_key,
                )
                if ret != RET_OK:
                    logger.warning("Futu kline failed for %s: %s", hk, str(data)[:100])
                    with lock:
                        errors.append(f"H:{hk}")
                    break
                all_data.append(data)
                if page_req_key is None:
                    break

            if all_data:
                raw = pd.concat(all_data, ignore_index=True)
                df = pd.DataFrame(
                    {
                        "date": pd.to_datetime(raw["time_key"]).dt.strftime("%Y-%m-%d"),
                        "open": raw["open"],
                        "high": raw["high"],
                        "low": raw["low"],
                        "close": raw["close"],
                        "volume": raw["volume"].astype(int),
                        "turnover": raw["turnover"],
                    }
                )
                saved = save_kline(hk, "H", df)
                if saved > 0:
                    max_date = df["date"].max()
                    if pd.notna(max_date):
                        update_sync_meta(hk, "H", max_date)
                    return saved
        except Exception as e:
            logger.warning("Futu kline error for %s: %s", hk, e)
            with lock:
                errors.append(f"H:{hk}")
        finally:
            with lock:
                done += 1
                if progress_cb:
                    progress_cb(f"H-share {hk} {pairs[hk].get('name', '')}", done, len(tasks))
        return 0

    try:
        with ThreadPoolExecutor(max_workers=SYNC_H_WORKERS) as pool:
            futures = {pool.submit(_fetch_one, hk, s): hk for hk, s in tasks}
            for future in as_completed(futures):
                count += future.result()
    finally:
        for ctx in _contexts:
            with contextlib.suppress(Exception):
                ctx.close()

    return count


def _sync_a_klines_hist(
    pairs: dict[str, dict[str, str]],
    default_start: str,
    today_str: str,
    progress_cb: Callable[[str, int, int], None] | None,
    errors: list[str],
) -> int:
    """Fetch A-share K-lines via AKShare/Tencent — multithreaded."""
    from src.config.settings import SYNC_A_WORKERS

    # Build work items: (a_code, start_date, hk_code)
    tasks: list[tuple[str, str, str]] = []
    for hk, info in pairs.items():
        a_code = info["a_code"]
        last = get_last_sync_date(a_code, "A")
        if last and last >= today_str:
            continue
        if last and last > default_start:
            start = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            start = default_start
        if start <= today_str:
            tasks.append((a_code, start, hk))

    if not tasks:
        return 0

    count = 0
    done = 0
    lock = threading.Lock()

    def _fetch_one(a_code: str, start: str, hk: str) -> int:
        nonlocal done
        try:
            df = get_a_kline(a_code, start, today_str)
            if not df.empty:
                df = df.copy()
                df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
                saved = save_kline(a_code, "A", df)
                if saved > 0:
                    max_date = df["date"].max()
                    if pd.notna(max_date):
                        update_sync_meta(a_code, "A", max_date)
                    return saved
        except Exception as e:
            logger.warning("A-share kline error for %s: %s", a_code, e)
            with lock:
                errors.append(f"A:{a_code}")
        finally:
            with lock:
                done += 1
                if progress_cb:
                    progress_cb(f"A-share {a_code}", done, len(tasks))
        return 0

    with ThreadPoolExecutor(max_workers=SYNC_A_WORKERS) as pool:
        futures = {pool.submit(_fetch_one, ac, s, hk): ac for ac, s, hk in tasks}
        for future in as_completed(futures):
            count += future.result()

    return count


def _recompute_premium(
    pairs: dict[str, dict[str, str]],
    start: str,
    end: str,
) -> int:
    """Recompute premium_daily for all pairs from cached K-line data."""
    df_fx = get_fx_range(start, end)
    count = 0

    # Prepare FX DataFrame once
    if not df_fx.empty:
        df_fx_dt = df_fx.copy()
        df_fx_dt["date"] = pd.to_datetime(df_fx_dt["date"])
    else:
        df_fx_dt = pd.DataFrame()

    for hk, info in pairs.items():
        a_code = info["a_code"]
        df_h = load_kline(hk, "H", start, end)
        df_a = load_kline(a_code, "A", start, end)

        if df_h.empty or df_a.empty:
            continue

        # Ensure date columns are datetime for merge
        df_h = df_h.copy()
        df_a = df_a.copy()
        df_h["date"] = pd.to_datetime(df_h["date"])
        df_a["date"] = pd.to_datetime(df_a["date"])

        # Inner join on overlapping trading dates
        merged = pd.merge(df_a, df_h, on="date", suffixes=("_a", "_h"))
        if merged.empty:
            continue

        # Attach FX rates
        if not df_fx_dt.empty:
            merged = pd.merge(merged, df_fx_dt[["date", "rate"]], on="date", how="left")
            merged["rate"] = merged["rate"].ffill().bfill().fillna(DEFAULT_FX_RATE)
        else:
            merged["rate"] = DEFAULT_FX_RATE

        # Filter out rows where A-share close is zero (would cause division by zero)
        merged = merged[merged["close_a"] > 0]
        if merged.empty:
            continue

        # Compute premium metrics
        result = pd.DataFrame()
        result["date"] = merged["date"].dt.strftime("%Y-%m-%d")
        result["ratio_close"] = (merged["close_h"] * merged["rate"]) / merged["close_a"]
        result["a_turnover"] = merged["turnover_a"].fillna(0).astype(float)
        result["h_turnover"] = merged["turnover_h"].fillna(0).astype(float) * merged["rate"]
        result["fx_rate"] = merged["rate"]

        saved = save_premium_daily(hk, result)
        count += saved

    logger.info("Recomputed premium_daily: %d rows", count)
    return count
