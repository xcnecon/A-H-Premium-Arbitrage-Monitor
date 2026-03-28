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
    get_last_sync_date,
    load_kline,
    save_kline,
    save_premium_daily,
    update_sync_meta,
)

logger = logging.getLogger(__name__)

_FUTU_DELAY: float = 0.5
_A_DELAY: float = 1.0


def sync_all(
    progress_cb: Callable[[str, int, int], None] | None = None,
) -> dict[str, Any]:
    """Download missing K-line data for all A/H pairs and compute premium.

    First run: fetches ALL available history via historical APIs.
    Subsequent runs: uses market snapshots for today (zero quota).

    Args:
        progress_cb: Optional callback(message, current, total) for progress.

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

    yesterday_str = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    for hk, info in pairs.items():
        a_code = info["a_code"]
        last_h = get_last_sync_date(hk, "H")
        last_a = get_last_sync_date(a_code, "A")

        if last_h and last_h >= today_str and last_a and last_a >= today_str:
            continue  # fully synced

        # H-share classification
        if not last_h:
            need_full_h.append(hk)
        elif last_h < yesterday_str:
            need_gap_h.append(hk)  # missed >1 day, need historical API
        elif last_h < today_str:
            need_today.append(hk)  # only missing today

        # A-share classification
        if not last_a:
            need_full_a.append(hk)
        elif last_a < yesterday_str:
            need_gap_a.append(hk)

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


def _sync_today_from_snapshots(
    pairs: dict[str, dict],
    today_str: str,
    errors: list[str],
) -> int:
    """Update today's K-line row using market snapshots — zero quota consumed.

    Uses the same batch snapshot APIs that the screener/watchlist already call.
    Converts snapshot OHLCV into a single K-line row per stock.
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

        # H-share snapshot → kline row
        h = h_snaps.get(hk)
        if h and h.get("price", 0) > 0:
            df_h = pd.DataFrame(
                [
                    {
                        "date": today_str,
                        "open": h.get("open", h["price"]),
                        "high": h.get("high", h["price"]),
                        "low": h.get("low", h["price"]),
                        "close": h["price"],
                        "volume": h.get("volume", 0),
                        "turnover": h.get("turnover", 0),
                    }
                ]
            )
            save_kline(hk, "H", df_h)
            update_sync_meta(hk, "H", today_str)

            # Also compute today's premium
            a = a_snaps.get(a_code)
            if a and a.get("price", 0) > 0:
                ratio = (h["price"] * fx) / a["price"]
                h_turnover_cny = h.get("turnover", 0) * fx
                df_prem = pd.DataFrame(
                    [
                        {
                            "date": today_str,
                            "ratio_close": ratio,
                            "a_turnover": a.get("turnover", 0),
                            "h_turnover": h_turnover_cny,
                            "fx_rate": fx,
                        }
                    ]
                )
                save_premium_daily(hk, df_prem)

                # Save A-share kline too
                df_a = pd.DataFrame(
                    [
                        {
                            "date": today_str,
                            "open": a.get("open", a["price"]),
                            "high": a.get("high", a["price"]),
                            "low": a.get("low", a["price"]),
                            "close": a["price"],
                            "volume": a.get("volume", 0),
                            "turnover": a.get("turnover", 0),
                        }
                    ]
                )
                save_kline(a_code, "A", df_a)
                update_sync_meta(a_code, "A", today_str)

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
            time.sleep(_FUTU_DELAY)
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
