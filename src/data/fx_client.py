"""HKD/CNH exchange rate fetcher with SQLite caching.

Sources (in priority order):
  1. Yahoo Finance via yfinance — HKDCNH=X for live rate (offshore, tradeable)
  2. Yahoo Finance via yfinance — HKDCNY=X for historical range (CNH has no
     Yahoo history; CNY-CNH spread is typically < 0.1%, acceptable proxy)
  3. AKShare fx_spot_quote — live spot only (backup)
"""

import logging
from datetime import date, datetime, timedelta

import pandas as pd
import yfinance as yf

from src.config.settings import DEFAULT_FX_RATE
from src.storage.db import get_fx_cached, get_fx_range_cached, save_fx_rates

logger = logging.getLogger(__name__)


# ─── Source 1: Yahoo Finance via yfinance (handles crumb/cookie auth) ───


def _yahoo_fx_history(start: str, end: str) -> pd.DataFrame:
    """Fetch daily HKD→CNH from Yahoo Finance via yfinance.

    HKDCNH=X has no historical data on Yahoo, so we use HKDCNY=X as proxy.
    The CNY-CNH spread is typically < 0.1% — acceptable for premium calculation.

    Returns DataFrame with columns: date, rate (approx CNH per 1 HKD).
    """
    # yfinance end date is exclusive, add 1 day
    end_dt = datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)
    df = yf.download(
        "HKDCNY=X", start=start, end=end_dt.strftime("%Y-%m-%d"),
        interval="1d", progress=False, auto_adjust=False, multi_level_index=False,
    )
    if df is None or df.empty:
        return pd.DataFrame()
    rows = []
    for idx, row in df.iterrows():
        c = row["Close"]
        if pd.notna(c):
            rows.append({"date": idx.date(), "rate": round(float(c), 6)})
    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


def _yahoo_fx_latest() -> float | None:
    """Fetch latest HKD→CNH from Yahoo Finance via yfinance."""
    df = yf.download(
        "HKDCNH=X", period="5d", interval="1d",
        progress=False, auto_adjust=False, multi_level_index=False,
    )
    if df is None or df.empty:
        return None
    for c in reversed(df["Close"].tolist()):
        if pd.notna(c):
            return round(float(c), 6)
    return None


# ─── Source 2: AKShare fx_spot_quote (live fallback) ───


def _akshare_fx_spot() -> float | None:
    """Fetch live HKD/CNH spot from AKShare."""
    try:
        import akshare as ak

        df = ak.fx_spot_quote()
        if df is None or df.empty:
            return None
        for _, row in df.iterrows():
            row_str = " ".join(str(v) for v in row.values)
            if "HKD" in row_str.upper() and "CNH" in row_str.upper():
                for val in row.values:
                    try:
                        rate = float(val)
                        if 0.5 < rate < 1.5:
                            return rate
                    except (ValueError, TypeError):
                        continue
    except Exception as e:
        logger.warning("AKShare FX spot failed: %s", e)
    return None


# ─── Public API ───


def get_fx_latest() -> float:
    """Get the latest CNH-per-HKD rate.

    Priority: SQLite cache (instant) → network fetch (slow, only if stale).
    The rate moves <0.1% per day, so a cached value from today or yesterday is fine.

    Returns:
        CNH per 1 HKD (e.g., 0.92)
    """
    # 1. Check SQLite cache — today or yesterday (instant, no network)
    today = date.today()
    for d in [today, today - timedelta(days=1)]:
        cached = get_fx_cached(d.isoformat())
        if cached is not None:
            logger.debug("FX from cache (%s): %.5f", d, cached)
            return cached

    # 2. Cache miss — fetch from network (Yahoo first — direct HK, no proxy)
    for name, fn in [("Yahoo", _yahoo_fx_latest), ("AKShare", _akshare_fx_spot)]:
        try:
            rate = fn()
            if rate and 0.5 < rate < 1.5:
                logger.info("FX latest from %s: %.5f", name, rate)
                # Persist to cache for next time
                save_fx_rates(pd.DataFrame([{"date": today, "rate": rate}]))
                return rate
        except Exception as e:
            logger.warning("FX latest from %s failed: %s", name, e)

    # 3. Any cached value at all
    from src.storage.db import get_fx_range_cached

    recent = get_fx_range_cached("2020-01-01", today.isoformat())
    if not recent.empty and "rate" in recent.columns:
        rate = recent.iloc[-1]["rate"]
        logger.info("FX from historical cache: %.5f", rate)
        return float(rate)

    logger.warning("All FX sources failed, using default %s", DEFAULT_FX_RATE)
    return DEFAULT_FX_RATE


def get_fx_range(start: str, end: str) -> pd.DataFrame:
    """Get daily CNH-per-HKD rates for a date range.

    Checks SQLite cache first, fetches missing dates from Yahoo Finance,
    then persists new data.

    Returns:
        DataFrame with columns: date (datetime.date), rate (float)
    """
    cached_df = get_fx_range_cached(start, end)

    if not cached_df.empty:
        cached_dates = set(cached_df["date"].tolist())
        all_dates = set(pd.bdate_range(start, end).date)
        missing = all_dates - cached_dates
        if not missing:
            return cached_df
        fetch_start = min(missing).isoformat()
        fetch_end = max(missing).isoformat()
    else:
        fetch_start, fetch_end = start, end

    # Fetch from Yahoo Finance
    fetched = pd.DataFrame()
    try:
        fetched = _yahoo_fx_history(fetch_start, fetch_end)
        if not fetched.empty:
            logger.info("FX from Yahoo: %d rows", len(fetched))
    except Exception as e:
        logger.warning("Yahoo FX range failed: %s", e)

    if not fetched.empty:
        save_fx_rates(fetched)

    # Combine
    if not cached_df.empty and not fetched.empty:
        result = pd.concat([cached_df, fetched]).drop_duplicates(subset="date")
    elif not cached_df.empty:
        result = cached_df
    elif not fetched.empty:
        result = fetched
    else:
        rate = get_fx_latest()
        dates = pd.bdate_range(start, end)
        result = pd.DataFrame({"date": dates.date, "rate": rate})

    return result.sort_values("date").reset_index(drop=True)
