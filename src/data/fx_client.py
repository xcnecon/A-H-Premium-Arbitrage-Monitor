"""HKD/CNH exchange rate fetcher with SQLite caching.

Sources (in priority order):
  1. Yahoo Finance via yfinance — HKDCNH=X for live rate (offshore, tradeable)
  2. Yahoo Finance via yfinance — HKDCNY=X for historical range (CNH has no
     Yahoo history; CNY-CNH spread is typically < 0.1%, acceptable proxy)
  3. AKShare fx_spot_quote — live spot only (backup)

Robustness: yfinance calls go through a shared curl_cffi session with
configurable timeout (YAHOO_TIMEOUT) and optional proxy (YAHOO_PROXY_URL).
Transient failures are retried up to 2 times with backoff.
"""

import asyncio
import logging
import time
from datetime import date, datetime, timedelta

import pandas as pd

from src.config.settings import DEFAULT_FX_RATE, YAHOO_PROXY, YAHOO_TIMEOUT
from src.storage.db import get_fx_cached, get_fx_range_cached, save_fx_rates

logger = logging.getLogger(__name__)

# ─── Shared curl_cffi session for Yahoo Finance ───
# yfinance ≥ 1.0 uses curl_cffi (not requests). Reusing one session
# keeps TLS connections alive and avoids repeated TLS handshakes.

_yf_session = None


def _get_yf_session():
    """Lazily create a curl_cffi session for yfinance."""
    global _yf_session
    if _yf_session is not None:
        return _yf_session

    from curl_cffi.requests import Session

    kwargs = {"impersonate": "chrome", "timeout": YAHOO_TIMEOUT}
    if YAHOO_PROXY:
        kwargs["proxies"] = YAHOO_PROXY
        logger.info("Yahoo Finance using proxy: %s", list(YAHOO_PROXY.values())[0])

    _yf_session = Session(**kwargs)
    return _yf_session


def _clear_yf_cookies() -> None:
    """Clear yfinance's cached cookies so the next request gets a fresh session.

    Yahoo tracks rate limits via the A3 cookie. Clearing it is equivalent
    to a fresh browser — the rate limit is tied to the cookie, not the IP.
    Uses yfinance's own cache API to avoid corrupting the DB.
    """
    global _yf_session
    try:
        from yfinance.cache import get_cookie_cache

        cc = get_cookie_cache()
        # store(strategy, None) deletes the cookie row cleanly
        cc.store("curlCffi", None)
        logger.info("Cleared yfinance cookie via cache API")
    except Exception as e:
        logger.warning("Failed to clear yfinance cookies via API: %s", e)

    # Force recreate session on next call
    _yf_session = None


def _ensure_event_loop() -> None:
    """Ensure the current thread has an open asyncio event loop.

    Streamlit's ScriptRunner thread and ThreadPoolExecutor workers have no
    event loop by default.  curl_cffi (used by yfinance) needs one for
    transport cleanup — without it, ``RuntimeError: Event loop is closed``.
    """
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            asyncio.set_event_loop(asyncio.new_event_loop())
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())


def _yf_download_with_retry(ticker: str, retries: int = 2, **kwargs) -> pd.DataFrame:
    """Call yf.download with timeout, session reuse, and retry on failure.

    On 429 (rate limit), clears the cached Yahoo cookie (which carries the
    rate-limit tag) and retries once with a fresh session. If still limited,
    falls through to AKShare immediately.

    Args:
        ticker: Yahoo Finance ticker symbol.
        retries: Max retry attempts on transient failure.
        **kwargs: Passed to yf.download().

    Returns:
        DataFrame from yfinance, or empty DataFrame on failure.
    """
    _ensure_event_loop()
    import yfinance as yf

    session = _get_yf_session()
    kwargs.setdefault("progress", False)
    kwargs.setdefault("auto_adjust", False)
    kwargs.setdefault("multi_level_index", False)
    kwargs["session"] = session
    kwargs["timeout"] = int(YAHOO_TIMEOUT)

    last_err = None
    for attempt in range(1, retries + 2):  # 1 initial + N retries
        try:
            t0 = time.monotonic()
            df = yf.download(ticker, **kwargs)
            elapsed = time.monotonic() - t0
            if df is not None and not df.empty:
                if attempt > 1:
                    logger.info("Yahoo %s succeeded on attempt %d (%.1fs)", ticker, attempt, elapsed)
                return df
            # Empty result — might be transient, retry
            logger.debug("Yahoo %s returned empty on attempt %d (%.1fs)", ticker, attempt, elapsed)
        except Exception as e:
            last_err = e
            err_str = str(e)
            logger.warning("Yahoo %s attempt %d failed: %s", ticker, attempt, e)

            # 429 rate limit — clear poisoned cookie and retry once
            if "RateLimit" in err_str or "Too Many Requests" in err_str or "429" in err_str:
                _clear_yf_cookies()
                # Retry with fresh session
                session = _get_yf_session()
                kwargs["session"] = session
                try:
                    logger.info("Yahoo %s retrying with fresh cookies...", ticker)
                    df = yf.download(ticker, **kwargs)
                    if df is not None and not df.empty:
                        logger.info("Yahoo %s succeeded after cookie reset", ticker)
                        return df
                except Exception as e2:
                    logger.warning("Yahoo %s still failed after cookie reset: %s", ticker, e2)
                return pd.DataFrame()

        if attempt <= retries:
            backoff = 2.0 * attempt  # 2s, 4s
            time.sleep(backoff)

    if last_err:
        logger.warning("Yahoo %s all %d attempts failed, last error: %s", ticker, retries + 1, last_err)
    return pd.DataFrame()


# ─── Source 1: Yahoo Finance via yfinance (handles crumb/cookie auth) ───


def _yahoo_fx_history(start: str, end: str) -> pd.DataFrame:
    """Fetch daily HKD→CNH from Yahoo Finance via yfinance.

    HKDCNH=X has no historical data on Yahoo, so we use HKDCNY=X as proxy.
    The CNY-CNH spread is typically < 0.1% — acceptable for premium calculation.

    Returns DataFrame with columns: date, rate (approx CNH per 1 HKD).
    """
    # yfinance end date is exclusive, add 1 day
    end_dt = datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)
    df = _yf_download_with_retry(
        "HKDCNY=X", start=start, end=end_dt.strftime("%Y-%m-%d"),
        interval="1d",
    )
    if df.empty:
        return pd.DataFrame()
    rows = []
    for idx, row in df.iterrows():
        c = row["Close"]
        if pd.notna(c):
            rows.append({"date": idx.date(), "rate": round(float(c), 6)})
    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


def _yahoo_fx_latest() -> float | None:
    """Fetch latest HKD→CNH from Yahoo Finance via yfinance."""
    df = _yf_download_with_retry("HKDCNH=X", period="5d", interval="1d")
    if df.empty:
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

    # 2. Cache miss — fetch from network (Yahoo first — direct HK, no proxy needed)
    for name, fn in [("Yahoo", _yahoo_fx_latest), ("AKShare", _akshare_fx_spot)]:
        try:
            t0 = time.monotonic()
            rate = fn()
            elapsed = time.monotonic() - t0
            if rate and 0.5 < rate < 1.5:
                logger.info("FX latest from %s: %.5f (%.1fs)", name, rate, elapsed)
                # Persist to cache for next time
                save_fx_rates(pd.DataFrame([{"date": today, "rate": rate}]))
                return rate
            logger.warning("FX from %s returned invalid rate: %s (%.1fs)", name, rate, elapsed)
        except Exception as e:
            logger.warning("FX latest from %s failed: %s", name, e)

    # 3. Any cached value at all
    recent = get_fx_range_cached("2020-01-01", today.isoformat())
    if not recent.empty and "rate" in recent.columns:
        rate = recent.iloc[-1]["rate"]
        logger.info("FX from historical cache: %.5f", rate)
        return float(rate)

    logger.warning("All FX sources failed, using default %s", DEFAULT_FX_RATE)
    return DEFAULT_FX_RATE


def get_fx_range(start: str, end: str) -> pd.DataFrame:
    """Get daily CNH-per-HKD rates for a date range.

    Uses SQLite cache with forward-fill for missing dates (holidays, etc.).
    Only fetches from Yahoo when the cache has no data at all for the range.

    Returns:
        DataFrame with columns: date (datetime.date), rate (float)
    """
    cached_df = get_fx_range_cached(start, end)

    if not cached_df.empty:
        # Cache has data — forward-fill any gaps (holidays, weekends, today)
        # No network call needed; FX moves < 0.1%/day
        return _ffill_fx(cached_df, start, end)

    # Cache completely empty for this range — fetch once from Yahoo
    fetched = pd.DataFrame()
    try:
        fetched = _yahoo_fx_history(start, end)
        if not fetched.empty:
            logger.info("FX from Yahoo (first fetch): %d rows", len(fetched))
    except Exception as e:
        logger.warning("Yahoo FX range failed: %s", e)

    if not fetched.empty:
        save_fx_rates(fetched)
        return _ffill_fx(fetched, start, end)

    # Yahoo failed too — use latest spot rate for all dates
    rate = get_fx_latest()
    dates = pd.bdate_range(start, end)
    return pd.DataFrame({"date": dates.date, "rate": rate})


def _ffill_fx(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    """Forward-fill FX rates to cover every business day in [start, end]."""
    bdays = pd.bdate_range(start, end)
    if bdays.empty:
        return df
    full = pd.DataFrame({"date": bdays.date})
    merged = full.merge(df, on="date", how="left")
    merged["rate"] = merged["rate"].ffill().bfill()
    return merged.reset_index(drop=True)
