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
import threading
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

# ─── Cross-call coordination ───
# Without these, the screener (TTL 25s), live fragment (every 5s) and chart
# panel can all hit a cold cache simultaneously and each fire its own 30s
# Yahoo retry chain — flooding logs and stalling the UI for ~minute.
_fx_fetch_lock = threading.Lock()
_yahoo_cooldown_until = 0.0  # epoch seconds; skip Yahoo entirely while in cooldown
_YAHOO_COOLDOWN_AFTER_429 = 300.0  # 5 minutes — long enough for Yahoo's per-cookie limit to relax

# In-memory short-circuit for fallback values. We do NOT persist fallback rates
# to the SQLite cache: doing so poisons subsequent days, because the next call
# finds yesterday's fake-fallback row via _check_cache_today_or_yesterday and
# never tries the network again. Instead, when all sources fail we cache the
# fallback value here for a short window so concurrent fragment refreshes in
# the same process don't each fire the full probe chain.
_fallback_value_until = 0.0
_fallback_value: float | None = None
_FALLBACK_TTL = 300.0  # 5 minutes


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


_YF_RETRIES = 2  # 1 initial + 2 retries on transient empty / non-429 failure


def _yahoo_in_cooldown() -> bool:
    """True if Yahoo is currently being skipped after a recent 429."""
    if time.time() < _yahoo_cooldown_until:
        remaining = int(_yahoo_cooldown_until - time.time())
        logger.debug("Yahoo in cooldown (%ds left) — skipping", remaining)
        return True
    return False


def _yf_download(ticker: str, **kwargs) -> pd.DataFrame:
    """Call yf.download with timeout, session reuse, retry, and 429 cooldown.

    Returns empty DataFrame and engages a global 5-min cooldown on rate-limit
    so concurrent callers (live fragment 5s, screener 25s) don't keep hammering
    Yahoo with the same poisoned cookie.
    """
    if _yahoo_in_cooldown():
        return pd.DataFrame()

    _ensure_event_loop()
    import yfinance as yf

    session = _get_yf_session()
    kwargs.setdefault("progress", False)
    kwargs.setdefault("auto_adjust", False)
    kwargs.setdefault("multi_level_index", False)
    kwargs["session"] = session
    kwargs["timeout"] = int(YAHOO_TIMEOUT)

    last_err = None
    for attempt in range(1, _YF_RETRIES + 2):
        try:
            t0 = time.monotonic()
            df = yf.download(ticker, **kwargs)
            elapsed = time.monotonic() - t0
            if df is not None and not df.empty:
                if attempt > 1:
                    logger.info("Yahoo %s succeeded on attempt %d (%.1fs)", ticker, attempt, elapsed)
                return df
            logger.debug("Yahoo %s returned empty on attempt %d (%.1fs)", ticker, attempt, elapsed)
        except Exception as e:
            last_err = e
            err_str = str(e)
            logger.warning("Yahoo %s attempt %d failed: %s", ticker, attempt, e)
            if "RateLimit" in err_str or "Too Many Requests" in err_str or "429" in err_str:
                global _yahoo_cooldown_until
                _yahoo_cooldown_until = time.time() + _YAHOO_COOLDOWN_AFTER_429
                logger.warning(
                    "Yahoo cooldown engaged for %.0fs after rate limit",
                    _YAHOO_COOLDOWN_AFTER_429,
                )
                return pd.DataFrame()

        if attempt <= _YF_RETRIES:
            time.sleep(2.0 * attempt)  # 2s, 4s

    if last_err:
        logger.warning("Yahoo %s all attempts failed, last error: %s", ticker, last_err)
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
    df = _yf_download(
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
    df = _yf_download("HKDCNH=X", period="5d", interval="1d")
    if df.empty:
        return None
    for c in reversed(df["Close"].tolist()):
        if pd.notna(c):
            return round(float(c), 6)
    return None


def get_usd_hkd_latest() -> float:
    """Get the latest USD→HKD rate (HKD per 1 USD, ~7.80 peg band 7.75–7.85).

    Source: Yahoo Finance ``HKD=X``. Falls back to 7.80 (peg center) if Yahoo
    is unreachable. Rate moves <0.02% per day — cache at the caller.
    """
    try:
        df = _yf_download("HKD=X", period="5d", interval="1d")
        if not df.empty:
            for c in reversed(df["Close"].tolist()):
                if pd.notna(c) and 7.70 < float(c) < 7.90:
                    return round(float(c), 5)
    except Exception as e:
        logger.warning("USD/HKD fetch failed: %s", e)
    return 7.80


# ─── Source 2: AKShare fx_spot_quote (live fallback) ───


def _akshare_fx_spot() -> float | None:
    """Fetch live HKD→CNH spot from AKShare (Bank of China feed).

    BOC reports HKD/CNY (onshore) — used as a CNH proxy since the
    onshore/offshore spread is typically < 0.1%. Schema:
        货币对 | 买报价 | 卖报价   (currency pair | bid | ask)

    Source blocks non-CN IPs, so we route through ``A_SHARE_PROXY_URL`` if
    configured (same proxy used by the other AKShare A-share endpoints).
    """
    try:
        import akshare as ak

        from src.data.akshare_client import _a_share_proxy_env

        with _a_share_proxy_env():
            df = ak.fx_spot_quote()
        if df is None or df.empty:
            return None
        for _, row in df.iterrows():
            pair = str(row.get("货币对", "")).upper()
            if "HKD" not in pair or ("CNY" not in pair and "CNH" not in pair):
                continue
            bid = row.get("买报价")
            ask = row.get("卖报价")
            try:
                bid_f, ask_f = float(bid), float(ask)
            except (ValueError, TypeError):
                continue
            mid = (bid_f + ask_f) / 2.0
            if 0.5 < mid < 1.5:
                return round(mid, 6)
    except Exception as e:
        logger.warning("AKShare FX spot failed: %s", e)
    return None


# ─── Public API ───


def _check_cache_today_or_yesterday(today: date) -> float | None:
    for d in [today, today - timedelta(days=1)]:
        cached = get_fx_cached(d.isoformat())
        if cached is not None:
            logger.debug("FX from cache (%s): %.5f", d, cached)
            return cached
    return None


def get_fx_latest() -> float:
    """Get the latest CNH-per-HKD rate.

    Priority: SQLite cache (instant) → network fetch (slow, only if stale).
    The rate moves <0.1% per day, so a cached value from today or yesterday is fine.

    Concurrent callers are coalesced via ``_fx_fetch_lock``: only the first
    thread does the network round-trip; subsequent threads re-check the cache
    after acquiring the lock and almost always hit it. Without this, the
    screener (TTL 25s), live fragment (every 5s) and chart panel can each
    fire their own 30s Yahoo retry chain in parallel.

    Returns:
        CNH per 1 HKD (e.g., 0.92)
    """
    today = date.today()
    cached = _check_cache_today_or_yesterday(today)
    if cached is not None:
        return cached

    with _fx_fetch_lock:
        # Re-check inside the lock — another thread may have populated it.
        cached = _check_cache_today_or_yesterday(today)
        if cached is not None:
            return cached

        # In-memory fallback short-circuit. If we recently fell through to a
        # fallback within the same process, return it directly instead of
        # re-running the full probe chain on every fragment refresh.
        global _fallback_value_until, _fallback_value
        if _fallback_value is not None and time.time() < _fallback_value_until:
            return _fallback_value

        # Cache miss — fetch from network. _yahoo_fx_latest self-skips when
        # in cooldown so we don't retry every few seconds after a 429.
        for name, fn in [("Yahoo", _yahoo_fx_latest), ("AKShare", _akshare_fx_spot)]:
            try:
                t0 = time.monotonic()
                rate = fn()
                elapsed = time.monotonic() - t0
                if rate and 0.5 < rate < 1.5:
                    logger.info("FX latest from %s: %.5f (%.1fs)", name, rate, elapsed)
                    save_fx_rates(pd.DataFrame([{"date": today, "rate": rate}]))
                    return rate
                logger.debug("FX from %s returned invalid rate: %s (%.1fs)", name, rate, elapsed)
            except Exception as e:
                logger.warning("FX latest from %s failed: %s", name, e)

        # All live sources failed — fall through to the most recent historical
        # rate. Cache only in memory: persisting the fallback to today's slot
        # would make tomorrow's _check_cache_today_or_yesterday hit it and skip
        # the network entirely, freezing the rate for as long as the app keeps
        # running (the bug we used to ship with).
        recent = get_fx_range_cached("2020-01-01", today.isoformat())
        if not recent.empty and "rate" in recent.columns:
            rate = float(recent.iloc[-1]["rate"])
            logger.info("FX from historical cache: %.5f (in-memory only)", rate)
        else:
            rate = DEFAULT_FX_RATE
            logger.warning("All FX sources failed, using default %s", DEFAULT_FX_RATE)

        _fallback_value = rate
        _fallback_value_until = time.time() + _FALLBACK_TTL
        return rate


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
