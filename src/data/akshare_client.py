import logging
import time
from datetime import datetime

import pandas as pd
import akshare as ak

logger = logging.getLogger(__name__)

MAX_RETRIES = 2
RETRY_DELAY = 2.0


def _with_retry(func, *args, **kwargs):
    """Retry wrapper for AKShare calls."""
    for attempt in range(MAX_RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt < MAX_RETRIES:
                logger.warning("Retry %d/%d for %s: %s", attempt + 1, MAX_RETRIES, func.__name__, e)
                time.sleep(RETRY_DELAY)
            else:
                raise


def _a_code_to_tx_symbol(a_code: str) -> str:
    """Convert A-share code to Tencent format (sh600519 / sz000001)."""
    if a_code.startswith(("6", "9")):
        return f"sh{a_code}"
    else:
        return f"sz{a_code}"


def get_a_kline(code: str, start: str, end: str, adjust: str = "") -> pd.DataFrame:
    """
    Fetch A-share K-line from AKShare (Tencent source).

    Args:
        code: A-share code, e.g. "601939"
        start: Start date "YYYY-MM-DD"
        end: End date "YYYY-MM-DD"
        adjust: "qfq" (forward), "hfq" (backward), "" (none)

    Returns:
        DataFrame with columns: date, open, high, low, close, volume, turnover
    """
    symbol = _a_code_to_tx_symbol(code)
    df = _with_retry(
        ak.stock_zh_a_hist_tx,
        symbol=symbol,
        start_date=start.replace("-", ""),
        end_date=end.replace("-", ""),
        adjust=adjust
    )
    if df is None or df.empty:
        logger.warning("No A-share data for %s", code)
        return pd.DataFrame()

    # AKShare tx returns English columns: date, open, close, high, low, amount
    # Or sometimes Chinese: 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额
    col_map = {}
    for col in df.columns:
        lc = col.lower()
        if col in ("日期",) or lc == "date":
            col_map[col] = "date"
        elif col in ("开盘",) or lc == "open":
            col_map[col] = "open"
        elif col in ("最高",) or lc == "high":
            col_map[col] = "high"
        elif col in ("最低",) or lc == "low":
            col_map[col] = "low"
        elif col in ("收盘",) or lc == "close":
            col_map[col] = "close"
        elif col in ("成交量",) or lc == "volume":
            col_map[col] = "volume"
        elif col in ("成交额",) or lc == "turnover":
            col_map[col] = "turnover"
        elif lc == "amount":
            # Tencent source: 'amount' is trading volume in lots (手)
            col_map[col] = "volume"

    df = df.rename(columns=col_map)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # A-share volume from Tencent is in lots (手, 1手=100股), convert to shares
    if "volume" in df.columns:
        df["volume"] = df["volume"] * 100

    # Ensure volume and turnover columns exist
    if "volume" not in df.columns:
        df["volume"] = 0.0
    if "turnover" not in df.columns:
        # Estimate turnover as volume(shares) * close price
        df["turnover"] = df["volume"].astype(float) * df["close"].astype(float)

    return df[["date", "open", "high", "low", "close", "volume", "turnover"]].reset_index(drop=True)
