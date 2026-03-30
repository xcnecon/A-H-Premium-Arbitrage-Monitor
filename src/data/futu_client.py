import logging
from datetime import datetime

import pandas as pd
from futu import RET_OK, AuType, KLType

from src.data.futu_ctx import get_quote_ctx

logger = logging.getLogger(__name__)


def get_h_kline(code: str, start: str, end: str, ktype: str = "K_DAY") -> pd.DataFrame:
    """
    Fetch H-share K-line from Futu OpenD.

    Args:
        code: HK stock code, e.g. "00939" (will be formatted as "HK.00939")
        start: Start date "YYYY-MM-DD"
        end: End date "YYYY-MM-DD"
        ktype: K-line type, default "K_DAY"

    Returns:
        DataFrame with columns: date, open, high, low, close, volume, turnover
    """
    futu_code = f"HK.{code}" if not code.startswith("HK.") else code
    kl_type = getattr(KLType, ktype, KLType.K_DAY)

    ctx = get_quote_ctx()
    try:
        all_data = []
        page_req_key = None
        while True:
            ret, data, page_req_key = ctx.request_history_kline(
                futu_code,
                start=start,
                end=end,
                ktype=kl_type,
                autype=AuType.NONE,
                max_count=1000,
                page_req_key=page_req_key,
            )
            if ret != RET_OK:
                logger.error("Futu request_history_kline failed: %s", data)
                break
            all_data.append(data)
            if page_req_key is None:
                break

        if not all_data:
            logger.warning("No data returned for %s", futu_code)
            return pd.DataFrame()

        df = pd.concat(all_data, ignore_index=True)
        # Standardize columns
        df = df.rename(
            columns={
                "time_key": "date",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "volume",
                "turnover": "turnover",
            }
        )
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df[["date", "open", "high", "low", "close", "volume", "turnover"]]
    except Exception as e:
        logger.error("Futu connection error: %s", e)
        return _fallback_akshare_hk(code, start, end)


def get_h_kline_with_ctx(
    ctx: "OpenQuoteContext",
    code: str,
    start: str,
    end: str,
) -> pd.DataFrame:
    """Fetch H-share K-line reusing an existing Futu connection.

    Same as get_h_kline but caller manages the OpenQuoteContext lifecycle.
    Used by sync.py to avoid opening/closing 169 connections.

    Args:
        ctx: An already-connected OpenQuoteContext.
        code: HK stock code, e.g. "00939".
        start: Start date "YYYY-MM-DD".
        end: End date "YYYY-MM-DD".

    Returns:
        DataFrame with columns: date, open, high, low, close, volume, turnover.
        Empty DataFrame on failure.
    """
    futu_code = f"HK.{code}" if not code.startswith("HK.") else code
    try:
        ret, data, _ = ctx.request_history_kline(
            futu_code,
            start=start,
            end=end,
            ktype=KLType.K_DAY,
            autype=AuType.NONE,
            max_count=500,
        )
        if ret != RET_OK:
            logger.warning("Futu kline (ctx) failed for %s: %s", code, str(data)[:100])
            return pd.DataFrame()

        if data.empty:
            return pd.DataFrame()

        df = pd.DataFrame(
            {
                "date": pd.to_datetime(data["time_key"]).dt.strftime("%Y-%m-%d"),
                "open": data["open"],
                "high": data["high"],
                "low": data["low"],
                "close": data["close"],
                "volume": data["volume"].astype(int),
                "turnover": data["turnover"],
            }
        )
        return df
    except Exception as e:
        logger.warning("Futu kline (ctx) error for %s: %s", code, e)
        return pd.DataFrame()


def _fallback_akshare_hk(code: str, start: str, end: str) -> pd.DataFrame:
    """Fallback to AKShare for HK stock data when OpenD unavailable."""
    try:
        import akshare as ak

        logger.info("Falling back to AKShare for HK.%s", code)
        df = ak.stock_hk_daily(symbol=code, adjust="qfq")
        if df.empty:
            return pd.DataFrame()
        # stock_hk_daily returns: date, open, high, low, close, volume, amount
        if "amount" in df.columns and "turnover" not in df.columns:
            df = df.rename(columns={"amount": "turnover"})
        if "turnover" not in df.columns:
            df["turnover"] = 0.0
        df["date"] = pd.to_datetime(df["date"]).dt.date
        start_d = datetime.strptime(start, "%Y-%m-%d").date()
        end_d = datetime.strptime(end, "%Y-%m-%d").date()
        df = df[(df["date"] >= start_d) & (df["date"] <= end_d)]
        cols = ["date", "open", "high", "low", "close", "volume", "turnover"]
        return df[cols].reset_index(drop=True)
    except Exception as e:
        logger.error("AKShare HK fallback also failed: %s", e)
        return pd.DataFrame()
