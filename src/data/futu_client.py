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
