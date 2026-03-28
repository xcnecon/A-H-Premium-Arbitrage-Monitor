import logging
import pandas as pd

from src.config.settings import DEFAULT_FX_RATE

logger = logging.getLogger(__name__)


def compute_ratio_ohlcv(
    df_a: pd.DataFrame,
    df_h: pd.DataFrame,
    df_fx: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute H/A price ratio OHLCV from A-share, H-share, and FX data.

    The ratio represents how many units of A-share value one H-share is worth
    (in CNY terms). Ratio > 1 means H-share trades at a premium.

    Args:
        df_a: A-share K-line with columns: date, open, high, low, close, volume, turnover
        df_h: H-share K-line with columns: date, open, high, low, close, volume, turnover
        df_fx: FX rates with columns: date, rate (CNH per 1 HKD, ≈0.92)

    Returns:
        DataFrame with columns: date, open, high, low, close, a_turnover, h_turnover
    """
    if df_a.empty or df_h.empty:
        logger.warning("Empty input dataframe(s)")
        return pd.DataFrame()

    # Ensure date columns are the same type
    df_a = df_a.copy()
    df_h = df_h.copy()
    df_a["date"] = pd.to_datetime(df_a["date"])
    df_h["date"] = pd.to_datetime(df_h["date"])

    # Inner join A and H on date (only overlapping trading days)
    merged = pd.merge(df_a, df_h, on="date", suffixes=("_a", "_h"))

    if merged.empty:
        logger.warning("No overlapping dates between A and H data")
        return pd.DataFrame()

    # Handle FX rates
    if df_fx.empty or len(df_fx) == 0:
        logger.warning("No FX data, using default rate %.4f", DEFAULT_FX_RATE)
        merged["fx"] = DEFAULT_FX_RATE
    else:
        df_fx = df_fx.copy()
        df_fx["date"] = pd.to_datetime(df_fx["date"])
        # Merge FX on date, forward-fill missing dates
        merged = pd.merge(merged, df_fx[["date", "rate"]], on="date", how="left")
        merged["rate"] = merged["rate"].ffill().bfill().fillna(DEFAULT_FX_RATE)
        merged.rename(columns={"rate": "fx"}, inplace=True)

    # Filter out rows where A-share prices are zero (would cause division by zero)
    valid = (
        (merged["open_a"] > 0) & (merged["close_a"] > 0)
        & (merged["high_a"] > 0) & (merged["low_a"] > 0)
    )
    merged = merged[valid]
    if merged.empty:
        logger.warning("No valid A-share prices after filtering zeros")
        return pd.DataFrame()

    # Compute ratio OHLC
    # fx ≈ 0.917 = CNH per 1 HKD
    # To convert HKD to CNH: H_CNH = H_HKD * fx
    # H/A ratio = H_CNH / A_CNY = (H_HKD * fx) / A_CNY

    result = pd.DataFrame()
    result["date"] = merged["date"]
    result["open"] = (merged["open_h"] * merged["fx"]) / merged["open_a"]
    result["close"] = (merged["close_h"] * merged["fx"]) / merged["close_a"]
    # High ratio ≈ max(H/A) ≈ (H_high * fx) / A_low
    result["high"] = (merged["high_h"] * merged["fx"]) / merged["low_a"]
    # Low ratio ≈ min(H/A) ≈ (H_low * fx) / A_high
    result["low"] = (merged["low_h"] * merged["fx"]) / merged["high_a"]

    # Volume/turnover: convert A turnover to HKD (divide by fx)
    result["a_turnover"] = merged["turnover_a"].fillna(0).astype(float) / merged["fx"]
    result["h_turnover"] = merged["turnover_h"].fillna(0).astype(float)

    result = result.sort_values("date").reset_index(drop=True)

    if not result.empty:
        logger.info("Computed ratio OHLCV: %d rows, date range %s to %s", len(result), result['date'].iloc[0], result['date'].iloc[-1])
    return result


def compute_premium_pct(ratio: float) -> float:
    """
    Convert H/A ratio to H-share premium percentage.

    Premium % = (ratio - 1) * 100
    When ratio > 1, H-share trades at a premium (positive %)
    When ratio < 1, H-share trades at a discount (negative %)

    Args:
        ratio: H/A price ratio (H_CNH / A_CNY)

    Returns:
        H-share premium percentage
    """
    if ratio <= 0:
        return 0.0
    return (ratio - 1.0) * 100.0


def compute_premium_stats(df_ratio: pd.DataFrame, window: int = 30) -> dict:
    """Compute statistical metrics for premium analysis.

    Args:
        df_ratio: DataFrame with 'close' column (ratio close values)
        window: Rolling window in trading days (default 30)

    Returns:
        dict with keys: zscore, percentile, mean_30d, std_30d,
                        prem_min_30d, prem_max_30d, prem_median_30d
        Returns empty dict if insufficient data.
    """
    if df_ratio.empty or "close" not in df_ratio.columns or len(df_ratio) < window:
        return {}

    closes = df_ratio["close"].dropna()
    if len(closes) < window:
        return {}

    # Convert ratio to premium %
    prem = (closes - 1) * 100

    current = prem.iloc[-1]
    rolling_mean = prem.rolling(window).mean().iloc[-1]
    rolling_std = prem.rolling(window).std().iloc[-1]

    if pd.isna(rolling_std) or rolling_std == 0:
        zscore = 0.0
    else:
        zscore = (current - rolling_mean) / rolling_std

    # Percentile over full history
    percentile = (prem < current).sum() / len(prem) * 100

    return {
        "zscore": round(zscore, 2),
        "percentile": round(percentile, 1),
        "mean_30d": round(rolling_mean, 2),
        "std_30d": round(rolling_std, 2),
        "prem_min_30d": round(prem.tail(window).min(), 2),
        "prem_max_30d": round(prem.tail(window).max(), 2),
        "prem_median_30d": round(prem.tail(window).median(), 2),
    }
