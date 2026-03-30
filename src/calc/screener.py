"""Screening metrics for all A/H dual-listed pairs.

Computes real-time premium, daily premium change, and volume ratio
for every pair in the mapping, using live snapshot data and FX rates.
"""

import logging

import pandas as pd
from futu import RET_OK

from src.data.futu_ctx import get_quote_ctx
from src.data.ah_mapping import get_all_pairs
from src.data.fx_client import get_fx_latest
from src.data.realtime import get_a_snapshots_batch

logger = logging.getLogger(__name__)


def _safe_premium(h_price: float, a_price: float, fx: float) -> float | None:
    """Compute H-share premium percentage, returning None on bad inputs.

    Formula: (H_price * fx / A_price - 1) * 100

    Args:
        h_price: H-share price in HKD.
        a_price: A-share price in CNY.
        fx: CNH per 1 HKD (~0.92).

    Returns:
        Premium percentage, or None if inputs are zero/negative.
    """
    if not h_price or not a_price or h_price <= 0 or a_price <= 0 or fx <= 0:
        return None
    return (h_price * fx / a_price - 1.0) * 100.0


def _safe_vol_ratio(h_turnover: float, a_turnover: float, fx: float) -> float | None:
    """Compute H/A turnover ratio in CNH terms, returning None on bad inputs.

    Args:
        h_turnover: H-share turnover in HKD.
        a_turnover: A-share turnover in CNY.
        fx: CNH per 1 HKD (~0.92).

    Returns:
        Turnover ratio, or None if A-share turnover is zero/missing.
    """
    if not a_turnover or a_turnover <= 0 or fx <= 0:
        return None
    if h_turnover is None or h_turnover < 0:
        return None
    return (h_turnover * fx) / a_turnover


def _fetch_all_h_snapshots(hk_codes: list[str], chunk_size: int = 400) -> dict[str, dict]:
    """Fetch H-share snapshots using the singleton Futu connection, chunked to isolate bad codes."""
    ctx = get_quote_ctx()
    result: dict[str, dict] = {}
    try:
        for i in range(0, len(hk_codes), chunk_size):
            chunk = hk_codes[i : i + chunk_size]
            futu_codes = [f"HK.{c}" for c in chunk]
            ret, data = ctx.get_market_snapshot(futu_codes)
            if ret == RET_OK:
                for _, row in data.iterrows():
                    code = str(row["code"]).replace("HK.", "")
                    result[code] = {
                        "price": float(row["last_price"]),
                        "prev_close": float(row["prev_close_price"]),
                        "open": float(row["open_price"]),
                        "high": float(row["high_price"]),
                        "low": float(row["low_price"]),
                        "volume": int(row["volume"]),
                        "turnover": float(row["turnover"]),
                    }
            else:
                logger.warning("Futu chunk %d–%d failed: %s", i, i + len(chunk), str(data)[:100])
    except Exception as e:
        logger.error("Futu screener fetch error: %s", e)
    logger.info("Fetched %d/%d H-share snapshots", len(result), len(hk_codes))
    return result


def compute_screener_table() -> pd.DataFrame:
    """Compute screening metrics for all A/H pairs.

    Fetches live snapshots for every pair, computes premium, daily premium
    change, and volume ratio, then returns a sorted DataFrame.

    Returns:
        DataFrame with columns: hk_code, a_code, name, premium, daily_chg,
        vol_ratio — sorted by premium descending.  Pairs with missing or
        zero price data are excluded.
    """
    pairs = get_all_pairs()
    if not pairs:
        logger.warning("No A/H pairs loaded")
        return pd.DataFrame(
            columns=["hk_code", "a_code", "name", "premium", "daily_chg", "vol_ratio"]
        )

    fx = get_fx_latest()
    logger.info("Screener FX rate: %.5f CNH/HKD", fx)

    hk_codes = list(pairs.keys())
    a_codes = [pairs[hk]["a_code"] for hk in hk_codes]

    # Single Futu connection, chunked calls — avoids rate-limit on rapid reconnects
    # and isolates bad stock codes to their own chunk.
    h_snaps = _fetch_all_h_snapshots(hk_codes)
    if not h_snaps:
        logger.error("All H-share snapshots failed — Futu OpenD may be disconnected")

    a_snaps = get_a_snapshots_batch(a_codes)

    logger.info(
        "Screener snapshots: %d/%d H, %d/%d A",
        len(h_snaps),
        len(hk_codes),
        len(a_snaps),
        len(a_codes),
    )

    rows: list[dict] = []
    for hk_code in hk_codes:
        info = pairs[hk_code]
        a_code = info["a_code"]
        name = info.get("name", "")

        h_snap = h_snaps.get(hk_code)
        a_snap = a_snaps.get(a_code)

        # Skip if either snapshot is entirely missing
        if h_snap is None or a_snap is None:
            continue

        h_price = h_snap.get("price", 0)
        a_price = a_snap.get("price", 0)

        # Skip pairs with zero or missing current price
        if not h_price or not a_price or h_price <= 0 or a_price <= 0:
            continue

        premium = _safe_premium(h_price, a_price, fx)

        # Daily change: compare current premium to previous-close premium
        h_prev = h_snap.get("prev_close", 0)
        a_prev = a_snap.get("prev_close", 0)
        prev_premium = _safe_premium(h_prev, a_prev, fx)

        daily_chg: float | None = None
        if premium is not None and prev_premium is not None:
            daily_chg = premium - prev_premium

        h_turnover = h_snap.get("turnover", 0)
        a_turnover = a_snap.get("turnover", 0)
        vol_ratio = _safe_vol_ratio(h_turnover, a_turnover, fx)

        rows.append(
            {
                "hk_code": hk_code,
                "a_code": a_code,
                "name": name,
                "premium": round(premium, 2) if premium is not None else None,
                "daily_chg": round(daily_chg, 2) if daily_chg is not None else None,
                "vol_ratio": round(vol_ratio, 4) if vol_ratio is not None else None,
            }
        )

    df = pd.DataFrame(
        rows,
        columns=[
            "hk_code",
            "a_code",
            "name",
            "premium",
            "daily_chg",
            "vol_ratio",
        ],
    )

    if not df.empty:
        df = df.sort_values("premium", ascending=False, na_position="last")
        df = df.reset_index(drop=True)

    logger.info("Screener table: %d pairs with data out of %d total", len(df), len(pairs))
    return df
