"""Tests for premium calculation."""

import pandas as pd

from src.calc.premium import compute_premium_pct, compute_premium_stats, compute_ratio_ohlcv


def _make_test_data(n: int = 5, fx_rate: float = 0.883):
    dates = pd.date_range("2024-01-01", periods=n)
    df_a = pd.DataFrame(
        {
            "date": dates,
            "open": [10.0] * n,
            "high": [11.0] * n,
            "low": [9.0] * n,
            "close": [10.0] * n,
            "volume": [1000] * n,
            "turnover": [10000.0] * n,
        }
    )
    df_h = pd.DataFrame(
        {
            "date": dates,
            "open": [8.0] * n,
            "high": [9.0] * n,
            "low": [7.0] * n,
            "close": [8.0] * n,
            "volume": [500] * n,
            "turnover": [4000.0] * n,
        }
    )
    df_fx = pd.DataFrame({"date": dates, "rate": [fx_rate] * n})
    return df_a, df_h, df_fx


def test_compute_ratio_basic():
    df_a, df_h, df_fx = _make_test_data()
    result = compute_ratio_ohlcv(df_a, df_h, df_fx)
    assert len(result) == 5
    assert "open" in result.columns
    assert "close" in result.columns


def test_ratio_close_value():
    df_a, df_h, df_fx = _make_test_data(fx_rate=0.883)
    result = compute_ratio_ohlcv(df_a, df_h, df_fx)
    # H_close=8, fx=0.883 (CNH per HKD), A_close=10
    # ratio = (8 * 0.883) / 10 = 7.064 / 10 = 0.7064
    assert 0.70 < result.iloc[0]["close"] < 0.72


def test_empty_input():
    df_a, _, df_fx = _make_test_data()
    empty = pd.DataFrame()
    result = compute_ratio_ohlcv(df_a, empty, df_fx)
    assert result.empty


def test_premium_pct_at_parity():
    assert compute_premium_pct(1.0) == 0.0


def test_premium_pct_h_discount():
    # ratio < 1 means H trades at discount
    pct = compute_premium_pct(0.9)
    assert pct < 0  # negative = H discount


def test_premium_pct_h_premium():
    # ratio > 1 means H trades at premium
    pct = compute_premium_pct(1.1)
    assert pct > 0  # positive = H premium


# --- compute_premium_stats tests ---

_STATS_KEYS = {"zscore", "percentile", "mean_30d", "std_30d",
               "prem_min_30d", "prem_max_30d", "prem_median_30d"}


def test_premium_stats_basic():
    """50 rows alternating around 1.05 — all expected keys present."""
    closes = [1.04 if i % 2 == 0 else 1.06 for i in range(50)]
    df = pd.DataFrame({"close": closes})
    stats = compute_premium_stats(df)
    assert set(stats.keys()) == _STATS_KEYS


def test_premium_stats_insufficient_data():
    """Only 10 rows (< default window=30) — should return empty dict."""
    df = pd.DataFrame({"close": [1.05] * 10})
    stats = compute_premium_stats(df)
    assert stats == {}


def test_premium_stats_empty_df():
    """Empty DataFrame — should return empty dict."""
    df = pd.DataFrame()
    stats = compute_premium_stats(df)
    assert stats == {}


def test_premium_stats_at_parity():
    """All close values are 1.0 (parity) — zscore should be 0."""
    df = pd.DataFrame({"close": [1.0] * 50})
    stats = compute_premium_stats(df)
    assert stats["zscore"] == 0.0
    assert 0 <= stats["percentile"] <= 100


def test_premium_stats_extreme_premium():
    """Last value much higher than the rest — zscore positive, percentile > 90."""
    closes = [1.0] * 49 + [1.50]  # last value jumps to 50% premium
    df = pd.DataFrame({"close": closes})
    stats = compute_premium_stats(df)
    assert stats["zscore"] > 0
    assert stats["percentile"] > 90
