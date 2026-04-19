"""Tests for src.storage.kline_cache.get_premium_history.

Covers the window-function rewrite:
  - Return shape (one row per input code, ratio_{n}d columns).
  - Ordering: ratio_1d is most recent, ratio_5d is 5th most recent, etc.
  - Missing offsets (fewer rows than requested) → None.
  - Empty inputs.
"""

from datetime import date, timedelta

import pandas as pd

from src.storage.db import init_db
from src.storage.kline_cache import get_premium_history, save_premium_daily


def _missing(v) -> bool:
    """pandas promotes None → NaN in float columns; accept both."""
    return v is None or pd.isna(v)


def _seed(hk_code: str, n_rows: int) -> None:
    """Seed premium_daily with n_rows descending-date entries for one hk_code.

    Row i (0-based) has date = (anchor - i days) and ratio_close = 1.0 + i*0.01,
    so ratio_1d = 1.00 (most recent), ratio_2d = 1.01, ..., ratio_Nd = 1.0 + (N-1)*0.01.
    """
    anchor = date(2025, 12, 31)
    rows = [
        {
            "date": (anchor - timedelta(days=i)).isoformat(),
            "ratio_close": 1.0 + i * 0.01,
            "a_turnover": 0.0,
            "h_turnover": 0.0,
            "fx_rate": 0.92,
        }
        for i in range(n_rows)
    ]
    save_premium_daily(hk_code, pd.DataFrame(rows))


def test_empty_inputs():
    init_db()
    assert get_premium_history([]).empty
    assert get_premium_history(["00001"], offsets=[]).empty


def test_single_code_full_history():
    init_db()
    _seed("91001", 10)
    df = get_premium_history(["91001"], offsets=[1, 5])
    assert list(df.columns) == ["hk_code", "ratio_1d", "ratio_5d"]
    assert len(df) == 1
    assert df.iloc[0]["hk_code"] == "91001"
    # 1d = most recent = 1.00; 5d = 5th most recent = 1.04
    assert df.iloc[0]["ratio_1d"] == 1.00
    assert df.iloc[0]["ratio_5d"] == 1.04


def test_insufficient_history_fills_none():
    init_db()
    _seed("91002", 3)
    df = get_premium_history(["91002"], offsets=[1, 5, 20])
    r = df.iloc[0]
    assert r["ratio_1d"] == 1.00
    assert _missing(r["ratio_5d"])
    assert _missing(r["ratio_20d"])


def test_multiple_codes_isolated():
    init_db()
    _seed("91003", 5)
    _seed("91004", 2)
    df = get_premium_history(["91003", "91004"], offsets=[1, 5])
    assert len(df) == 2
    r3 = df[df["hk_code"] == "91003"].iloc[0]
    r4 = df[df["hk_code"] == "91004"].iloc[0]
    assert r3["ratio_1d"] == 1.00
    assert r3["ratio_5d"] == 1.04
    assert r4["ratio_1d"] == 1.00
    assert _missing(r4["ratio_5d"])


def test_unknown_code_returns_row_with_none():
    init_db()
    df = get_premium_history(["91999"], offsets=[1, 5])
    assert len(df) == 1
    assert df.iloc[0]["hk_code"] == "91999"
    assert _missing(df.iloc[0]["ratio_1d"])
    assert _missing(df.iloc[0]["ratio_5d"])


def test_default_offsets():
    init_db()
    _seed("91005", 70)
    df = get_premium_history(["91005"])
    assert list(df.columns) == ["hk_code", "ratio_1d", "ratio_5d", "ratio_20d", "ratio_60d"]
    r = df.iloc[0]
    assert r["ratio_1d"] == 1.00
    assert round(r["ratio_60d"], 4) == round(1.0 + 59 * 0.01, 4)
