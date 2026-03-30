"""K-line cache storage — bulk read/write for A-share and H-share daily bars."""

import logging
import sqlite3
import threading

import pandas as pd

from src.config.settings import DB_PATH

logger = logging.getLogger(__name__)

# Module-level write lock — serializes all DB writes across threads
# Reads don't need this lock (WAL mode allows concurrent readers)
_write_lock = threading.Lock()


_thread_local = threading.local()


def _get_connection() -> sqlite3.Connection:
    """Get thread-local persistent SQLite connection."""
    conn = getattr(_thread_local, "conn", None)
    if conn is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(DB_PATH), timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        _thread_local.conn = conn
    return conn


def _table_for_market(market: str) -> str:
    """Return the table name for the given market."""
    market = market.upper()
    if market == "A":
        return "kline_a"
    elif market == "H":
        return "kline_h"
    else:
        raise ValueError(f"Invalid market '{market}', must be 'A' or 'H'")


def save_kline(code: str, market: str, df: pd.DataFrame) -> int:
    """Bulk upsert K-line rows into kline_a or kline_h.

    Args:
        code: Stock code (e.g. "601939" for A, "00939" for H).
        market: 'A' or 'H'.
        df: DataFrame with columns: date, open, high, low, close, volume, turnover.

    Returns:
        Row count saved.
    """
    if df.empty:
        return 0

    table = _table_for_market(market)
    rows = [
        (
            code,
            str(row["date"]),
            float(row["open"]),
            float(row["high"]),
            float(row["low"]),
            float(row["close"]),
            int(row["volume"]),
            float(row["turnover"]),
        )
        for _, row in df.iterrows()
    ]
    with _write_lock:
        conn = _get_connection()
        conn.executemany(
            f"INSERT OR REPLACE INTO {table} "
            "(code, date, open, high, low, close, volume, turnover) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    logger.info("Saved %d %s K-line rows for %s", len(rows), market.upper(), code)
    return len(rows)


def load_kline(code: str, market: str, start: str, end: str) -> pd.DataFrame:
    """Load cached K-line from SQLite.

    Args:
        code: Stock code.
        market: 'A' or 'H'.
        start: Start date (inclusive), 'YYYY-MM-DD'.
        end: End date (inclusive), 'YYYY-MM-DD'.

    Returns:
        DataFrame with columns: date, open, high, low, close, volume, turnover.
    """
    table = _table_for_market(market)
    conn = _get_connection()
    cursor = conn.execute(
        f"SELECT date, open, high, low, close, volume, turnover "
        f"FROM {table} "
        f"WHERE code = ? AND date >= ? AND date <= ? ORDER BY date",
        (code, start, end),
    )
    rows = cursor.fetchall()
    if not rows:
        return pd.DataFrame()
    data = [dict(r) for r in rows]
    return pd.DataFrame(data)


def get_last_sync_date(code: str, market: str) -> str | None:
    """Return the last synced date from sync_meta, or None if never synced.

    Args:
        code: Stock code.
        market: 'A' or 'H'.

    Returns:
        Date string 'YYYY-MM-DD' or None.
    """
    conn = _get_connection()
    cursor = conn.execute(
        "SELECT last_date FROM sync_meta WHERE code = ? AND market = ?",
        (code, market.upper()),
    )
    row = cursor.fetchone()
    return str(row["last_date"]) if row else None


def update_sync_meta(code: str, market: str, last_date: str) -> None:
    """Update sync_meta with the latest date for this code+market.

    Args:
        code: Stock code.
        market: 'A' or 'H'.
        last_date: Most recent synced date, 'YYYY-MM-DD'.
    """
    with _write_lock:
        conn = _get_connection()
        conn.execute(
            "INSERT OR REPLACE INTO sync_meta (code, market, last_date, updated_at) "
            "VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
            (code, market.upper(), last_date),
        )
        conn.commit()
    logger.info("Updated sync_meta: %s/%s -> %s", code, market.upper(), last_date)


def save_premium_daily(hk_code: str, df: pd.DataFrame) -> int:
    """Bulk upsert premium_daily rows.

    Args:
        hk_code: HK stock code (e.g. "00939").
        df: DataFrame with columns: date, ratio_close, a_turnover, h_turnover, fx_rate.

    Returns:
        Row count saved.
    """
    if df.empty:
        return 0

    rows = [
        (
            hk_code,
            str(row["date"]),
            float(row["ratio_close"]) if pd.notna(row["ratio_close"]) else None,
            float(row["a_turnover"]) if pd.notna(row["a_turnover"]) else None,
            float(row["h_turnover"]) if pd.notna(row["h_turnover"]) else None,
            float(row["fx_rate"]) if pd.notna(row["fx_rate"]) else None,
        )
        for _, row in df.iterrows()
    ]
    with _write_lock:
        conn = _get_connection()
        conn.executemany(
            "INSERT OR REPLACE INTO premium_daily "
            "(hk_code, date, ratio_close, a_turnover, h_turnover, fx_rate) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    logger.info("Saved %d premium_daily rows for %s", len(rows), hk_code)
    return len(rows)


def get_premium_history(
    hk_codes: list[str],
    offsets: list[int] | None = None,
) -> pd.DataFrame:
    """For each hk_code, return ratio_close at the Nth most recent trading day.

    Runs individual LIMIT queries per code to leverage the (hk_code, date)
    PRIMARY KEY index for fast reverse scans, instead of a full-table
    ROW_NUMBER() window function.

    Args:
        hk_codes: List of HK stock codes.
        offsets: List of row-number offsets (1 = most recent day, 5 = 5th most recent, etc.).

    Returns:
        DataFrame with columns: hk_code, ratio_1d, ratio_5d, ratio_20d, ratio_60d
        (column names derived from offsets).
    """
    if offsets is None:
        offsets = [1, 5, 20, 60]
    if not hk_codes or not offsets:
        return pd.DataFrame()

    offset_ints = sorted(set(offsets))
    max_offset = max(offset_ints)

    sql = (
        "SELECT ratio_close FROM premium_daily "
        "WHERE hk_code = ? ORDER BY date DESC LIMIT ?"
    )

    data: list[dict] = []
    conn = _get_connection()
    for code in hk_codes:
        cursor = conn.execute(sql, (code, max_offset))
        ratios = [row["ratio_close"] for row in cursor.fetchall()]
        row_dict: dict = {"hk_code": code}
        for n in offset_ints:
            idx = n - 1  # 1-based offset to 0-based index
            row_dict[f"ratio_{n}d"] = ratios[idx] if idx < len(ratios) else None
        data.append(row_dict)

    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data)


def get_all_sync_meta() -> dict[tuple[str, str], str]:
    """Load all sync_meta rows in one query.

    Returns:
        Dict mapping (code, market) -> last_date string.
    """
    conn = _get_connection()
    cursor = conn.execute("SELECT code, market, last_date FROM sync_meta")
    return {(row["code"], row["market"]): row["last_date"] for row in cursor.fetchall()}
