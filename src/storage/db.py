import logging
import sqlite3
import threading
from datetime import datetime

import pandas as pd

from src.config.settings import DB_PATH
from src.storage.kline_cache import _write_lock

logger = logging.getLogger(__name__)


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


def init_db() -> None:
    """Initialize the database schema."""
    conn = _get_connection()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            hk_code TEXT PRIMARY KEY,
            a_code  TEXT NOT NULL,
            name    TEXT NOT NULL,
            added_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fx_rates (
            date TEXT PRIMARY KEY,
            rate REAL NOT NULL,
            source TEXT DEFAULT 'api',
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS kline_a (
            code TEXT NOT NULL, date TEXT NOT NULL,
            open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
            volume INTEGER NOT NULL, turnover REAL NOT NULL,
            PRIMARY KEY (code, date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS kline_h (
            code TEXT NOT NULL, date TEXT NOT NULL,
            open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
            volume INTEGER NOT NULL, turnover REAL NOT NULL,
            PRIMARY KEY (code, date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS premium_daily (
            hk_code TEXT NOT NULL, date TEXT NOT NULL,
            ratio_close REAL, a_turnover REAL, h_turnover REAL, fx_rate REAL,
            PRIMARY KEY (hk_code, date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_meta (
            code TEXT NOT NULL, market TEXT NOT NULL,
            last_date TEXT NOT NULL, updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, market)
        )
    """)
    # ── Migrate old alert schema (direction-based) to crossover schema ──
    cursor = conn.execute("PRAGMA table_info(alert_rules)")
    old_cols = {row[1] for row in cursor.fetchall()}
    if "direction" in old_cols:
        logger.info("Migrating alert tables to crossover schema")
        conn.execute("DROP TABLE IF EXISTS alert_history")
        conn.execute("DROP TABLE IF EXISTS alert_state")
        conn.execute("DROP TABLE IF EXISTS alert_rules")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS alert_rules (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            hk_code       TEXT NOT NULL,
            threshold     REAL NOT NULL,
            enabled       INTEGER NOT NULL DEFAULT 1,
            created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(hk_code, threshold)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alert_state (
            rule_id         INTEGER PRIMARY KEY REFERENCES alert_rules(id),
            last_side       TEXT,
            last_premium    REAL,
            updated_at      TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alert_history (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id        INTEGER NOT NULL,
            hk_code        TEXT NOT NULL,
            direction      TEXT NOT NULL,
            event          TEXT NOT NULL,
            premium_value  REAL,
            detail         TEXT,
            created_at     TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_alert_history_time
        ON alert_history(created_at DESC)
    """)
    conn.commit()
    logger.info("Database initialized at %s", DB_PATH)


def add_pair(hk_code: str, a_code: str, name: str) -> bool:
    """
    Add an A/H pair to the watchlist.

    Args:
        hk_code: HK stock code (e.g. "00939")
        a_code: A-share code (e.g. "601939")
        name: Stock name

    Returns:
        True if added, False if already exists
    """
    conn = _get_connection()
    conn.execute(
        "INSERT OR IGNORE INTO watchlist (hk_code, a_code, name) VALUES (?, ?, ?)",
        (hk_code, a_code, name),
    )
    conn.commit()
    added = conn.total_changes > 0
    if added:
        logger.info("Added to watchlist: %s / %s (%s)", hk_code, a_code, name)
    return added


def remove_pair(hk_code: str) -> bool:
    """
    Remove an A/H pair from the watchlist.

    Args:
        hk_code: HK stock code to remove

    Returns:
        True if removed, False if not found
    """
    conn = _get_connection()
    cursor = conn.execute(
        "DELETE FROM watchlist WHERE hk_code = ?",
        (hk_code,),
    )
    conn.commit()
    removed = cursor.rowcount > 0
    if removed:
        logger.info("Removed from watchlist: %s", hk_code)
    return removed


def get_watchlist() -> list[dict]:
    """
    Get all pairs in the watchlist.

    Returns:
        List of dicts with keys: hk_code, a_code, name, added_at
    """
    conn = _get_connection()
    cursor = conn.execute(
        "SELECT hk_code, a_code, name, added_at FROM watchlist ORDER BY added_at"
    )
    return [dict(row) for row in cursor.fetchall()]


def get_pair(hk_code: str) -> dict | None:
    """
    Get a specific pair from the watchlist.

    Args:
        hk_code: HK stock code

    Returns:
        Dict with pair info, or None if not found
    """
    conn = _get_connection()
    cursor = conn.execute(
        "SELECT hk_code, a_code, name, added_at FROM watchlist WHERE hk_code = ?",
        (hk_code,),
    )
    row = cursor.fetchone()
    return dict(row) if row else None


def save_fx_rates(df: pd.DataFrame) -> int:
    """Save FX rates to cache. df must have columns: date, rate."""
    rows = [(str(row["date"]), float(row["rate"])) for _, row in df.iterrows()]
    with _write_lock:
        conn = _get_connection()
        for d, r in rows:
            conn.execute(
                "INSERT OR REPLACE INTO fx_rates (date, rate) VALUES (?, ?)",
                (d, r),
            )
        conn.commit()
    return len(rows)


def get_fx_cached(date_str: str) -> float | None:
    """Get a single cached FX rate by date string 'YYYY-MM-DD'."""
    conn = _get_connection()
    cursor = conn.execute("SELECT rate FROM fx_rates WHERE date = ?", (date_str,))
    row = cursor.fetchone()
    return float(row["rate"]) if row else None


def get_fx_range_cached(start: str, end: str) -> pd.DataFrame:
    """Get cached FX rates for a date range. Returns DataFrame with date, rate."""
    conn = _get_connection()
    cursor = conn.execute(
        "SELECT date, rate FROM fx_rates WHERE date >= ? AND date <= ? ORDER BY date",
        (start, end),
    )
    rows = cursor.fetchall()
    if not rows:
        return pd.DataFrame()
    data = [
        {"date": datetime.strptime(r["date"], "%Y-%m-%d").date(), "rate": r["rate"]}
        for r in rows
    ]
    return pd.DataFrame(data)


# ---------------------------------------------------------------------------
# Alert rules, state, and history
# ---------------------------------------------------------------------------


def upsert_alert_rule(hk_code: str, threshold: float) -> int:
    """Create a crossover alert rule.  Max 3 per stock.

    Returns:
        Rule id on success, -1 if the per-stock limit is reached.
    """
    conn = _get_connection()
    # Already exists?
    cursor = conn.execute(
        "SELECT id FROM alert_rules WHERE hk_code=? AND threshold=?",
        (hk_code, threshold),
    )
    row = cursor.fetchone()
    if row:
        return row["id"]

    # Enforce max 3 per stock
    cursor = conn.execute(
        "SELECT COUNT(*) AS cnt FROM alert_rules WHERE hk_code=? AND enabled=1",
        (hk_code,),
    )
    if cursor.fetchone()["cnt"] >= 3:
        logger.warning("Max 3 alert rules per stock (%s)", hk_code)
        return -1

    conn.execute(
        "INSERT INTO alert_rules (hk_code, threshold) VALUES (?, ?)",
        (hk_code, threshold),
    )
    conn.commit()
    cursor = conn.execute(
        "SELECT id FROM alert_rules WHERE hk_code=? AND threshold=?",
        (hk_code, threshold),
    )
    rule_id = cursor.fetchone()["id"]
    conn.execute(
        "INSERT OR IGNORE INTO alert_state (rule_id) VALUES (?)",
        (rule_id,),
    )
    conn.commit()
    logger.info("Created alert rule: %s @ %.2f%%", hk_code, threshold)
    return rule_id


def delete_alert_rule(hk_code: str, threshold: float) -> bool:
    """Delete an alert rule by hk_code and threshold. Returns True if deleted."""
    conn = _get_connection()
    cursor = conn.execute(
        "SELECT id FROM alert_rules WHERE hk_code=? AND threshold=?",
        (hk_code, threshold),
    )
    row = cursor.fetchone()
    if not row:
        return False
    rule_id = row["id"]
    conn.execute("DELETE FROM alert_state WHERE rule_id=?", (rule_id,))
    conn.execute("DELETE FROM alert_rules WHERE id=?", (rule_id,))
    conn.commit()
    logger.info("Deleted alert rule: %s @ %.2f%%", hk_code, threshold)
    return True


def get_alert_rules(hk_code: str | None = None) -> list[dict]:
    """Get enabled alert rules with crossover state, optionally filtered."""
    conn = _get_connection()
    if hk_code:
        cursor = conn.execute(
            """SELECT r.*, s.last_side, s.last_premium
               FROM alert_rules r
               LEFT JOIN alert_state s ON r.id = s.rule_id
               WHERE r.hk_code=? AND r.enabled=1
               ORDER BY r.threshold""",
            (hk_code,),
        )
    else:
        cursor = conn.execute(
            """SELECT r.*, s.last_side, s.last_premium
               FROM alert_rules r
               LEFT JOIN alert_state s ON r.id = s.rule_id
               WHERE r.enabled=1
               ORDER BY r.hk_code, r.threshold"""
        )
    return [dict(row) for row in cursor.fetchall()]


def update_alert_state(
    rule_id: int, last_side: str | None = None, last_premium: float | None = None
) -> None:
    """Update alert crossover state (last_side and/or last_premium)."""
    conn = _get_connection()
    sets = ["updated_at=CURRENT_TIMESTAMP"]
    vals: list = []
    if last_side is not None:
        sets.append("last_side=?")
        vals.append(last_side)
    if last_premium is not None:
        sets.append("last_premium=?")
        vals.append(last_premium)
    vals.append(rule_id)
    conn.execute(
        f"UPDATE alert_state SET {', '.join(sets)} WHERE rule_id=?",
        vals,
    )
    conn.commit()


def log_alert_event(
    rule_id: int,
    hk_code: str,
    direction: str,
    event: str,
    premium_value: float,
    detail: str | None = None,
) -> None:
    """Append to alert audit log."""
    conn = _get_connection()
    conn.execute(
        """INSERT INTO alert_history (rule_id, hk_code, direction, event, premium_value, detail)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (rule_id, hk_code, direction, event, premium_value, detail),
    )
    conn.commit()


def get_alert_history(limit: int = 20) -> list[dict]:
    """Get recent alert history entries."""
    conn = _get_connection()
    cursor = conn.execute(
        "SELECT * FROM alert_history ORDER BY created_at DESC LIMIT ?",
        (limit,),
    )
    return [dict(row) for row in cursor.fetchall()]


def get_all_alert_rules_with_state() -> list[dict]:
    """Get ALL alert rules (enabled or not) with their current crossover state."""
    conn = _get_connection()
    cursor = conn.execute(
        """SELECT r.*, s.last_side, s.last_premium
           FROM alert_rules r
           LEFT JOIN alert_state s ON r.id = s.rule_id
           ORDER BY r.hk_code, r.threshold"""
    )
    return [dict(row) for row in cursor.fetchall()]
