"""Tests for SQLite storage helpers."""

from src.storage.db import (
    _get_connection,
    add_pair,
    get_fx_spot_cached,
    get_pair,
    get_watchlist,
    init_db,
    remove_pair,
    save_fx_spot_rate,
    set_alert_pending_retry,
    upsert_alert_rule,
)


def test_init_db():
    init_db()  # should not raise


def test_add_and_get():
    init_db()
    add_pair("99999", "699999", "Test Stock")
    pair = get_pair("99999")
    assert pair is not None
    assert pair["a_code"] == "699999"
    assert pair["name"] == "Test Stock"
    # Cleanup
    remove_pair("99999")


def test_remove():
    init_db()
    add_pair("99998", "699998", "Test Remove")
    assert remove_pair("99998") is True
    assert get_pair("99998") is None


def test_watchlist():
    init_db()
    add_pair("99997", "699997", "Test WL")
    wl = get_watchlist()
    codes = [item["hk_code"] for item in wl]
    assert "99997" in codes
    remove_pair("99997")


def test_duplicate_add():
    init_db()
    add_pair("99996", "699996", "Dup Test")
    add_pair("99996", "699996", "Dup Test")  # should not raise
    remove_pair("99996")


def test_fx_spot_cache():
    init_db()
    save_fx_spot_rate("TESTUSDHKD", 7.81234)

    assert get_fx_spot_cached("TESTUSDHKD", ttl_seconds=3600) == 7.81234


def test_init_db_migrates_alert_state_retry_columns():
    init_db()
    rule_id = upsert_alert_rule("TMIGR", 1.0)

    conn = _get_connection()
    conn.execute("DROP TABLE alert_state")
    conn.execute("""
        CREATE TABLE alert_state (
            rule_id      INTEGER PRIMARY KEY REFERENCES alert_rules(id),
            last_side    TEXT,
            last_premium REAL,
            updated_at   TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

    init_db()
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(alert_state)").fetchall()}
    assert {"pending_direction", "pending_retry_after"} <= cols

    set_alert_pending_retry(rule_id, "cross_up", 123.0)
    row = conn.execute(
        "SELECT pending_direction, pending_retry_after FROM alert_state WHERE rule_id=?",
        (rule_id,),
    ).fetchone()
    assert row["pending_direction"] == "cross_up"
    assert row["pending_retry_after"] == 123.0
