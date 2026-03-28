"""Tests for SQLite watchlist."""

from src.storage.db import add_pair, get_pair, get_watchlist, init_db, remove_pair


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
