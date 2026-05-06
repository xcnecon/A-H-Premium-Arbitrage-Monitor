import pandas as pd

from src.data import fx_client
from src.storage.db import _get_connection, init_db, save_fx_spot_rate


def _clear_usd_hkd_cache() -> None:
    conn = _get_connection()
    conn.execute("DELETE FROM fx_spot_cache WHERE symbol = 'USDHKD'")
    conn.commit()


def test_usd_hkd_uses_sqlite_cache(monkeypatch):
    init_db()
    _clear_usd_hkd_cache()
    save_fx_spot_rate("USDHKD", 7.81234)

    def _raise_if_called(*args, **kwargs):
        raise AssertionError("yfinance should not be called on fresh USDHKD cache")

    monkeypatch.setattr(fx_client, "_yf_download", _raise_if_called)

    assert fx_client.get_usd_hkd_latest() == 7.81234


def test_usd_hkd_fetches_once_then_uses_sqlite_cache(monkeypatch):
    init_db()
    _clear_usd_hkd_cache()
    calls: list[str] = []

    def _fake_download(ticker, **kwargs):
        calls.append(ticker)
        return pd.DataFrame({"Close": [7.82345]})

    monkeypatch.setattr(fx_client, "_yf_download", _fake_download)

    assert fx_client.get_usd_hkd_latest() == 7.82345
    assert fx_client.get_usd_hkd_latest() == 7.82345
    assert calls == ["HKD=X"]


def test_usd_hkd_refreshes_stale_sqlite_cache(monkeypatch):
    init_db()
    _clear_usd_hkd_cache()
    save_fx_spot_rate("USDHKD", 7.8)
    conn = _get_connection()
    conn.execute(
        "UPDATE fx_spot_cache SET updated_at = '2000-01-01 00:00:00' WHERE symbol = 'USDHKD'"
    )
    conn.commit()

    def _fake_download(ticker, **kwargs):
        return pd.DataFrame({"Close": [7.83456]})

    monkeypatch.setattr(fx_client, "_yf_download", _fake_download)

    assert fx_client.get_usd_hkd_latest() == 7.83456


def test_usd_hkd_fallback_is_cached(monkeypatch):
    init_db()
    _clear_usd_hkd_cache()
    calls: list[str] = []

    def _fake_download(ticker, **kwargs):
        calls.append(ticker)
        return pd.DataFrame()

    monkeypatch.setattr(fx_client, "_yf_download", _fake_download)

    assert fx_client.get_usd_hkd_latest() == 7.80
    assert fx_client.get_usd_hkd_latest() == 7.80
    assert calls == ["HKD=X"]
