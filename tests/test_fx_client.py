import pandas as pd

from src.data import fx_client
from src.storage.db import _get_connection, init_db, save_fx_spot_rate


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def _clear_fx_spot_cache(symbol: str) -> None:
    conn = _get_connection()
    conn.execute("DELETE FROM fx_spot_cache WHERE symbol = ?", (symbol,))
    conn.commit()


def _clear_fx_daily_cache() -> None:
    conn = _get_connection()
    conn.execute("DELETE FROM fx_rates")
    conn.commit()


def test_usd_hkd_uses_sqlite_cache(monkeypatch):
    init_db()
    _clear_fx_spot_cache("USDHKD")
    save_fx_spot_rate("USDHKD", 7.81234)

    def _raise_if_called(*args, **kwargs):
        raise AssertionError("network should not be called on fresh USDHKD cache")

    monkeypatch.setattr(fx_client, "_eastmoney_fx_latest", _raise_if_called)
    monkeypatch.setattr(fx_client, "_yf_download", _raise_if_called)

    assert fx_client.get_usd_hkd_latest() == 7.81234


def test_usd_hkd_fetches_eastmoney_once_then_uses_sqlite_cache(monkeypatch):
    init_db()
    _clear_fx_spot_cache("USDHKD")
    calls: list[str] = []

    def _fake_eastmoney(symbol):
        calls.append(symbol)
        return 7.82345

    def _raise_if_called(*args, **kwargs):
        raise AssertionError("yfinance should not be called when Eastmoney works")

    monkeypatch.setattr(fx_client, "_eastmoney_fx_latest", _fake_eastmoney)
    monkeypatch.setattr(fx_client, "_yf_download", _raise_if_called)

    assert fx_client.get_usd_hkd_latest() == 7.82345
    assert fx_client.get_usd_hkd_latest() == 7.82345
    assert calls == ["USDHKD"]


def test_usd_hkd_refreshes_stale_sqlite_cache(monkeypatch):
    init_db()
    _clear_fx_spot_cache("USDHKD")
    save_fx_spot_rate("USDHKD", 7.8)
    conn = _get_connection()
    conn.execute(
        "UPDATE fx_spot_cache SET updated_at = '2000-01-01 00:00:00' WHERE symbol = 'USDHKD'"
    )
    conn.commit()

    monkeypatch.setattr(fx_client, "_eastmoney_fx_latest", lambda symbol: 7.83456)

    def _raise_if_called(*args, **kwargs):
        raise AssertionError("yfinance should not be called when Eastmoney works")

    monkeypatch.setattr(fx_client, "_yf_download", _raise_if_called)

    assert fx_client.get_usd_hkd_latest() == 7.83456


def test_usd_hkd_falls_back_to_yahoo_when_eastmoney_fails(monkeypatch):
    init_db()
    _clear_fx_spot_cache("USDHKD")
    calls: list[str] = []

    def _fake_download(ticker, **kwargs):
        calls.append(ticker)
        return pd.DataFrame({"Close": [7.83456]})

    monkeypatch.setattr(fx_client, "_eastmoney_fx_latest", lambda symbol: None)
    monkeypatch.setattr(fx_client, "_yf_download", _fake_download)

    assert fx_client.get_usd_hkd_latest() == 7.83456
    assert calls == ["HKD=X"]


def test_usd_hkd_falls_back_to_yahoo_when_eastmoney_payload_is_malformed(monkeypatch):
    init_db()
    _clear_fx_spot_cache("USDHKD")
    calls: list[str] = []

    def _fake_download(ticker, **kwargs):
        calls.append(ticker)
        return pd.DataFrame({"Close": [7.83456]})

    monkeypatch.setattr(
        fx_client.httpx,
        "get",
        lambda *args, **kwargs: _FakeResponse({"data": None}),
    )
    monkeypatch.setattr(fx_client, "_yf_download", _fake_download)

    assert fx_client.get_usd_hkd_latest() == 7.83456
    assert calls == ["HKD=X"]


def test_usd_hkd_fallback_is_cached(monkeypatch):
    init_db()
    _clear_fx_spot_cache("USDHKD")
    calls: list[str] = []

    def _fake_download(ticker, **kwargs):
        calls.append(ticker)
        return pd.DataFrame()

    monkeypatch.setattr(fx_client, "_eastmoney_fx_latest", lambda symbol: None)
    monkeypatch.setattr(fx_client, "_yf_download", _fake_download)

    assert fx_client.get_usd_hkd_latest() == 7.80
    assert fx_client.get_usd_hkd_latest() == 7.80
    assert calls == ["HKD=X"]


def test_hkd_cnh_latest_uses_spot_cache(monkeypatch):
    init_db()
    _clear_fx_spot_cache("HKDCNH")
    save_fx_spot_rate("HKDCNH", 0.870123)

    def _raise_if_called(*args, **kwargs):
        raise AssertionError("network should not be called on fresh HKDCNH cache")

    monkeypatch.setattr(fx_client, "_eastmoney_fx_latest", _raise_if_called)
    monkeypatch.setattr(fx_client, "_akshare_fx_spot", _raise_if_called)
    monkeypatch.setattr(fx_client, "_yf_download", _raise_if_called)

    assert fx_client.get_fx_latest() == 0.870123


def test_hkd_cnh_latest_fetches_eastmoney_and_caches(monkeypatch):
    init_db()
    _clear_fx_spot_cache("HKDCNH")
    _clear_fx_daily_cache()
    calls: list[str] = []

    def _fake_eastmoney(symbol):
        calls.append(symbol)
        return 0.870234

    def _raise_if_called(*args, **kwargs):
        raise AssertionError("fallback sources should not be called when Eastmoney works")

    monkeypatch.setattr(fx_client, "_eastmoney_fx_latest", _fake_eastmoney)
    monkeypatch.setattr(fx_client, "_akshare_fx_spot", _raise_if_called)
    monkeypatch.setattr(fx_client, "_yf_download", _raise_if_called)

    assert fx_client.get_fx_latest() == 0.870234
    assert fx_client.get_fx_latest() == 0.870234
    assert calls == ["HKDCNH"]


def test_hkd_cnh_latest_falls_back_to_akshare_before_yahoo(monkeypatch):
    init_db()
    _clear_fx_spot_cache("HKDCNH")
    _clear_fx_daily_cache()

    def _raise_if_called(*args, **kwargs):
        raise AssertionError("yfinance should not be called when AKShare works")

    monkeypatch.setattr(fx_client, "_eastmoney_fx_latest", lambda symbol: None)
    monkeypatch.setattr(fx_client, "_akshare_fx_spot", lambda: 0.870345)
    monkeypatch.setattr(fx_client, "_yf_download", _raise_if_called)

    assert fx_client.get_fx_latest() == 0.870345


def test_eastmoney_fx_latest_rejects_malformed_payloads(monkeypatch):
    payloads = iter([
        [],
        {"data": None},
        {"data": {}},
        {"data": {"diff": None}},
        {"data": {"diff": {}}},
    ])

    monkeypatch.setattr(
        fx_client.httpx,
        "get",
        lambda *args, **kwargs: _FakeResponse(next(payloads)),
    )

    for _ in range(5):
        assert fx_client._eastmoney_fx_latest("HKDCNH") is None
