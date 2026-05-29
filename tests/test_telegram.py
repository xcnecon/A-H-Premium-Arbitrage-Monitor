from src.alerts import telegram
from src.config import settings


class _OkResponse:
    status_code = 200
    text = '{"ok": true}'

    def json(self):
        return {"ok": True}


def _enable_send_path(monkeypatch):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(telegram, "TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setattr(telegram, "TELEGRAM_CHAT_ID", "chat")


def test_send_alert_normalises_blank_proxy(monkeypatch):
    _enable_send_path(monkeypatch)
    monkeypatch.setattr(telegram, "TELEGRAM_PROXY_URL", "   ")

    proxies: list[str | None] = []

    def _fake_post(url, *, json, timeout, proxy):
        proxies.append(proxy)
        return _OkResponse()

    monkeypatch.setattr(telegram.httpx, "post", _fake_post)

    assert telegram.send_alert("hello")
    assert proxies == [None]


def test_send_alert_handles_proxy_config_error(monkeypatch):
    _enable_send_path(monkeypatch)
    monkeypatch.setattr(telegram, "TELEGRAM_PROXY_URL", "socks5://127.0.0.1:1080")

    def _fake_post(url, *, json, timeout, proxy):
        raise ImportError("Using SOCKS proxy, but socksio package is not installed")

    monkeypatch.setattr(telegram.httpx, "post", _fake_post)

    assert not telegram.send_alert("hello")
    assert telegram.get_last_error().startswith("proxy_config_error:")


def test_telegram_proxy_inherits_only_http_yahoo_proxy():
    assert (
        settings._telegram_proxy_url(None, "http://proxy.example:8080")
        == "http://proxy.example:8080"
    )
    assert (
        settings._telegram_proxy_url(None, "https://proxy.example:8443")
        == "https://proxy.example:8443"
    )
    assert settings._telegram_proxy_url(None, "socks5://127.0.0.1:1080") is None
    assert (
        settings._telegram_proxy_url(
            "socks5://127.0.0.1:1080",
            "http://proxy.example:8080",
        )
        == "socks5://127.0.0.1:1080"
    )
