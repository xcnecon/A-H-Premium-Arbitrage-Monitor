"""Telegram Bot API client for sending alert messages.

Uses raw httpx for synchronous HTTP — no event loop conflicts with Streamlit.
"""

import logging
import os
import time

import httpx

from src.config.settings import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    TELEGRAM_PROXY_URL,
    TELEGRAM_TIMEOUT,
)

logger = logging.getLogger(__name__)

_API_URL = "https://api.telegram.org/bot{token}/sendMessage"
_LAST_ERROR: str | None = None


def _set_last_error(message: str | None) -> None:
    global _LAST_ERROR
    _LAST_ERROR = message


def get_last_error() -> str | None:
    """Return the most recent Telegram send failure reason."""
    return _LAST_ERROR


def _clean_error(message: object, limit: int = 240) -> str:
    text = str(message).replace("\n", " ").strip()
    return text[:limit]


def _normalise_proxy_url(proxy_url: str | None) -> str | None:
    if proxy_url is None:
        return None
    proxy_url = str(proxy_url).strip()
    return proxy_url or None


def send_alert(
    text: str,
    chat_id: str | None = None,
    parse_mode: str = "HTML",
    disable_notification: bool = False,
    max_retries: int = 3,
) -> bool:
    """Send a Telegram alert message.

    Args:
        text: Message text (HTML formatted).
        chat_id: Target chat ID. Defaults to TELEGRAM_CHAT_ID from settings.
        parse_mode: Message parse mode ("HTML" or "MarkdownV2").
        disable_notification: If True, send silently.
        max_retries: Maximum retry attempts on transient failures.

    Returns:
        True if message sent successfully, False otherwise.
    """
    _set_last_error(None)

    # Belt-and-suspenders: test suite leaked real Telegram messages to the user's
    # chat when a test's `patch("src.alerts.telegram.send_alert", ...)` silently
    # failed (the prior test's `patch.dict("sys.modules", ...)` evicted this
    # module on exit — see tests/conftest.py). Even with the conftest fix,
    # short-circuit under pytest so a future mock regression can't spam chat.
    # PYTEST_CURRENT_TEST is set by pytest for the duration of every test.
    if os.environ.get("PYTEST_CURRENT_TEST"):
        _set_last_error("suppressed_under_pytest")
        logger.debug("Telegram send_alert suppressed under pytest")
        return False
    token = TELEGRAM_BOT_TOKEN
    if not token:
        _set_last_error("token_not_configured")
        logger.debug("Telegram token not configured, skipping alert")
        return False

    target = chat_id or TELEGRAM_CHAT_ID
    if not target:
        _set_last_error("chat_id_not_configured")
        logger.debug("Telegram chat_id not configured, skipping alert")
        return False

    url = _API_URL.format(token=token)
    payload = {
        "chat_id": target,
        "text": text,
        "parse_mode": parse_mode,
        "disable_notification": disable_notification,
    }
    proxy_url = _normalise_proxy_url(TELEGRAM_PROXY_URL)

    for attempt in range(max_retries):
        try:
            resp = httpx.post(
                url,
                json=payload,
                timeout=TELEGRAM_TIMEOUT,
                proxy=proxy_url,
            )
            try:
                data = resp.json()
            except ValueError:
                detail = f"non_json_response status={resp.status_code}: {_clean_error(resp.text)}"
                _set_last_error(detail)
                logger.warning(
                    "Telegram non-JSON response (attempt %s/%s): %s",
                    attempt + 1,
                    max_retries,
                    detail,
                )
                data = {}

            if data.get("ok"):
                logger.info("Telegram alert sent to %s", target)
                return True

            error_code = data.get("error_code", resp.status_code)
            description = _clean_error(data.get("description") or resp.text)
            _set_last_error(f"telegram_error {error_code}: {description}")

            # Rate limited — respect retry_after
            if error_code == 429:
                wait = data.get("parameters", {}).get("retry_after", 5)
                logger.warning("Telegram rate limited, retry after %ss", wait)
                time.sleep(wait)
                continue

            # Permanent failures — do not retry
            if error_code in (400, 401, 403):
                logger.error("Telegram error %s: %s", error_code, description)
                return False

            logger.warning(
                "Telegram error %s (attempt %s/%s): %s",
                error_code,
                attempt + 1,
                max_retries,
                description,
            )

        except httpx.TimeoutException:
            _set_last_error("timeout")
            logger.warning("Telegram timeout (attempt %s/%s)", attempt + 1, max_retries)
        except httpx.HTTPError as exc:
            _set_last_error(f"http_error: {_clean_error(exc)}")
            logger.warning("Telegram HTTP error (attempt %s/%s): %s", attempt + 1, max_retries, exc)
        except (ImportError, ValueError) as exc:
            _set_last_error(f"proxy_config_error: {_clean_error(exc)}")
            logger.error("Telegram proxy configuration error: %s", exc)
            return False

        if attempt < max_retries - 1:
            time.sleep(2**attempt)

    logger.error("Telegram alert failed after %s retries", max_retries)
    return False


def format_premium_alert(
    name: str,
    hk_code: str,
    a_code: str,
    premium_pct: float,
    threshold: float,
    direction: str,
    a_price: float,
    h_price: float,
    fx_rate: float,
    daily_chg: float | None = None,
) -> str:
    """Format an A/H premium alert message in HTML.

    Args:
        name: Stock name (Chinese).
        hk_code: HK stock code (e.g. "00939").
        a_code: A-share code (e.g. "601939").
        premium_pct: Current H/A premium percentage.
        threshold: The threshold that was crossed.
        direction: "above" or "below".
        a_price: A-share price in CNY.
        h_price: H-share price in HKD.
        fx_rate: CNH per 1 HKD.
        daily_chg: Daily premium change in pp, if available.

    Returns:
        HTML-formatted message string.
    """
    from datetime import datetime, timedelta, timezone

    hkt = timezone(timedelta(hours=8))
    ts = datetime.now(hkt).strftime("%Y-%m-%d %H:%M:%S HKT")

    h_cny = h_price * fx_rate

    # Crossover direction indicators
    if direction == "cross_up":
        emoji = "\U0001f4c8"  # chart increasing
        cross_text = f"上穿 {threshold:+.1f}%"
    else:
        emoji = "\U0001f4c9"  # chart decreasing
        cross_text = f"下穿 {threshold:+.1f}%"

    # Severity
    abs_prem = abs(premium_pct)
    if abs_prem > 30:
        sev = "\U0001f534"  # red circle
    elif abs_prem > 15:
        sev = "\U0001f7e1"  # yellow circle
    else:
        sev = "\U0001f7e2"  # green circle

    sign = "+" if premium_pct >= 0 else ""
    chg_line = ""
    if daily_chg is not None:
        chg_sign = "+" if daily_chg >= 0 else ""
        chg_line = f"日内变动: {chg_sign}{daily_chg:.2f} pp\n"

    msg = (
        f"{sev} <b>A/H Premium Alert</b> {emoji}\n"
        f"\n"
        f"<b>{name}</b>\n"
        f"A: <code>{a_code}</code>  |  H: <code>HK.{hk_code}</code>\n"
        f"\n"
        f"<b>Premium: {sign}{premium_pct:.2f}%</b>\n"
        f"{cross_text}\n"
        f"{chg_line}"
        f"\n"
        f"<pre>"
        f"A price     CNY {a_price:>10.3f}\n"
        f"H price     HKD {h_price:>10.3f}\n"
        f"H in CNH    CNH {h_cny:>10.3f}\n"
        f"FX rate         {fx_rate:>10.4f}"
        f"</pre>\n"
        f"\n"
        f"<i>{ts}</i>"
    )
    return msg
