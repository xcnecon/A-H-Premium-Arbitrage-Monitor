"""Alert checker engine -- crossover signal detection.

Fires once when premium crosses a threshold level, then waits for the next
crossing in either direction.  Each stock supports up to 3 threshold levels.
"""

import logging
import time
from datetime import datetime, timedelta, timezone

from src.alerts.telegram import format_premium_alert, get_last_error, send_alert
from src.config.settings import (
    ALERT_BUFFER_PCT,
    ALERT_MAX_PER_MINUTE,
    MARKET_OVERLAP_END,
    MARKET_OVERLAP_START,
)
from src.data.ah_mapping import get_a_code, get_pair_name
from src.storage.db import (
    get_alert_rules,
    log_alert_event,
    update_alert_state,
)

logger = logging.getLogger(__name__)

_HKT = timezone(timedelta(hours=8))
_RETRY_AFTER_FAILURE_SECONDS = 60

# Module-level rate limiter: list of timestamps of recent sends
_recent_sends: list[float] = []
_pending_notification_retries: dict[tuple[int, str], float] = {}


def _is_rate_limited() -> bool:
    """Check if we've exceeded the global rate limit."""
    now = time.time()
    while _recent_sends and _recent_sends[0] < now - 60:
        _recent_sends.pop(0)
    return len(_recent_sends) >= ALERT_MAX_PER_MINUTE


def _record_send() -> None:
    """Record a send for rate limiting."""
    _recent_sends.append(time.time())


def _retry_deferred(rule_id: int, direction: str) -> bool:
    retry_after = _pending_notification_retries.get((rule_id, direction), 0.0)
    return time.time() < retry_after


def _defer_retry(rule_id: int, direction: str) -> None:
    _pending_notification_retries[(rule_id, direction)] = time.time() + _RETRY_AFTER_FAILURE_SECONDS


def _clear_retry(rule_id: int, direction: str) -> None:
    _pending_notification_retries.pop((rule_id, direction), None)


def _is_permanent_telegram_failure(last_error: str | None) -> bool:
    if not last_error:
        return False
    permanent_markers = (
        "telegram_error 400:",
        "telegram_error 401:",
        "telegram_error 403:",
        "token_not_configured",
        "chat_id_not_configured",
        "suppressed_under_pytest",
    )
    return any(marker in last_error for marker in permanent_markers)


def _is_market_overlap(now: datetime | None = None) -> bool:
    """True during the A/H arbitrage overlap window in HKT."""
    now = now or datetime.now(_HKT)
    if now.weekday() >= 5:
        return False

    current = now.time()
    start = datetime.strptime(MARKET_OVERLAP_START, "%H:%M").time()
    end = datetime.strptime(MARKET_OVERLAP_END, "%H:%M").time()
    return start <= current <= end


def _side_for_premium(premium: float, threshold: float, last_side: str | None) -> str:
    """Return the threshold side, applying hysteresis when a prior side exists."""
    buf = ALERT_BUFFER_PCT
    if last_side == "below":
        return "above" if premium >= threshold + buf else "below"
    if last_side == "above":
        return "below" if premium < threshold - buf else "above"
    return "above" if premium >= threshold else "below"


def evaluate_alerts(premium_data: dict[str, dict], fx_rate: float) -> list[dict]:
    """Evaluate crossover alerts against current premium data.

    Compares the current premium to each threshold.  If the premium has crossed
    from one side to the other since the last evaluation, fires a one-shot
    notification.  The first evaluation after a rule is created only records
    the initial side without firing.

    Args:
        premium_data: Dict mapping hk_code to snapshot info:
            {hk_code: {"premium": float, "a_price": float,
                        "h_price": float, "daily_chg": float|None}}
        fx_rate: Current CNH/HKD FX rate.

    Returns:
        List of crossover events that were triggered (for UI feedback).
    """
    rules = get_alert_rules()
    if not rules:
        return []

    events: list[dict] = []

    for rule in rules:
        hk_code = rule["hk_code"]
        data = premium_data.get(hk_code)
        if not data:
            continue

        premium = data.get("premium")
        if premium is None:
            continue

        rule_id = rule["id"]
        threshold = rule["threshold"]
        last_side = rule.get("last_side")

        current_side = _side_for_premium(premium, threshold, last_side)

        if last_side is None:
            # First evaluation — record the side without firing
            update_alert_state(rule_id, last_side=current_side, last_premium=premium)
            logger.info(
                "Alert initialized: %s @ %.1f%% (side=%s, premium=%.2f%%)",
                hk_code,
                threshold,
                current_side,
                premium,
            )
            continue

        if current_side == last_side:
            # Same side — no crossing, just update last_premium
            update_alert_state(rule_id, last_premium=premium)
            continue

        # ── CROSSOVER detected (passed buffer threshold) ──
        cross_dir = "cross_up" if current_side == "above" else "cross_down"
        prior_premium = rule.get("last_premium")
        prior_side = (
            _side_for_premium(prior_premium, threshold, last_side)
            if prior_premium is not None
            else last_side
        )
        already_pending = prior_side == current_side

        if not _is_market_overlap():
            update_alert_state(rule_id, last_premium=premium)
            if prior_side != current_side:
                logger.info(
                    "Skipping Telegram alert outside A/H overlap window (%s-%s HKT): %s",
                    MARKET_OVERLAP_START,
                    MARKET_OVERLAP_END,
                    hk_code,
                )
                log_alert_event(
                    rule_id,
                    hk_code,
                    cross_dir,
                    "outside_market_overlap",
                    premium,
                    detail=f"threshold={threshold:.1f}",
                )
            continue

        if already_pending and _retry_deferred(rule_id, cross_dir):
            update_alert_state(rule_id, last_premium=premium)
            continue

        logger.info(
            "CROSSOVER: %s premium %.2f%% crossed %.1f%% (%s)",
            hk_code,
            premium,
            threshold,
            cross_dir,
        )

        if _is_rate_limited():
            logger.warning("Rate limited, skipping notification for %s", hk_code)
            update_alert_state(rule_id, last_premium=premium)
            _defer_retry(rule_id, cross_dir)
            log_alert_event(
                rule_id,
                hk_code,
                cross_dir,
                "rate_limited",
                premium,
                detail=f"threshold={threshold:.1f}",
            )
            continue

        # Send Telegram notification
        name = get_pair_name(hk_code) or hk_code
        a_code = get_a_code(hk_code) or ""
        a_price = data.get("a_price", 0)
        h_price = data.get("h_price", 0)
        daily_chg = data.get("daily_chg")

        msg = format_premium_alert(
            name=name,
            hk_code=hk_code,
            a_code=a_code,
            premium_pct=premium,
            threshold=threshold,
            direction=cross_dir,
            a_price=a_price,
            h_price=h_price,
            fx_rate=fx_rate,
            daily_chg=daily_chg,
        )
        sent = send_alert(msg)
        if sent:
            update_alert_state(rule_id, last_side=current_side, last_premium=premium)
            _record_send()
            _clear_retry(rule_id, cross_dir)
        else:
            last_error = get_last_error()
            if _is_permanent_telegram_failure(last_error):
                update_alert_state(rule_id, last_side=current_side, last_premium=premium)
                _clear_retry(rule_id, cross_dir)
            else:
                update_alert_state(rule_id, last_premium=premium)
                _defer_retry(rule_id, cross_dir)

        event_type = "fired" if sent else "send_failed"
        detail = f"threshold={threshold:.1f}"
        if not sent:
            if last_error:
                detail = f"{detail}; telegram_error={last_error}"
        log_alert_event(
            rule_id, hk_code, cross_dir, event_type, premium, detail=detail
        )
        events.append(
            {
                "event": event_type,
                "hk_code": hk_code,
                "direction": cross_dir,
                "premium": premium,
                "threshold": threshold,
                "sent": sent,
            }
        )

    return events
