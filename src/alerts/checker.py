"""Alert checker engine -- crossover signal detection.

Fires once when premium crosses a threshold level, then waits for the next
crossing in either direction.  Each stock supports up to 3 threshold levels.
"""

import logging
import time
from datetime import timedelta, timezone

from src.alerts.telegram import format_premium_alert, send_alert
from src.config.settings import ALERT_BUFFER_PCT, ALERT_MAX_PER_MINUTE
from src.data.ah_mapping import get_a_code, get_pair_name
from src.storage.db import (
    get_alert_rules,
    log_alert_event,
    update_alert_state,
)

logger = logging.getLogger(__name__)

_HKT = timezone(timedelta(hours=8))

# Module-level rate limiter: list of timestamps of recent sends
_recent_sends: list[float] = []


def _is_rate_limited() -> bool:
    """Check if we've exceeded the global rate limit."""
    now = time.time()
    while _recent_sends and _recent_sends[0] < now - 60:
        _recent_sends.pop(0)
    return len(_recent_sends) >= ALERT_MAX_PER_MINUTE


def _record_send() -> None:
    """Record a send for rate limiting."""
    _recent_sends.append(time.time())


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

        # Determine which side of the threshold we are on now,
        # applying hysteresis buffer to prevent rapid flip-flopping.
        # If currently "below", must rise to threshold + buffer to cross up.
        # If currently "above", must drop to threshold - buffer to cross down.
        buf = ALERT_BUFFER_PCT
        if last_side == "below":
            current_side = "above" if premium >= threshold + buf else "below"
        elif last_side == "above":
            current_side = "below" if premium < threshold - buf else "above"
        else:
            # First evaluation — no buffer, just record side
            current_side = "above" if premium >= threshold else "below"

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
        logger.info(
            "CROSSOVER: %s premium %.2f%% crossed %.1f%% (%s)",
            hk_code,
            premium,
            threshold,
            cross_dir,
        )

        # Always update side first so we don't re-fire on the next tick
        update_alert_state(rule_id, last_side=current_side, last_premium=premium)

        if _is_rate_limited():
            logger.warning("Rate limited, skipping notification for %s", hk_code)
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
            _record_send()

        event_type = "fired" if sent else "send_failed"
        log_alert_event(
            rule_id, hk_code, cross_dir, event_type, premium, detail=f"threshold={threshold:.1f}"
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
