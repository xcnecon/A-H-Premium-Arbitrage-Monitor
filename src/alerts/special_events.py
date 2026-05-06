"""Special-event Telegram alerts for confirmed A/H market-state changes."""

import html
import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from futu import RET_OK, TradeDateMarket

from src.alerts.telegram import send_alert
from src.data.ah_mapping import get_all_pairs
from src.data.futu_ctx import get_quote_ctx
from src.storage.db import _get_connection

logger = logging.getLogger(__name__)

_HKT = ZoneInfo("Asia/Hong_Kong")
_EVENT_TYPE_SINGLE_SIDE_NO_TRADE = "single_side_no_trade"
_STATUS_ACTIVE = "active"
_STATUS_RESOLVE_PENDING = "resolve_pending"
_STATUS_RESOLVED = "resolved"
_MIN_MISSING_TRADE_DAYS = 2
_MIN_OTHER_SIDE_ACTIVE_DAYS = 2
_MAX_INDIVIDUAL_EVENTS_PER_RUN = int(os.getenv("SPECIAL_EVENT_MAX_PER_RUN", "8"))
_h_trade_dates_cache: dict[tuple[str, str], set[date] | None] = {}


def _ensure_schema() -> None:
    conn = _get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS special_event_state (
            event_key            TEXT PRIMARY KEY,
            event_type           TEXT NOT NULL,
            hk_code              TEXT NOT NULL,
            a_code               TEXT NOT NULL,
            side                 TEXT NOT NULL,
            status               TEXT NOT NULL,
            started_date         TEXT NOT NULL,
            last_seen_date       TEXT NOT NULL,
            ended_date           TEXT,
            evidence_json        TEXT,
            notified_at          TEXT,
            resolved_notified_at TEXT,
            updated_at           TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_special_event_state_status
        ON special_event_state(status, event_type)
    """)
    conn.commit()


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _today_hkt() -> date:
    return datetime.now(_HKT).date()


def _weekdays_between(start_exclusive: date, end_inclusive: date) -> list[date]:
    days: list[date] = []
    current = start_exclusive + timedelta(days=1)
    while current <= end_inclusive:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def _a_trade_days_between(start_exclusive: date, end_inclusive: date) -> list[date]:
    from src.data import sync

    trade_dates = sync._get_a_trade_dates()
    if not trade_dates:
        return _weekdays_between(start_exclusive, end_inclusive)
    return sorted(d for d in trade_dates if start_exclusive < d <= end_inclusive)


def _get_h_trade_dates(start: date, end: date) -> set[date] | None:
    """Return HKEX trading days from Futu, or None when the source is unavailable."""
    if start > end:
        return set()

    key = (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    if key in _h_trade_dates_cache:
        return _h_trade_dates_cache[key]

    try:
        ctx = get_quote_ctx()
        ret, data = ctx.request_trading_days(
            TradeDateMarket.HK,
            start=key[0],
            end=key[1],
        )
        if ret != RET_OK:
            logger.warning("Futu HK trading calendar failed: %s", data)
            _h_trade_dates_cache[key] = None
            return None

        trade_dates = {_parse_date(row.get("time")) for row in data}
        result = {d for d in trade_dates if d is not None}
        _h_trade_dates_cache[key] = result
        return result
    except Exception as e:
        logger.warning("Futu HK trading calendar error: %s", e)
        _h_trade_dates_cache[key] = None
        return None


def _h_trade_days_between(start_exclusive: date, end_inclusive: date) -> list[date] | None:
    trade_dates = _get_h_trade_dates(start_exclusive + timedelta(days=1), end_inclusive)
    if trade_dates is None:
        return None
    return sorted(d for d in trade_dates if start_exclusive < d <= end_inclusive)


def _prev_h_trading_day(d: date) -> date:
    start = d - timedelta(days=365)
    end = d - timedelta(days=1)
    trade_dates = _get_h_trade_dates(start, end)
    if trade_dates:
        return max(trade_dates)

    from src.data import sync

    return sync._prev_trading_day(d)


def _last_bar(conn, table: str, code: str) -> dict[str, Any] | None:
    row = conn.execute(
        f"SELECT date, close, volume, turnover FROM {table} "
        "WHERE code = ? ORDER BY date DESC LIMIT 1",
        (code,),
    ).fetchone()
    return dict(row) if row else None


def _active_days_after(
    conn,
    table: str,
    code: str,
    start_exclusive: date,
    end_inclusive: date,
) -> int:
    row = conn.execute(
        f"SELECT COUNT(*) AS cnt FROM {table} "
        "WHERE code = ? AND date > ? AND date <= ? AND volume > 0",
        (code, start_exclusive.strftime("%Y-%m-%d"), end_inclusive.strftime("%Y-%m-%d")),
    ).fetchone()
    return int(row["cnt"]) if row else 0


def _states_needing_resolution() -> list[dict[str, Any]]:
    conn = _get_connection()
    rows = conn.execute(
        "SELECT * FROM special_event_state "
        "WHERE event_type = ? AND status IN (?, ?)",
        (_EVENT_TYPE_SINGLE_SIDE_NO_TRADE, _STATUS_ACTIVE, _STATUS_RESOLVE_PENDING),
    ).fetchall()
    return [dict(row) for row in rows]


def _state_for(event_key: str) -> dict[str, Any] | None:
    conn = _get_connection()
    row = conn.execute(
        "SELECT * FROM special_event_state WHERE event_key = ?",
        (event_key,),
    ).fetchone()
    return dict(row) if row else None


def _upsert_active_event(event: dict[str, Any], notified: bool) -> None:
    conn = _get_connection()
    now_sql = "CURRENT_TIMESTAMP"
    notified_sql = now_sql if notified else "NULL"
    conn.execute(
        f"""
        INSERT INTO special_event_state (
            event_key, event_type, hk_code, a_code, side, status,
            started_date, last_seen_date, ended_date, evidence_json,
            notified_at, resolved_notified_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, 'active', ?, ?, NULL, ?, {notified_sql}, NULL, CURRENT_TIMESTAMP)
        ON CONFLICT(event_key) DO UPDATE SET
            status = 'active',
            started_date = CASE
                WHEN special_event_state.status = 'resolved'
                THEN excluded.started_date
                ELSE special_event_state.started_date
            END,
            last_seen_date = excluded.last_seen_date,
            ended_date = NULL,
            evidence_json = excluded.evidence_json,
            notified_at = CASE
                WHEN special_event_state.status = 'resolved'
                THEN excluded.notified_at
                ELSE COALESCE(special_event_state.notified_at, excluded.notified_at)
            END,
            resolved_notified_at = CASE
                WHEN special_event_state.status = 'resolved'
                THEN NULL
                ELSE special_event_state.resolved_notified_at
            END,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            event["event_key"],
            event["event_type"],
            event["hk_code"],
            event["a_code"],
            event["side"],
            event["started_date"],
            event["last_seen_date"],
            json.dumps(event["evidence"], ensure_ascii=False, sort_keys=True),
        ),
    )
    conn.commit()


def _mark_resolution_attempt(
    state: dict[str, Any],
    ended_date: str,
    notified: bool,
    evidence: dict,
) -> None:
    conn = _get_connection()
    status = _STATUS_RESOLVED if notified else _STATUS_RESOLVE_PENDING
    notified_sql = "CURRENT_TIMESTAMP" if notified else "NULL"
    conn.execute(
        f"""
        UPDATE special_event_state
        SET status = ?,
            ended_date = ?,
            evidence_json = ?,
            resolved_notified_at = COALESCE(resolved_notified_at, {notified_sql}),
            updated_at = CURRENT_TIMESTAMP
        WHERE event_key = ?
        """,
        (
            status,
            ended_date,
            json.dumps(evidence, ensure_ascii=False, sort_keys=True),
            state["event_key"],
        ),
    )
    conn.commit()


def _format_event_message(event: dict[str, Any], resolved: bool = False) -> str:
    name = html.escape(str(event.get("name") or event["hk_code"]))
    side = event["side"]
    side_name = "A-share" if side == "A" else "H-share"
    other_side = "H-share" if side == "A" else "A-share"
    title = "A/H Special Event Resolved" if resolved else "A/H Special Event"
    status_line = (
        f"{side_name} trading has resumed."
        if resolved
        else f"{side_name} has no confirmed trading while {other_side} continues trading."
    )
    evidence = event["evidence"]
    ts = datetime.now(_HKT).strftime("%Y-%m-%d %H:%M:%S HKT")

    lines = [
        f"{title}",
        "",
        f"<b>{name}</b>",
        "A: <code>{}</code>  |  H: <code>HK.{}</code>".format(
            html.escape(event["a_code"]),
            html.escape(event["hk_code"]),
        ),
        "",
        f"<b>{html.escape(status_line)}</b>",
        f"Started: <code>{html.escape(event['started_date'])}</code>",
        f"Last confirmed: <code>{html.escape(event['last_seen_date'])}</code>",
    ]
    if resolved and event.get("ended_date"):
        lines.append(f"Resolved: <code>{html.escape(event['ended_date'])}</code>")

    lines.extend([
        "",
        "<pre>",
        f"{side_name} last trade: {evidence.get('missing_side_last_trade', '-')}",
        f"{other_side} latest trade: {evidence.get('other_side_latest_trade', '-')}",
        f"Missing expected days: {evidence.get('missing_expected_days', 0)}",
        f"{other_side} active days: {evidence.get('other_side_active_days', 0)}",
        "</pre>",
        "",
        f"<i>{ts}</i>",
    ])
    return "\n".join(lines)


def _single_side_candidates(pairs: dict[str, dict[str, str]], today: date) -> list[dict[str, Any]]:
    from src.data import sync

    prev_h_td: date | None = None
    prev_a_td: date | None = None
    conn = _get_connection()
    candidates: list[dict[str, Any]] = []

    for hk_code, info in pairs.items():
        a_code = info["a_code"]
        name = info.get("name", "")
        a_bar = _last_bar(conn, "kline_a", a_code)
        h_bar = _last_bar(conn, "kline_h", hk_code)
        if not a_bar or not h_bar:
            continue

        a_last = _parse_date(a_bar["date"])
        h_last = _parse_date(h_bar["date"])
        if not a_last or not h_last:
            continue

        if prev_h_td is None:
            prev_h_td = _prev_h_trading_day(today)
        if prev_a_td is None:
            prev_a_td = sync._prev_a_trading_day(today)

        a_missing_days = _a_trade_days_between(a_last, prev_a_td)
        h_active_after_a = _active_days_after(conn, "kline_h", hk_code, a_last, prev_h_td)
        if (
            len(a_missing_days) >= _MIN_MISSING_TRADE_DAYS
            and h_active_after_a >= _MIN_OTHER_SIDE_ACTIVE_DAYS
            and h_last >= prev_h_td
        ):
            candidates.append({
                "event_key": f"{_EVENT_TYPE_SINGLE_SIDE_NO_TRADE}:A:{hk_code}:{a_code}",
                "event_type": _EVENT_TYPE_SINGLE_SIDE_NO_TRADE,
                "hk_code": hk_code,
                "a_code": a_code,
                "name": name,
                "side": "A",
                "started_date": a_missing_days[0].strftime("%Y-%m-%d"),
                "last_seen_date": a_missing_days[-1].strftime("%Y-%m-%d"),
                "evidence": {
                    "missing_side_last_trade": a_bar["date"],
                    "other_side_latest_trade": h_bar["date"],
                    "missing_expected_days": len(a_missing_days),
                    "other_side_active_days": h_active_after_a,
                },
            })

        h_missing_days = _h_trade_days_between(h_last, prev_h_td)
        a_active_after_h = _active_days_after(conn, "kline_a", a_code, h_last, prev_a_td)
        if (
            h_missing_days is not None
            and len(h_missing_days) >= _MIN_MISSING_TRADE_DAYS
            and a_active_after_h >= _MIN_OTHER_SIDE_ACTIVE_DAYS
            and a_last >= prev_a_td
        ):
            candidates.append({
                "event_key": f"{_EVENT_TYPE_SINGLE_SIDE_NO_TRADE}:H:{hk_code}:{a_code}",
                "event_type": _EVENT_TYPE_SINGLE_SIDE_NO_TRADE,
                "hk_code": hk_code,
                "a_code": a_code,
                "name": name,
                "side": "H",
                "started_date": h_missing_days[0].strftime("%Y-%m-%d"),
                "last_seen_date": h_missing_days[-1].strftime("%Y-%m-%d"),
                "evidence": {
                    "missing_side_last_trade": h_bar["date"],
                    "other_side_latest_trade": a_bar["date"],
                    "missing_expected_days": len(h_missing_days),
                    "other_side_active_days": a_active_after_h,
                },
            })

    return candidates


def evaluate_special_events(
    pairs: dict[str, dict[str, str]] | None = None,
    today: date | None = None,
) -> list[dict[str, Any]]:
    """Detect and notify confirmed special events.

    The detector intentionally waits for at least two missed completed trading
    days and two active days on the other side, so ordinary one-day gaps and
    transient data-source failures do not generate Telegram noise.
    """
    _ensure_schema()
    pairs = pairs or get_all_pairs()
    today = today or _today_hkt()
    candidates = _single_side_candidates(pairs, today)
    candidate_keys = {event["event_key"] for event in candidates}
    results: list[dict[str, Any]] = []

    if len(candidates) > _MAX_INDIVIDUAL_EVENTS_PER_RUN:
        logger.warning(
            "Suppressing %d special-event candidates; likely broad data issue", len(candidates)
        )
        candidates_to_notify: list[dict[str, Any]] = []
    else:
        candidates_to_notify = candidates

    for event in candidates_to_notify:
        state = _state_for(event["event_key"])
        should_notify = (
            state is None
            or state["status"] == _STATUS_RESOLVED
            or not state["notified_at"]
        )
        sent = False
        if should_notify:
            sent = send_alert(_format_event_message(event), disable_notification=False)
        _upsert_active_event(event, notified=sent)
        if should_notify:
            results.append({
                "event": "fired" if sent else "send_failed",
                "event_type": event["event_type"],
                "hk_code": event["hk_code"],
                "a_code": event["a_code"],
                "side": event["side"],
                "sent": sent,
            })

    pairs_by_hk = pairs
    for state in _states_needing_resolution():
        if state["event_key"] in candidate_keys:
            continue
        info = pairs_by_hk.get(state["hk_code"], {})
        event = {
            "event_key": state["event_key"],
            "event_type": state["event_type"],
            "hk_code": state["hk_code"],
            "a_code": state["a_code"],
            "name": info.get("name", ""),
            "side": state["side"],
            "started_date": state["started_date"],
            "last_seen_date": state["last_seen_date"],
            "ended_date": state["ended_date"] or today.strftime("%Y-%m-%d"),
            "evidence": json.loads(state["evidence_json"] or "{}"),
        }
        sent = send_alert(_format_event_message(event, resolved=True), disable_notification=False)
        _mark_resolution_attempt(
            state,
            event["ended_date"],
            notified=sent,
            evidence=event["evidence"],
        )
        results.append({
            "event": "resolved" if sent else "resolve_send_failed",
            "event_type": state["event_type"],
            "hk_code": state["hk_code"],
            "a_code": state["a_code"],
            "side": state["side"],
            "sent": sent,
        })

    return results
