from datetime import date

import pandas as pd

from src.alerts import special_events
from src.data import sync
from src.storage.db import _get_connection, init_db
from src.storage.kline_cache import save_kline


def _reset_special_event_state() -> None:
    conn = _get_connection()
    conn.execute("DELETE FROM special_event_state")
    conn.commit()


def _seed_single_side_a_halt(hk_code: str, a_code: str) -> None:
    save_kline(
        a_code,
        "A",
        pd.DataFrame(
            [
                {
                    "date": "2026-04-17",
                    "open": 9.0,
                    "high": 9.2,
                    "low": 8.9,
                    "close": 9.1,
                    "volume": 1000,
                    "turnover": 9100.0,
                }
            ]
        ),
    )
    save_kline(
        hk_code,
        "H",
        pd.DataFrame(
            [
                {
                    "date": "2026-04-20",
                    "open": 6.0,
                    "high": 6.2,
                    "low": 5.9,
                    "close": 6.1,
                    "volume": 1000,
                    "turnover": 6100.0,
                },
                {
                    "date": "2026-04-21",
                    "open": 6.1,
                    "high": 6.2,
                    "low": 6.0,
                    "close": 6.1,
                    "volume": 1000,
                    "turnover": 6100.0,
                },
                {
                    "date": "2026-05-05",
                    "open": 6.2,
                    "high": 6.3,
                    "low": 6.1,
                    "close": 6.2,
                    "volume": 1000,
                    "turnover": 6200.0,
                },
            ]
        ),
    )


def _patch_calendars(monkeypatch, a_trade_dates: set[date], prev_a: date, prev_h: date) -> None:
    monkeypatch.setattr(sync, "_get_a_trade_dates", lambda: a_trade_dates)
    monkeypatch.setattr(sync, "_prev_a_trading_day", lambda d: prev_a)
    monkeypatch.setattr(special_events, "_prev_h_trading_day", lambda d: prev_h)
    monkeypatch.setattr(special_events, "_h_trade_days_between", lambda start, end: [])


def test_special_event_alerts_once_for_confirmed_a_side_no_trade(monkeypatch):
    init_db()
    _reset_special_event_state()
    hk_code = "TSE01"
    a_code = "TSEA01"
    _seed_single_side_a_halt(hk_code, a_code)
    pairs = {hk_code: {"a_code": a_code, "name": "Test Securities"}}
    a_trade_dates = {
        date(2026, 4, 20),
        date(2026, 4, 21),
        date(2026, 4, 22),
        date(2026, 4, 23),
        date(2026, 4, 24),
        date(2026, 4, 27),
        date(2026, 4, 28),
        date(2026, 4, 29),
        date(2026, 4, 30),
    }
    sent: list[str] = []

    _patch_calendars(monkeypatch, a_trade_dates, date(2026, 4, 30), date(2026, 5, 5))
    monkeypatch.setattr(
        special_events,
        "send_alert",
        lambda msg, **kwargs: sent.append(msg) or True,
    )

    events = special_events.evaluate_special_events(pairs=pairs, today=date(2026, 5, 6))
    second_events = special_events.evaluate_special_events(pairs=pairs, today=date(2026, 5, 6))

    assert len(sent) == 1
    assert events == [
        {
            "event": "fired",
            "event_type": "single_side_no_trade",
            "hk_code": hk_code,
            "a_code": a_code,
            "side": "A",
            "sent": True,
        }
    ]
    assert second_events == []


def test_special_event_resolves_when_a_side_resumes(monkeypatch):
    init_db()
    _reset_special_event_state()
    hk_code = "TSE02"
    a_code = "TSEA02"
    _seed_single_side_a_halt(hk_code, a_code)
    pairs = {hk_code: {"a_code": a_code, "name": "Test Securities"}}
    a_trade_dates = {
        date(2026, 4, 20),
        date(2026, 4, 21),
        date(2026, 4, 22),
        date(2026, 4, 23),
        date(2026, 4, 24),
        date(2026, 4, 27),
        date(2026, 4, 28),
        date(2026, 4, 29),
        date(2026, 4, 30),
    }
    sent: list[str] = []

    _patch_calendars(monkeypatch, a_trade_dates, date(2026, 4, 30), date(2026, 5, 5))
    monkeypatch.setattr(
        special_events,
        "send_alert",
        lambda msg, **kwargs: sent.append(msg) or True,
    )

    special_events.evaluate_special_events(pairs=pairs, today=date(2026, 5, 6))
    save_kline(
        a_code,
        "A",
        pd.DataFrame(
            [
                {
                    "date": "2026-04-30",
                    "open": 9.0,
                    "high": 9.2,
                    "low": 8.9,
                    "close": 9.1,
                    "volume": 1000,
                    "turnover": 9100.0,
                }
            ]
        ),
    )
    events = special_events.evaluate_special_events(pairs=pairs, today=date(2026, 5, 6))

    assert len(sent) == 2
    assert "Resolved" in sent[-1]
    assert events == [
        {
            "event": "resolved",
            "event_type": "single_side_no_trade",
            "hk_code": hk_code,
            "a_code": a_code,
            "side": "A",
            "sent": True,
        }
    ]


def test_special_event_resolve_send_failure_retries(monkeypatch):
    init_db()
    _reset_special_event_state()
    hk_code = "TSE03"
    a_code = "TSEA03"
    _seed_single_side_a_halt(hk_code, a_code)
    pairs = {hk_code: {"a_code": a_code, "name": "Test Securities"}}
    a_trade_dates = {
        date(2026, 4, 20),
        date(2026, 4, 21),
        date(2026, 4, 22),
        date(2026, 4, 23),
        date(2026, 4, 24),
        date(2026, 4, 27),
        date(2026, 4, 28),
        date(2026, 4, 29),
        date(2026, 4, 30),
    }
    send_results = iter([True, False, True])
    sent: list[str] = []

    _patch_calendars(monkeypatch, a_trade_dates, date(2026, 4, 30), date(2026, 5, 5))
    monkeypatch.setattr(
        special_events,
        "send_alert",
        lambda msg, **kwargs: sent.append(msg) or next(send_results),
    )

    special_events.evaluate_special_events(pairs=pairs, today=date(2026, 5, 6))
    save_kline(
        a_code,
        "A",
        pd.DataFrame(
            [
                {
                    "date": "2026-04-30",
                    "open": 9.0,
                    "high": 9.2,
                    "low": 8.9,
                    "close": 9.1,
                    "volume": 1000,
                    "turnover": 9100.0,
                }
            ]
        ),
    )

    first_resolution = special_events.evaluate_special_events(
        pairs=pairs,
        today=date(2026, 5, 6),
    )
    second_resolution = special_events.evaluate_special_events(
        pairs=pairs,
        today=date(2026, 5, 6),
    )

    assert first_resolution[0]["event"] == "resolve_send_failed"
    assert second_resolution[0]["event"] == "resolved"
    assert len(sent) == 3


def test_resolved_event_reactivation_resets_episode_and_retries_failed_send(monkeypatch):
    init_db()
    _reset_special_event_state()
    hk_code = "TSE04"
    a_code = "TSEA04"
    _seed_single_side_a_halt(hk_code, a_code)
    pairs = {hk_code: {"a_code": a_code, "name": "Test Securities"}}
    a_trade_dates_first = {
        date(2026, 4, 20),
        date(2026, 4, 21),
        date(2026, 4, 22),
        date(2026, 4, 23),
        date(2026, 4, 24),
        date(2026, 4, 27),
        date(2026, 4, 28),
        date(2026, 4, 29),
        date(2026, 4, 30),
    }
    sent: list[str] = []

    _patch_calendars(monkeypatch, a_trade_dates_first, date(2026, 4, 30), date(2026, 5, 5))
    monkeypatch.setattr(
        special_events,
        "send_alert",
        lambda msg, **kwargs: sent.append(msg) or True,
    )

    special_events.evaluate_special_events(pairs=pairs, today=date(2026, 5, 6))
    save_kline(
        a_code,
        "A",
        pd.DataFrame(
            [
                {
                    "date": "2026-04-30",
                    "open": 9.0,
                    "high": 9.2,
                    "low": 8.9,
                    "close": 9.1,
                    "volume": 1000,
                    "turnover": 9100.0,
                }
            ]
        ),
    )
    special_events.evaluate_special_events(pairs=pairs, today=date(2026, 5, 6))

    save_kline(
        hk_code,
        "H",
        pd.DataFrame(
            [
                {
                    "date": "2026-05-06",
                    "open": 6.0,
                    "high": 6.2,
                    "low": 5.9,
                    "close": 6.1,
                    "volume": 1000,
                    "turnover": 6100.0,
                },
                {
                    "date": "2026-05-07",
                    "open": 6.1,
                    "high": 6.2,
                    "low": 6.0,
                    "close": 6.1,
                    "volume": 1000,
                    "turnover": 6100.0,
                },
            ]
        ),
    )
    a_trade_dates_second = a_trade_dates_first | {date(2026, 5, 6), date(2026, 5, 7)}
    _patch_calendars(monkeypatch, a_trade_dates_second, date(2026, 5, 7), date(2026, 5, 7))
    send_results = iter([False, True])
    monkeypatch.setattr(
        special_events,
        "send_alert",
        lambda msg, **kwargs: sent.append(msg) or next(send_results),
    )

    failed_reactivation = special_events.evaluate_special_events(
        pairs=pairs,
        today=date(2026, 5, 8),
    )
    retry_reactivation = special_events.evaluate_special_events(
        pairs=pairs,
        today=date(2026, 5, 8),
    )
    state = _get_connection().execute(
        "SELECT started_date, notified_at FROM special_event_state WHERE hk_code = ?",
        (hk_code,),
    ).fetchone()

    assert failed_reactivation[0]["event"] == "send_failed"
    assert retry_reactivation[0]["event"] == "fired"
    assert state["started_date"] == "2026-05-06"
    assert state["notified_at"] is not None


def test_h_side_gap_uses_hk_calendar_not_weekdays(monkeypatch):
    init_db()
    _reset_special_event_state()
    hk_code = "TSE05"
    a_code = "TSEA05"
    save_kline(
        hk_code,
        "H",
        pd.DataFrame(
            [
                {
                    "date": "2025-12-24",
                    "open": 6.0,
                    "high": 6.2,
                    "low": 5.9,
                    "close": 6.1,
                    "volume": 1000,
                    "turnover": 6100.0,
                }
            ]
        ),
    )
    save_kline(
        a_code,
        "A",
        pd.DataFrame(
            [
                {
                    "date": "2025-12-25",
                    "open": 9.0,
                    "high": 9.2,
                    "low": 8.9,
                    "close": 9.1,
                    "volume": 1000,
                    "turnover": 9100.0,
                },
                {
                    "date": "2025-12-26",
                    "open": 9.0,
                    "high": 9.2,
                    "low": 8.9,
                    "close": 9.1,
                    "volume": 1000,
                    "turnover": 9100.0,
                },
            ]
        ),
    )
    pairs = {hk_code: {"a_code": a_code, "name": "Test Securities"}}
    sent: list[str] = []

    monkeypatch.setattr(
        sync,
        "_get_a_trade_dates",
        lambda: {date(2025, 12, 25), date(2025, 12, 26)},
    )
    monkeypatch.setattr(sync, "_prev_a_trading_day", lambda d: date(2025, 12, 26))
    monkeypatch.setattr(special_events, "_prev_h_trading_day", lambda d: date(2025, 12, 24))
    monkeypatch.setattr(special_events, "_h_trade_days_between", lambda start, end: [])
    monkeypatch.setattr(
        special_events,
        "send_alert",
        lambda msg, **kwargs: sent.append(msg) or True,
    )

    events = special_events.evaluate_special_events(pairs=pairs, today=date(2025, 12, 29))

    assert events == []
    assert sent == []
