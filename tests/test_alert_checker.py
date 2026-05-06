from datetime import datetime

from src.alerts import checker
from src.storage.db import get_alert_history, init_db, upsert_alert_rule


def test_market_overlap_window_bounds():
    hkt = checker._HKT

    assert not checker._is_market_overlap(datetime(2026, 5, 6, 9, 29, tzinfo=hkt))
    assert checker._is_market_overlap(datetime(2026, 5, 6, 9, 30, tzinfo=hkt))
    assert checker._is_market_overlap(datetime(2026, 5, 6, 15, 0, tzinfo=hkt))
    assert not checker._is_market_overlap(datetime(2026, 5, 6, 15, 1, tzinfo=hkt))
    assert not checker._is_market_overlap(datetime(2026, 5, 9, 10, 0, tzinfo=hkt))


def test_premium_alert_suppressed_outside_market_overlap(monkeypatch):
    init_db()
    checker._recent_sends.clear()

    hk_code = "TALRT1"
    upsert_alert_rule(hk_code, 0.0)

    premium_data = {
        hk_code: {
            "premium": -1.0,
            "a_price": 10.0,
            "h_price": 9.0,
            "daily_chg": None,
        }
    }

    checker.evaluate_alerts(premium_data, fx_rate=0.9)

    sent: list[str] = []
    monkeypatch.setattr(checker, "_is_market_overlap", lambda: False)
    monkeypatch.setattr(checker, "send_alert", lambda msg: sent.append(msg) or True)

    premium_data[hk_code]["premium"] = 1.0
    events = checker.evaluate_alerts(premium_data, fx_rate=0.9)

    assert events == []
    assert sent == []
    assert any(
        row["hk_code"] == hk_code and row["event"] == "outside_market_overlap"
        for row in get_alert_history(limit=20)
    )


def test_premium_alert_sent_inside_market_overlap(monkeypatch):
    init_db()
    checker._recent_sends.clear()

    hk_code = "TALRT2"
    upsert_alert_rule(hk_code, 0.0)

    premium_data = {
        hk_code: {
            "premium": -1.0,
            "a_price": 10.0,
            "h_price": 9.0,
            "daily_chg": None,
        }
    }

    checker.evaluate_alerts(premium_data, fx_rate=0.9)

    sent: list[str] = []
    monkeypatch.setattr(checker, "_is_market_overlap", lambda: True)
    monkeypatch.setattr(checker, "send_alert", lambda msg: sent.append(msg) or True)

    premium_data[hk_code]["premium"] = 1.0
    events = checker.evaluate_alerts(premium_data, fx_rate=0.9)

    assert len(sent) == 1
    assert events == [
        {
            "event": "fired",
            "hk_code": hk_code,
            "direction": "cross_up",
            "premium": 1.0,
            "threshold": 0.0,
            "sent": True,
        }
    ]
