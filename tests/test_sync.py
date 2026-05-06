import io
import json
from datetime import date, datetime

import pandas as pd

from src.data import sync
from src.storage.db import init_db
from src.storage.kline_cache import get_last_sync_date, update_sync_meta


def test_daily_snapshot_ready_uses_hk_close_buffer():
    assert not sync._is_daily_snapshot_ready(datetime(2026, 5, 6, 16, 14))
    assert sync._is_daily_snapshot_ready(datetime(2026, 5, 6, 16, 15))


def test_snapshot_has_full_ohlcv_rejects_wrong_quote_date():
    snap = {
        "price": 10.0,
        "open": 9.8,
        "high": 10.2,
        "low": 9.7,
        "volume": 1000,
        "update_time": "2026-04-30 16:10:00",
    }

    assert sync._snapshot_has_full_ohlcv(snap, "2026-04-30")
    assert not sync._snapshot_has_full_ohlcv(snap, "2026-05-01")


def test_h_share_full_backfill_stops_at_previous_h_day_before_close(monkeypatch):
    class FakeDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 5, 6)

    calls: dict[str, object] = {}

    monkeypatch.setattr(sync, "date", FakeDate)
    monkeypatch.setattr(sync, "_is_daily_snapshot_ready", lambda: False)
    monkeypatch.setattr(sync, "get_all_pairs", lambda: {"01234": {"a_code": "600001"}})
    monkeypatch.setattr(sync, "get_all_sync_meta", lambda: {("600001", "A"): "2026-05-06"})
    monkeypatch.setattr(
        sync,
        "_prev_trading_day",
        lambda d: date(2026, 5, 5) if d == date(2026, 5, 6) else date(2026, 5, 4),
    )
    monkeypatch.setattr(sync, "_prev_a_trading_day", lambda d: date(2026, 4, 30))
    monkeypatch.setattr(sync, "_is_a_trading_day", lambda d: True)

    def _fake_sync_h_hist(pairs, default_start, end_str, progress_cb, errors):
        calls["h_hist"] = (pairs, default_start, end_str)
        return 1

    monkeypatch.setattr(sync, "_sync_h_klines_hist", _fake_sync_h_hist)
    monkeypatch.setattr(sync, "get_fx_range", lambda start, end: pd.DataFrame())
    monkeypatch.setattr(sync, "_recompute_premium", lambda pairs, start, end: 1)

    sync.sync_all(_defer_ok=True)

    assert calls["h_hist"][2] == "2026-05-05"


def test_today_snapshot_sync_skips_before_close(monkeypatch):
    monkeypatch.setattr(sync, "_is_daily_snapshot_ready", lambda: False)

    saved = sync._sync_today_from_snapshots(
        {"01234": {"a_code": "600001"}},
        "2026-05-06",
        [],
    )

    assert saved == 0


def test_a_share_calendar_skips_mainland_holiday_gap(monkeypatch):
    class FakeDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 5, 6)

    monkeypatch.setattr(sync, "date", FakeDate)
    monkeypatch.setattr(sync, "get_all_pairs", lambda: {"01234": {"a_code": "600001"}})
    monkeypatch.setattr(
        sync,
        "get_all_sync_meta",
        lambda: {
            ("01234", "H"): "2026-05-06",
            ("600001", "A"): "2026-04-30",
        },
    )
    monkeypatch.setattr(sync, "_prev_a_trading_day", lambda d: date(2026, 4, 30))
    monkeypatch.setattr(sync, "_is_a_trading_day", lambda d: True)

    summary = sync.sync_all(_defer_ok=True)

    assert summary["gap_deferred"] == 0
    assert summary["today_deferred"] == 1


def test_a_share_gap_backfill_also_defers_today_snapshot(monkeypatch):
    class FakeDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 5, 6)

    monkeypatch.setattr(sync, "date", FakeDate)
    monkeypatch.setattr(sync, "get_all_pairs", lambda: {"01234": {"a_code": "600001"}})
    monkeypatch.setattr(
        sync,
        "get_all_sync_meta",
        lambda: {
            ("01234", "H"): "2026-05-06",
            ("600001", "A"): "2026-04-29",
        },
    )
    monkeypatch.setattr(sync, "_prev_a_trading_day", lambda d: date(2026, 4, 30))
    monkeypatch.setattr(sync, "_is_a_trading_day", lambda d: True)

    summary = sync.sync_all(_defer_ok=True)

    assert summary["gap_deferred"] == 1
    assert summary["today_deferred"] == 1


def test_a_share_full_backfill_updates_today_snapshot_same_run(monkeypatch):
    class FakeDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 5, 6)

    calls: dict[str, object] = {}

    monkeypatch.setattr(sync, "date", FakeDate)
    monkeypatch.setattr(sync, "get_all_pairs", lambda: {"01234": {"a_code": "600001"}})
    monkeypatch.setattr(sync, "get_all_sync_meta", lambda: {("01234", "H"): "2026-05-06"})
    monkeypatch.setattr(sync, "_prev_a_trading_day", lambda d: date(2026, 4, 30))
    monkeypatch.setattr(sync, "_is_a_trading_day", lambda d: True)

    def _fake_sync_a_hist(pairs, default_start, end_str, progress_cb, errors):
        calls["hist"] = (pairs, default_start, end_str)
        return 1

    def _fake_sync_today(pairs, today_str, errors):
        calls["today"] = (pairs, today_str)
        return 1

    monkeypatch.setattr(sync, "_sync_a_klines_hist", _fake_sync_a_hist)
    monkeypatch.setattr(sync, "_sync_today_from_snapshots", _fake_sync_today)
    monkeypatch.setattr(sync, "get_fx_range", lambda start, end: pd.DataFrame())
    monkeypatch.setattr(sync, "_recompute_premium", lambda pairs, start, end: 1)

    summary = sync.sync_all(_defer_ok=True)

    assert calls["hist"][2] == "2026-04-30"
    assert calls["today"] == ({"01234": {"a_code": "600001"}}, "2026-05-06")
    assert summary["daily_update"] == 1


def test_a_share_hist_fetch_ends_at_previous_a_trading_day(monkeypatch):
    init_db()
    update_sync_meta("600001", "A", "2026-04-29")
    calls: list[tuple[str, str, str]] = []

    def _fake_get_a_kline(code: str, start: str, end: str):
        calls.append((code, start, end))
        return pd.DataFrame(
            [
                {
                    "date": "2026-04-30",
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.0,
                    "close": 10.5,
                    "volume": 1000,
                    "turnover": 10500.0,
                }
            ]
        )

    monkeypatch.setattr(sync, "get_a_kline", _fake_get_a_kline)

    saved = sync._sync_a_klines_hist(
        {"01234": {"a_code": "600001"}},
        default_start="2000-01-01",
        end_str="2026-04-30",
        progress_cb=None,
        errors=[],
    )

    assert saved == 1
    assert calls == [("600001", "2026-04-30", "2026-04-30")]
    assert get_last_sync_date("600001", "A") == "2026-04-30"


def test_a_share_hist_fetch_skips_when_synced_to_previous_a_trading_day(monkeypatch):
    init_db()
    update_sync_meta("600002", "A", "2026-04-30")

    def _raise_if_called(*args, **kwargs):
        raise AssertionError("holiday-only A-share gap should not hit AKShare history")

    monkeypatch.setattr(sync, "get_a_kline", _raise_if_called)

    saved = sync._sync_a_klines_hist(
        {"01235": {"a_code": "600002"}},
        default_start="2000-01-01",
        end_str="2026-04-30",
        progress_cb=None,
        errors=[],
    )

    assert saved == 0


def test_a_share_calendar_falls_back_to_holiday_cn(monkeypatch):
    class FakeDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 5, 6)

    class FakeResponse(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.close()

    def _fake_urlopen(url, timeout):
        year = int(url.rsplit("/", 1)[-1].split(".", 1)[0])
        days = []
        if year == 2026:
            days = [
                {"name": "劳动节", "date": "2026-05-01", "isOffDay": True},
                {"name": "劳动节", "date": "2026-05-04", "isOffDay": True},
                {"name": "劳动节", "date": "2026-05-05", "isOffDay": True},
            ]
        payload = json.dumps({"year": year, "days": days}).encode()
        return FakeResponse(payload)

    monkeypatch.setattr(sync, "date", FakeDate)
    monkeypatch.setattr(sync.urllib.request, "urlopen", _fake_urlopen)
    monkeypatch.setattr(sync, "_a_trade_dates_cache", None)

    trade_dates = sync._get_a_trade_dates_from_holiday_cn()

    assert date(2026, 4, 30) in trade_dates
    assert date(2026, 5, 1) not in trade_dates
    assert date(2026, 5, 4) not in trade_dates
    assert date(2026, 5, 5) not in trade_dates
    assert date(2026, 5, 6) in trade_dates
