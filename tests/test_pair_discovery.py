"""Tests for src/data/pair_discovery.py and the CSV-backed ah_mapping registry."""

import csv
import json
import os
from datetime import date
from unittest.mock import patch

import pytest

from src.data import ah_mapping, pair_discovery
from src.storage.db import (
    _get_connection,
    get_scanned_hk_codes,
    init_db,
    mark_scanned_bulk,
)
from src.storage.kline_cache import (
    get_last_discovery_date,
    update_discovery_meta,
)

# Reserved test codes — Z-prefixed so they never collide with real HK equity.
_TEST_HK_CODES = ["ZZZ01", "ZZZ02", "ZZZ03", "ZZZ04"]


def _csv_path():
    return os.environ["AH_ARB_PAIRS_CSV"]


def _read_csv_rows():
    with open(_csv_path(), encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


@pytest.fixture(autouse=True)
def _setup_and_cleanup():
    """Schema + scrub test rows from DB before & after each test.
    (CSV state is snapshot-restored by the autouse fixture in conftest.py.)"""
    init_db()
    _wipe_db_test_rows()
    yield
    _wipe_db_test_rows()


def _wipe_db_test_rows():
    conn = _get_connection()
    placeholders = ",".join("?" * len(_TEST_HK_CODES))
    conn.execute(
        f"DELETE FROM pair_discovery_scanned WHERE hk_code IN ({placeholders})",
        _TEST_HK_CODES,
    )
    conn.execute(
        "DELETE FROM sync_meta WHERE code='__pair_discovery__' AND market='META'"
    )
    conn.commit()


# ---------------------------------------------------------------------------
# CSV registry helpers
# ---------------------------------------------------------------------------


def test_add_new_pair_returns_new():
    outcome = ah_mapping.add_pair("ZZZ01", "600001", "Test One")
    assert outcome == "new"
    assert ah_mapping.get_a_code("ZZZ01") == "600001"


def test_add_existing_active_returns_unchanged():
    ah_mapping.add_pair("ZZZ01", "600001", "Test")
    again = ah_mapping.add_pair("ZZZ01", "600001", "Test")
    assert again == "unchanged"


def test_delisted_pair_is_sticky():
    """Once a pair is delisted, discovery must NOT reactivate it (HKEX-listed
    shells with no A-side data would flip back daily otherwise)."""
    ah_mapping.add_pair("ZZZ01", "600001", "Test")
    assert ah_mapping.mark_pairs_delisted(["ZZZ01"]) == 1
    assert ah_mapping.get_a_code("ZZZ01") is None
    outcome = ah_mapping.add_pair("ZZZ01", "600001", "Test")
    assert outcome == "unchanged"
    assert ah_mapping.get_a_code("ZZZ01") is None  # still delisted


def test_mark_delisted_only_affects_active():
    ah_mapping.add_pair("ZZZ01", "600001", "A")
    ah_mapping.add_pair("ZZZ02", "600002", "B")
    assert ah_mapping.mark_pairs_delisted(["ZZZ01"]) == 1
    assert ah_mapping.get_a_code("ZZZ01") is None
    assert ah_mapping.get_a_code("ZZZ02") == "600002"


def test_red_chip_flag_persisted_in_csv():
    ah_mapping.add_pair(
        "ZZZ01", "600001", "Red Chip", source="manual", is_red_chip=True
    )
    rows = {r["hk_code"]: r for r in _read_csv_rows()}
    assert rows["ZZZ01"]["is_red_chip"] == "true"
    assert rows["ZZZ01"]["source"] == "manual"


def test_csv_writes_are_sorted_by_hk_code():
    ah_mapping.add_pair("ZZZ02", "600002", "B")
    ah_mapping.add_pair("ZZZ01", "600001", "A")
    test_rows = [r for r in _read_csv_rows() if r["hk_code"] in _TEST_HK_CODES]
    codes = [r["hk_code"] for r in test_rows]
    assert codes == sorted(codes)


def test_scanned_tracking_bulk():
    mark_scanned_bulk([
        ("ZZZ01", "2026-04-16", True),
        ("ZZZ02", "2026-04-16", False),
    ])
    scanned = get_scanned_hk_codes()
    assert "ZZZ01" in scanned and "ZZZ02" in scanned


def test_discovery_meta_roundtrip():
    update_discovery_meta("2026-04-16")
    assert get_last_discovery_date() == "2026-04-16"


# ---------------------------------------------------------------------------
# classify() decision logic
# ---------------------------------------------------------------------------


def test_classify_defers_when_never_run():
    out = pair_discovery.classify()
    assert out["deferred"] == 1
    assert out["already_done_today"] is False


def test_classify_skips_when_already_done_today():
    today = date.today().strftime("%Y-%m-%d")
    update_discovery_meta(today)
    out = pair_discovery.classify()
    assert out["deferred"] == 0
    assert out["already_done_today"] is True


# ---------------------------------------------------------------------------
# discover_background() end-to-end with network mocked
# ---------------------------------------------------------------------------


def _fake_load_los_candidates():
    return {"ZZZ01", "ZZZ02"}


def _fake_query_widgets(syms):
    out = []
    for s in syms:
        if s == "ZZZ01":
            out.append(("ZZZ01", "600001", "Test Corp - H Shares"))
    return out


def _fake_tencent_no_unknown(today_str):
    """Tencent reachable; no unknown pairs (all covered)."""
    return [], {"ZZZ01", "ZZZ02"}


def _fake_akshare(ah_name_df):
    """Build a stub `akshare` module that returns the given DataFrame for
    ``stock_zh_ah_name()`` — used to mock the Tencent A+H feed in tests."""
    class _FakeAk:
        @staticmethod
        def stock_zh_ah_name():
            return ah_name_df.copy()
    return _FakeAk


def test_discover_background_incremental_first_run():
    with patch.object(pair_discovery, "_load_los_candidates", _fake_load_los_candidates), \
         patch.object(pair_discovery, "_query_widgets", _fake_query_widgets), \
         patch.object(pair_discovery, "_resolve_names_and_a_liveness", lambda p: (p, [])), \
         patch.object(pair_discovery, "_tencent_alert_unknown", _fake_tencent_no_unknown):
        result = pair_discovery.discover_background()

    assert result.get("error") is None
    assert result["scanned"] == 2
    new_codes = {p["hk_code"] for p in result["new_pairs"]}
    assert "ZZZ01" in new_codes
    assert ah_mapping.get_a_code("ZZZ01") == "600001"
    assert ah_mapping.get_a_code("ZZZ02") is None
    assert {"ZZZ01", "ZZZ02"}.issubset(get_scanned_hk_codes())


def test_discover_background_skips_already_scanned():
    mark_scanned_bulk([("ZZZ01", "2026-04-15", True)])
    ah_mapping.add_pair("ZZZ01", "600001", "Existing")

    query_calls: list[list[str]] = []

    def spying_query(syms):
        query_calls.append(list(syms))
        return _fake_query_widgets(syms)

    with patch.object(pair_discovery, "_load_los_candidates", _fake_load_los_candidates), \
         patch.object(pair_discovery, "_query_widgets", spying_query), \
         patch.object(pair_discovery, "_resolve_names_and_a_liveness", lambda p: (p, [])), \
         patch.object(pair_discovery, "_tencent_alert_unknown", _fake_tencent_no_unknown):
        pair_discovery.discover_background()

    flat = [c for call in query_calls for c in call]
    assert "ZZZ02" in flat
    assert "ZZZ01" not in flat


def test_discover_background_skips_if_already_done_today():
    today = date.today().strftime("%Y-%m-%d")
    update_discovery_meta(today)

    called = {"load": False}

    def spy_load():
        called["load"] = True
        return set()

    with patch.object(pair_discovery, "_load_los_candidates", spy_load):
        result = pair_discovery.discover_background()

    assert result.get("already_done_today") is True
    assert called["load"] is False


def test_discover_background_skips_delisting_when_tencent_unreachable():
    """Empty Tencent result must not trigger soft-delete (network failure proxy)."""
    ah_mapping.add_pair(
        "ZZZ04", "600004", "Red-chip Example", source="manual", is_red_chip=True
    )

    def _tencent_empty(today_str):
        return [], set()

    with patch.object(pair_discovery, "_load_los_candidates", _fake_load_los_candidates), \
         patch.object(pair_discovery, "_query_widgets", _fake_query_widgets), \
         patch.object(pair_discovery, "_resolve_names_and_a_liveness", lambda p: (p, [])), \
         patch.object(pair_discovery, "_tencent_alert_unknown", _tencent_empty):
        result = pair_discovery.discover_background()

    assert result["delisted"] == []
    assert ah_mapping.get_a_code("ZZZ04") == "600004"


def test_discover_background_soft_deletes_absent_pair():
    ah_mapping.add_pair("ZZZ04", "600004", "Delisted Corp")

    with patch.object(pair_discovery, "_load_los_candidates", _fake_load_los_candidates), \
         patch.object(pair_discovery, "_query_widgets", _fake_query_widgets), \
         patch.object(pair_discovery, "_resolve_names_and_a_liveness", lambda p: (p, [])), \
         patch.object(pair_discovery, "_tencent_alert_unknown", _fake_tencent_no_unknown):
        result = pair_discovery.discover_background()

    assert "ZZZ04" in result["delisted"]
    assert ah_mapping.get_a_code("ZZZ04") is None


# ---------------------------------------------------------------------------
# _query_widget — JSONP envelope + RIC parsing (network-fragile; pin the shape)
# ---------------------------------------------------------------------------


class _FakeUrlopenResponse:
    """Minimal context-manager mock for urllib.request.urlopen."""

    def __init__(self, payload: bytes):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self) -> bytes:
        return self._payload


def _jsonp_payload(quote: dict | None) -> bytes:
    """Wrap a quote dict in HKEX's ``jq(...);`` JSONP envelope."""
    body: dict = {"data": {}} if quote is None else {"data": {"quote": quote}}
    return f"jq({json.dumps(body)});".encode()


def test_query_widget_parses_shanghai_ric():
    payload = _jsonp_payload({"underlying_ric": "601939.SS", "nm": "Bank of Comm"})
    with patch("urllib.request.urlopen", return_value=_FakeUrlopenResponse(payload)):
        assert pair_discovery._query_widget(3328) == ("03328", "601939", "Bank of Comm")


def test_query_widget_parses_shenzhen_ric():
    payload = _jsonp_payload({"underlying_ric": "000651.SZ", "nm": "Gree"})
    with patch("urllib.request.urlopen", return_value=_FakeUrlopenResponse(payload)):
        assert pair_discovery._query_widget(2126) == ("02126", "000651", "Gree")


def test_query_widget_returns_none_on_malformed_ric():
    payload = _jsonp_payload({"underlying_ric": "not-a-ric", "nm": "Foo"})
    with patch("urllib.request.urlopen", return_value=_FakeUrlopenResponse(payload)):
        assert pair_discovery._query_widget(123) is None


def test_query_widget_returns_none_when_quote_missing():
    payload = _jsonp_payload(None)
    with patch("urllib.request.urlopen", return_value=_FakeUrlopenResponse(payload)):
        assert pair_discovery._query_widget(123) is None


def test_query_widget_returns_none_on_http_error():
    with patch("urllib.request.urlopen", side_effect=OSError("widget down")):
        assert pair_discovery._query_widget(123) is None


# ---------------------------------------------------------------------------
# Tencent cross-check / Telegram alert path
# ---------------------------------------------------------------------------


def test_tencent_unknown_triggers_alert_and_marks_scanned():
    """Tencent reports a HK code we don't have → Telegram alert sent, code marked
    scanned so we don't alert again tomorrow."""
    import pandas as pd

    fake_df = pd.DataFrame({"hk_code": ["ZZZ03"], "name": ["神秘红筹"]})
    sent: list[str] = []

    def _fake_send_alert(text, **kwargs):
        sent.append(text)
        return True

    with patch.dict("sys.modules", {"akshare": _fake_akshare(fake_df)}), \
         patch("src.alerts.telegram.send_alert", _fake_send_alert):
        alerts, tencent_hk = pair_discovery._tencent_alert_unknown("2026-04-16")

    assert len(alerts) == 1
    assert alerts[0]["hk_code"] == "ZZZ03"
    assert "ZZZ03" in tencent_hk
    assert len(sent) == 1
    assert "ZZZ03" in sent[0]
    assert "神秘红筹" in sent[0]
    # Marked scanned → won't re-alert
    assert "ZZZ03" in get_scanned_hk_codes()


def test_dead_a_pair_alerts_and_skips_csv():
    """When HKEX widget points at an A code that's no longer in A-spot, the
    pair should be Telegram-alerted and NOT written to the CSV (otherwise
    daily K-line sync wastes ~55s/page-load chunking through empty Tencent
    responses, e.g. 00042 -> 000585 东电退)."""
    sent: list[str] = []

    def _fake_resolve(pairs):
        # Simulate: ZZZ02's A code is delisted; ZZZ01 is alive
        live = [p for p in pairs if p[0] != "ZZZ02"]
        dead = [p for p in pairs if p[0] == "ZZZ02"]
        return live, dead

    def _fake_send_alert(text, **kwargs):
        sent.append(text)
        return True

    def _fake_widgets_two(syms):
        out = []
        for s in syms:
            if s == "ZZZ01":
                out.append(("ZZZ01", "600001", "Live Corp"))
            elif s == "ZZZ02":
                out.append(("ZZZ02", "999999", "DeadA Corp"))
        return out

    with patch.object(pair_discovery, "_load_los_candidates", _fake_load_los_candidates), \
         patch.object(pair_discovery, "_query_widgets", _fake_widgets_two), \
         patch.object(pair_discovery, "_resolve_names_and_a_liveness", _fake_resolve), \
         patch.object(pair_discovery, "_tencent_alert_unknown", _fake_tencent_no_unknown), \
         patch("src.alerts.telegram.send_alert", _fake_send_alert):
        result = pair_discovery.discover_background()

    # ZZZ01 added; ZZZ02 NOT added (dead A)
    assert ah_mapping.get_a_code("ZZZ01") == "600001"
    assert ah_mapping.get_a_code("ZZZ02") is None
    # Dead-A alert sent
    assert any(p["hk_code"] == "ZZZ02" and p.get("kind") == "dead_a" for p in result["alerts"])
    assert len(sent) == 1
    assert "ZZZ02" in sent[0]
    assert "999999" in sent[0]
    # Both marked scanned so neither re-fires next day
    assert {"ZZZ01", "ZZZ02"}.issubset(get_scanned_hk_codes())


def test_tencent_unknown_skips_known_pairs():
    """Pairs already in CSV must not trigger alerts."""
    import pandas as pd

    ah_mapping.add_pair(
        "ZZZ03", "600003", "Already Known", source="manual", is_red_chip=True
    )
    fake_df = pd.DataFrame({"hk_code": ["ZZZ03"], "name": ["Already Known"]})
    sent: list[str] = []

    with patch.dict("sys.modules", {"akshare": _fake_akshare(fake_df)}), \
         patch("src.alerts.telegram.send_alert", lambda *a, **k: sent.append(a) or True):
        alerts, _ = pair_discovery._tencent_alert_unknown("2026-04-16")

    assert alerts == []
    assert sent == []


# ---------------------------------------------------------------------------
# ah_mapping read API
# ---------------------------------------------------------------------------


def test_ah_mapping_refresh_includes_new_pair():
    ah_mapping.add_pair("ZZZ03", "600003", "Refresh Test")
    assert ah_mapping.get_a_code("ZZZ03") == "600003"
    assert ah_mapping.get_hk_code("600003") == "ZZZ03"
    assert "ZZZ03" in ah_mapping.get_all_pairs()


def test_ah_mapping_csv_seed_has_real_pairs():
    """Sanity: the CSV the conftest copied in has the production pair count."""
    pairs = ah_mapping.get_all_pairs()
    assert len(pairs) >= 100  # production CSV has 186 entries
