from datetime import datetime

from src.data import hkex_entitlements
from src.storage.db import (
    get_hkex_entitlements,
    get_hkex_entitlements_count,
    init_db,
)
from src.storage.kline_cache import get_last_sync_date


SAMPLE_HTML = """
<html><body>
<font class=textfont>
Date : 06/05/2026<br/>
</font>
<table class=textfont>
<tr><td></td><td>Stock Short Name<br />(Stock Code)</td><td></td>
<td>Description</td><td>Ex-Date</td><td>Book Closing Date</td></tr>
<tr><td></td><td>---------------</td><td></td>
<td>---------------------------------------</td><td>-----</td>
<td>---------------------</td></tr>
<tr><td>&nbsp;</td><td>CCB<br/>(939)<td></td>
<td>FINAL DIVIDEND<br/>RMB2.029 PER 10 SHARES<br/>(Y.E. 31/12/2025)</td>
<td>02/07</td><td>05/07/2026 - 10/07/2026</td></tr>
<tr><td>&nbsp;</td><td><td></td>
<td>SPECIAL DIVIDEND<br/>RMB0.01 PER SHARE</td>
<td>02/07</td><td>05/07/2026 - 10/07/2026</td></tr>
<tr><td>&nbsp;</td><td>ZIJIN MINING<br/>(2899)<td></td>
<td>FINAL DIVIDEND<br/>RMB3.8 PER 10 SHARES<br/>(Y.E. 31/12/2025)</td>
<td></td><td>TO BE ANNOUNCED</td></tr>
<tr><td>&nbsp;</td><td>AM GROUP<br/>(1849)<td></td>
<td>NIL FINAL DIVIDEND<br/>(Y.E. 30/06/2025)</td>
<td></td><td>NO B/C DATE</td></tr>
</table>
</body></html>
"""


def test_parse_hkex_entitlements_html_handles_continuation_rows():
    page_date, rows = hkex_entitlements.parse_hkex_entitlements_html(
        SAMPLE_HTML,
        source_url="https://example.test/eent.htm",
    )

    assert page_date.isoformat() == "2026-05-06"
    assert len(rows) == 4

    ccb = rows[0]
    assert ccb["stock_code"] == "00939"
    assert ccb["stock_short_name"] == "CCB"
    assert ccb["ex_date"] == "2026-07-02"
    assert ccb["status"] == "scheduled"
    assert ccb["is_dividend"] is True
    assert ccb["is_nil_dividend"] is False

    continuation = rows[1]
    assert continuation["stock_code"] == "00939"
    assert continuation["stock_short_name"] == "CCB"

    tba = rows[2]
    assert tba["stock_code"] == "02899"
    assert tba["ex_date"] is None
    assert tba["status"] == "tba"

    nil = rows[3]
    assert nil["is_dividend"] is True
    assert nil["is_nil_dividend"] is True
    assert nil["status"] == "no_book_close"


def test_sync_latest_replaces_latest_table_and_marks_today(monkeypatch):
    init_db()
    monkeypatch.setattr(
        hkex_entitlements,
        "fetch_hkex_entitlements_html",
        lambda: (SAMPLE_HTML, "https://example.test/eent.htm"),
    )

    result = hkex_entitlements.sync_latest(force=True)

    assert result["saved"] == 4
    assert result["source_page_date"] == "2026-05-06"
    assert get_hkex_entitlements_count() == 4
    assert get_last_sync_date("__hkex_entitlements__", "META") == result["last_run"]

    ccb_rows = get_hkex_entitlements(["00939"], dividends_only=True)
    assert len(ccb_rows) == 2
    assert {row["description"].split()[0] for row in ccb_rows} == {"FINAL", "SPECIAL"}


def test_classify_skips_after_daily_success(monkeypatch):
    init_db()
    monkeypatch.setattr(
        hkex_entitlements,
        "fetch_hkex_entitlements_html",
        lambda: (SAMPLE_HTML, "https://example.test/eent.htm"),
    )
    hkex_entitlements.sync_latest(force=True)

    summary = hkex_entitlements.classify(datetime.now(hkex_entitlements._HKT))

    assert summary["already_done_today"] is True
    assert summary["deferred"] == 0
    assert summary["cached_rows"] == 4
