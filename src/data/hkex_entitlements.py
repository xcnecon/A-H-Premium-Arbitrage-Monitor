"""HKEX Dividends & Other Entitlements latest-table sync.

HKEX publishes the current Main Board entitlements table as static HTML:
https://www3.hkexnews.hk/reports/doe/eent.htm

The table is not a historical feed. HKEX removes entries after the first book
closing date or when there is no further progress update, so this module keeps
only the latest table and refreshes it once per local day at app startup.
"""

from __future__ import annotations

import logging
import re
import threading
import urllib.request
from datetime import date, datetime, timedelta
from html.parser import HTMLParser
from typing import Any
from zoneinfo import ZoneInfo

from src.storage.db import get_hkex_entitlements_count, replace_hkex_entitlements
from src.storage.kline_cache import get_last_sync_date, update_sync_meta

logger = logging.getLogger(__name__)

_DOE_URLS = (
    "https://www3.hkexnews.hk/reports/doe/eent.htm",
    "https://www.hkexnews.hk/reports/doe/eent.htm",
)
_HEADERS = {"User-Agent": "Mozilla/5.0"}
_REQUEST_TIMEOUT = 30
_META_KEY = ("__hkex_entitlements__", "META")
_HKT = ZoneInfo("Asia/Hong_Kong")
_sync_lock = threading.Lock()


class _TableTextParser(HTMLParser):
    """Extract table rows from HKEX's old, mildly malformed HTML."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell_parts: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tr":
            self._row = []
            self._cell_parts = None
        elif tag == "td" and self._row is not None:
            # Some HKEX rows start a new <td> before closing the previous one.
            self._close_cell()
            self._cell_parts = []
        elif tag == "br" and self._cell_parts is not None:
            self._cell_parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._cell_parts is not None:
            self._cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "td":
            self._close_cell()
        elif tag == "tr":
            self._close_cell()
            if self._row is not None:
                self.rows.append(self._row)
            self._row = None

    def _close_cell(self) -> None:
        if self._row is None or self._cell_parts is None:
            return
        self._row.append(_normalize_text("".join(self._cell_parts)))
        self._cell_parts = None


def _normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(value.replace("\xa0", " ").split()).strip()


def _split_name_code(value: str) -> tuple[str, str]:
    match = re.search(r"^(.*?)\s*\((\d{1,5})\)\s*$", value)
    if not match:
        return value.strip(), ""
    return match.group(1).strip(), match.group(2).zfill(5)


def _parse_page_date(html: str) -> date:
    match = re.search(r"Date\s*:\s*(\d{2}/\d{2}/\d{4})", html)
    if not match:
        raise ValueError("HKEX DOE page date not found")
    return datetime.strptime(match.group(1), "%d/%m/%Y").date()


def _parse_ex_date(raw_ex_date: str, raw_book_close: str, page_date: date) -> date | None:
    match = re.search(r"(\d{1,2})/(\d{1,2})(?:/(\d{4}))?", raw_ex_date or "")
    if not match:
        return None

    day = int(match.group(1))
    month = int(match.group(2))
    year = int(match.group(3)) if match.group(3) else None
    if year is None:
        book_match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw_book_close or "")
        year = int(book_match.group(3)) if book_match else page_date.year

    parsed = date(year, month, day)
    if parsed < page_date - timedelta(days=180):
        parsed = date(year + 1, month, day)
    elif parsed > page_date + timedelta(days=540):
        parsed = date(year - 1, month, day)
    return parsed


def _status(ex_date: date | None, raw_book_close: str) -> str:
    upper = (raw_book_close or "").upper()
    if "TO BE ANNOUNCED" in upper:
        return "tba"
    if "NO B/C DATE" in upper:
        return "no_book_close"
    if ex_date is not None:
        return "scheduled"
    return "unknown"


def _is_dividend(description: str) -> bool:
    return "DIVIDEND" in description.upper()


def _is_nil_dividend(description: str) -> bool:
    upper = description.upper()
    return "NIL" in upper and "DIVIDEND" in upper


def parse_hkex_entitlements_html(
    html: str,
    *,
    source_url: str = _DOE_URLS[0],
) -> tuple[date, list[dict[str, Any]]]:
    """Parse HKEX DOE HTML into normalized entitlement rows."""
    page_date = _parse_page_date(html)
    parser = _TableTextParser()
    parser.feed(html)

    records: list[dict[str, Any]] = []
    last_name = ""
    last_code = ""
    for row in parser.rows:
        if len(row) < 6:
            continue

        name_code = _normalize_text(row[1])
        description = _normalize_text(row[3])
        ex_date_raw = _normalize_text(row[4])
        book_close_raw = _normalize_text(row[5])

        if not description or description in {
            "Description",
            "---------------------------------------",
        }:
            continue
        if name_code and name_code != "Stock Short Name (Stock Code)":
            name, code = _split_name_code(name_code)
            if code:
                last_name = name
                last_code = code

        if not last_code:
            continue

        ex_date = _parse_ex_date(ex_date_raw, book_close_raw, page_date)
        records.append(
            {
                "source_page_date": page_date.isoformat(),
                "source_url": source_url,
                "sort_order": len(records),
                "stock_code": last_code,
                "stock_short_name": last_name,
                "description": description,
                "ex_date": ex_date.isoformat() if ex_date else None,
                "ex_date_raw": ex_date_raw or None,
                "book_close_raw": book_close_raw or None,
                "status": _status(ex_date, book_close_raw),
                "is_dividend": _is_dividend(description),
                "is_nil_dividend": _is_nil_dividend(description),
            }
        )

    if not records:
        raise ValueError("HKEX DOE table parsed successfully but yielded no records")
    return page_date, records


def fetch_hkex_entitlements_html() -> tuple[str, str]:
    """Fetch the current HKEX DOE HTML and return (html, source_url)."""
    last_error: Exception | None = None
    for url in _DOE_URLS:
        try:
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT) as resp:
                raw = resp.read()
            return raw.decode("utf-8", errors="replace"), url
        except Exception as exc:
            last_error = exc
            logger.warning("HKEX DOE fetch failed from %s: %s", url, exc)
    raise RuntimeError(f"HKEX DOE fetch failed: {last_error}")


def classify(now: datetime | None = None) -> dict[str, Any]:
    """Return whether a HKEX DOE refresh should be started in the background."""
    current = now or datetime.now(_HKT)
    current = current.replace(tzinfo=_HKT) if current.tzinfo is None else current.astimezone(_HKT)
    today_str = current.date().isoformat()
    last = get_last_sync_date(*_META_KEY)
    cached_rows = get_hkex_entitlements_count()
    if last and last >= today_str and cached_rows > 0:
        return {
            "already_done_today": True,
            "deferred": 0,
            "last_run": last,
            "cached_rows": cached_rows,
        }
    return {
        "already_done_today": False,
        "deferred": 1,
        "last_run": last,
        "cached_rows": cached_rows,
    }


def sync_latest(force: bool = False) -> dict[str, Any]:
    """Fetch, parse, and replace the cached latest HKEX DOE table."""
    if not force:
        summary = classify()
        if not summary.get("deferred"):
            return {"skipped": True, **summary}

    html, source_url = fetch_hkex_entitlements_html()
    page_date, rows = parse_hkex_entitlements_html(html, source_url=source_url)
    saved = replace_hkex_entitlements(rows)
    today_str = datetime.now(_HKT).date().isoformat()
    update_sync_meta(*_META_KEY, today_str)
    result = {
        "saved": saved,
        "source_page_date": page_date.isoformat(),
        "source_url": source_url,
        "last_run": today_str,
    }
    logger.info("HKEX DOE sync complete: %s", result)
    return result


def sync_background() -> dict[str, Any]:
    """Run HKEX DOE sync in a background-safe, duplicate-proof wrapper."""
    if not _sync_lock.acquire(blocking=False):
        logger.info("HKEX DOE sync already running")
        return {"skipped": True, "reason": "already_running"}
    try:
        return sync_latest(force=False)
    except Exception as exc:
        logger.exception("HKEX DOE sync failed")
        return {"error": str(exc)}
    finally:
        _sync_lock.release()
