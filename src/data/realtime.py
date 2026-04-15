"""Real-time quote fetcher for H-shares (Futu) and A-shares (Sina/Tencent HTTP)."""

import logging
import re
import time

import requests
from futu import RET_OK

from src.config.settings import A_SHARE_PROXY
from src.data.futu_ctx import get_quote_ctx

logger = logging.getLogger(__name__)

_REQUEST_TIMEOUT: float = 5.0
_BATCH_TIMEOUT: float = 15.0  # longer timeout for batch through proxy
_RETRY_TIMEOUT: float = 20.0  # even longer for retry attempts
_PROXIES: dict | None = A_SHARE_PROXY

# ---------------------------------------------------------------------------
# Shared requests.Session — reuses TCP connections to the proxy, avoiding
# repeated handshake overhead on every request.
# ---------------------------------------------------------------------------
_session: requests.Session | None = None


def _get_session() -> requests.Session:
    """Lazily create a requests.Session with proxy + connection pooling."""
    global _session
    if _session is not None:
        return _session
    s = requests.Session()
    if _PROXIES:
        s.proxies.update(_PROXIES)
    s.headers.update({"Referer": "http://finance.sina.com.cn"})
    _session = s
    return _session


# ---------------------------------------------------------------------------
# H-share: Futu get_market_snapshot
# ---------------------------------------------------------------------------


def get_h_snapshot(code: str) -> dict | None:
    """
    Fetch a real-time snapshot for an H-share via Futu OpenD.

    Args:
        code: HK stock code, e.g. "00939" (will be formatted as "HK.00939").

    Returns:
        Dict with keys price, open, high, low, volume, turnover, update_time,
        or None on failure.
    """
    futu_code = f"HK.{code}" if not code.startswith("HK.") else code
    ctx = get_quote_ctx()
    try:
        ret, data = ctx.get_market_snapshot([futu_code])
        if ret != RET_OK or data.empty:
            logger.error("Futu get_market_snapshot failed for %s: %s", futu_code, data)
            return None

        row = data.iloc[0]
        return {
            "price": float(row["last_price"]),
            "open": float(row["open_price"]),
            "high": float(row["high_price"]),
            "low": float(row["low_price"]),
            "volume": int(row["volume"]),
            "turnover": float(row["turnover"]),
            "update_time": str(row["update_time"]),
            "prev_close": float(row["prev_close_price"]),
        }
    except Exception as e:
        logger.error("Futu snapshot error for %s: %s", futu_code, e)
        return None


# ---------------------------------------------------------------------------
# A-share: Sina / Tencent HTTP real-time quote
# ---------------------------------------------------------------------------


def _a_code_to_sina_symbol(a_code: str) -> str:
    """Convert a bare A-share code to Sina symbol (sh601939 / sz000001)."""
    if a_code.startswith(("6", "9")):
        return f"sh{a_code}"
    return f"sz{a_code}"


def _a_code_to_tencent_symbol(a_code: str) -> str:
    """Convert a bare A-share code to Tencent symbol (sh601939 / sz000001)."""
    if a_code.startswith(("6", "9")):
        return f"sh{a_code}"
    return f"sz{a_code}"


def _fetch_sina(a_code: str) -> dict | None:
    """
    Try fetching a real-time A-share quote from the Sina HTTP API.

    Response format (GB2312-encoded):
    var hq_str_sh601939="name,open,pre_close,price,high,low,...,volume,turnover,...";

    Key field indices (0-based after splitting by comma):
        [1] open, [2] pre_close, [3] current_price, [4] high, [5] low,
        [8] volume (shares), [9] turnover (CNY)
    """
    symbol = _a_code_to_sina_symbol(a_code)
    url = f"http://hq.sinajs.cn/list={symbol}"
    headers = {"Referer": "http://finance.sina.com.cn"}

    try:
        resp = _get_session().get(url, headers=headers, timeout=_REQUEST_TIMEOUT)
        resp.encoding = "gb2312"
        text = resp.text.strip()

        if not text or '=""' in text:
            logger.warning("Sina returned empty data for %s", a_code)
            return None

        # Extract the quoted CSV part
        match = re.search(r'"(.+)"', text)
        if not match:
            logger.warning("Sina response parse error for %s: %s", a_code, text[:120])
            return None

        fields = match.group(1).split(",")
        if len(fields) < 10:
            logger.warning("Sina response has too few fields for %s (%d)", a_code, len(fields))
            return None

        price = float(fields[3])
        if price <= 0:
            logger.warning("Sina returned zero/negative price for %s", a_code)
            return None

        return {
            "price": price,
            "open": float(fields[1]),
            "high": float(fields[4]),
            "low": float(fields[5]),
            "volume": int(float(fields[8])),
            "turnover": float(fields[9]),
            "prev_close": float(fields[2]),
        }
    except Exception as e:
        logger.warning("Sina fetch failed for %s: %s", a_code, e)
        return None


def _fetch_tencent(a_code: str) -> dict | None:
    """
    Try fetching a real-time A-share quote from the Tencent HTTP API.

    Response format (GB2312-encoded):
    v_sh601939="1~股票名~代码~当前价~昨收~今开~成交量(手)~外盘~内盘~买一~买一量~...
                ~最高~最低~...~成交额(万)~..."

    Key field indices (0-based after splitting by '~'):
        [3] current_price, [4] pre_close, [5] open,
        [6] volume (手 = 100 shares), [33] high, [34] low,
        [37] turnover (万元 = 10000 CNY)
    """
    symbol = _a_code_to_tencent_symbol(a_code)
    url = f"https://qt.gtimg.cn/q={symbol}"

    try:
        resp = _get_session().get(url, timeout=_REQUEST_TIMEOUT)
        resp.encoding = "gb2312"
        text = resp.text.strip()

        if not text or '=""' in text:
            logger.warning("Tencent returned empty data for %s", a_code)
            return None

        match = re.search(r'"(.+)"', text)
        if not match:
            logger.warning("Tencent response parse error for %s: %s", a_code, text[:120])
            return None

        fields = match.group(1).split("~")
        if len(fields) < 38:
            logger.warning("Tencent response has too few fields for %s (%d)", a_code, len(fields))
            return None

        price = float(fields[3])
        if price <= 0:
            logger.warning("Tencent returned zero/negative price for %s", a_code)
            return None

        # Volume: Tencent reports in 手 (lots of 100 shares)
        volume_lots = float(fields[6])
        volume_shares = int(volume_lots * 100)

        # Turnover: Tencent reports in 万元 (10k CNY)
        turnover_wan = float(fields[37])
        turnover_cny = turnover_wan * 10_000

        return {
            "price": price,
            "open": float(fields[5]),
            "high": float(fields[33]),
            "low": float(fields[34]),
            "volume": volume_shares,
            "turnover": turnover_cny,
            "prev_close": float(fields[4]),
        }
    except Exception as e:
        logger.warning("Tencent fetch failed for %s: %s", a_code, e)
        return None


def get_h_snapshots_batch(codes: list[str]) -> dict[str, dict]:
    """
    Fetch real-time snapshots for multiple H-shares in one Futu call.

    Args:
        codes: List of HK stock codes, e.g. ["00939", "01398"].

    Returns:
        Dict mapping code -> snapshot dict (price, open, high, low, volume, turnover).
    """
    if not codes:
        return {}
    futu_codes = [f"HK.{c}" if not c.startswith("HK.") else c for c in codes]
    ctx = get_quote_ctx()
    try:
        ret, data = ctx.get_market_snapshot(futu_codes)
        if ret != RET_OK:
            logger.error("Futu batch snapshot failed: %s", data)
            return {}
        result: dict[str, dict] = {}
        for _, row in data.iterrows():
            code = str(row["code"]).replace("HK.", "")
            result[code] = {
                "price": float(row["last_price"]),
                "open": float(row["open_price"]),
                "high": float(row["high_price"]),
                "low": float(row["low_price"]),
                "volume": int(row["volume"]),
                "turnover": float(row["turnover"]),
                "prev_close": float(row["prev_close_price"]),
            }
        return result
    except Exception as e:
        logger.error("Futu batch snapshot error: %s", e)
        return {}


def _fetch_tencent_batch(codes: list[str]) -> dict[str, dict]:
    """
    Fetch real-time A-share quotes in batch via Tencent HTTP API.

    Tencent supports comma-separated symbols in a single URL, similar to Sina.
    Response is multi-line, each line: v_sh601939="1~name~code~price~..."
    """
    if not codes:
        return {}

    result: dict[str, dict] = {}
    # Build reverse lookup: tencent_symbol -> original code
    sym_to_code = {_a_code_to_tencent_symbol(c): c for c in codes}
    symbols = list(sym_to_code.keys())

    _CHUNK = 50
    for i in range(0, len(symbols), _CHUNK):
        chunk_syms = symbols[i : i + _CHUNK]
        url = f"https://qt.gtimg.cn/q={','.join(chunk_syms)}"
        try:
            resp = _get_session().get(url, timeout=_BATCH_TIMEOUT)
            resp.encoding = "gb2312"

            for line in resp.text.strip().split("\n"):
                match = re.search(r'v_(\w+)="(.+)"', line)
                if not match:
                    continue
                symbol = match.group(1)
                fields = match.group(2).split("~")

                code = sym_to_code.get(symbol)
                if code is None or len(fields) < 38:
                    continue

                price = float(fields[3])
                prev_close = float(fields[4])
                if price <= 0 and prev_close > 0:
                    price = prev_close
                if price <= 0:
                    continue

                volume_lots = float(fields[6])
                turnover_wan = float(fields[37])
                result[code] = {
                    "price": price,
                    "open": float(fields[5]) or prev_close,
                    "high": float(fields[33]) or price,
                    "low": float(fields[34]) or price,
                    "volume": int(volume_lots * 100),
                    "turnover": turnover_wan * 10_000,
                    "prev_close": prev_close,
                }
        except Exception as e:
            logger.warning("Tencent batch chunk %d–%d failed: %s", i, i + len(chunk_syms), e)

    return result


def _sina_batch_chunk(
    chunk: list[str], timeout: float,
) -> dict[str, dict]:
    """Fetch one chunk of A-share codes from Sina. Returns code->snapshot dict."""
    result: dict[str, dict] = {}
    symbols = [_a_code_to_sina_symbol(c) for c in chunk]
    # rn= timestamp prevents edge/proxy caching
    url = f"http://hq.sinajs.cn/rn={int(time.time() * 1000)}&list={','.join(symbols)}"
    headers = {"Referer": "http://finance.sina.com.cn"}

    resp = _get_session().get(url, headers=headers, timeout=timeout)
    resp.encoding = "gb2312"

    # Build reverse lookup for O(1) matching
    sym_to_code = {_a_code_to_sina_symbol(c): c for c in chunk}

    for line in resp.text.strip().split("\n"):
        match = re.search(r'hq_str_(\w+)="(.+)"', line)
        if not match:
            continue
        symbol = match.group(1)
        fields = match.group(2).split(",")
        code = sym_to_code.get(symbol)
        if code is None or len(fields) < 10:
            continue

        price = float(fields[3])
        prev_close = float(fields[2])
        if price <= 0 and prev_close > 0:
            price = prev_close
        if price > 0:
            result[code] = {
                "price": price,
                "open": float(fields[1]) or prev_close,
                "high": float(fields[4]) or price,
                "low": float(fields[5]) or price,
                "volume": int(float(fields[8])),
                "turnover": float(fields[9]),
                "prev_close": prev_close,
            }
    return result


def get_a_snapshots_batch(codes: list[str]) -> dict[str, dict]:
    """
    Fetch real-time snapshots for multiple A-shares via Sina, with Tencent
    batch fallback for any codes that Sina fails to return.

    Improvements over naive approach:
    - Connection pooling via requests.Session (reuses proxy TCP connections)
    - rn= timestamp on Sina URLs to prevent proxy/edge caching
    - Automatic retry (once) for failed Sina chunks with longer timeout
    - Tencent batch fallback for ALL missing codes (no cap)
    """
    if not codes:
        return {}

    result: dict[str, dict] = {}

    # Chunk to keep URL length reasonable
    _CHUNK = 50
    for i in range(0, len(codes), _CHUNK):
        chunk = codes[i : i + _CHUNK]

        # First attempt
        try:
            result.update(_sina_batch_chunk(chunk, timeout=_BATCH_TIMEOUT))
            continue  # success — skip retry
        except Exception as e:
            logger.warning("Sina chunk %d–%d failed (attempt 1): %s", i, i + len(chunk), e)

        # Retry once with longer timeout
        time.sleep(1.0)
        try:
            result.update(_sina_batch_chunk(chunk, timeout=_RETRY_TIMEOUT))
        except Exception as e:
            logger.warning("Sina chunk %d–%d failed (attempt 2): %s", i, i + len(chunk), e)

    # Tencent batch fallback for ALL missing codes (no cap)
    missing = [c for c in codes if c not in result]
    if missing:
        logger.info("Sina missing %d codes, Tencent batch fallback", len(missing))
        tencent_result = _fetch_tencent_batch(missing)
        result.update(tencent_result)
        still_missing = len(missing) - len(tencent_result)
        if still_missing:
            logger.warning("Still missing %d codes after Tencent fallback", still_missing)

    return result


def get_a_snapshot(code: str) -> dict | None:
    """
    Fetch a real-time snapshot for an A-share.

    Tries the Sina HTTP API first; falls back to Tencent if Sina fails.

    Args:
        code: A-share code, e.g. "601939".

    Returns:
        Dict with keys price, open, high, low, volume, turnover,
        or None on failure.
    """
    result = _fetch_sina(code)
    if result is not None:
        return result

    logger.info("Sina failed for %s, falling back to Tencent", code)
    result = _fetch_tencent(code)
    if result is not None:
        return result

    logger.error("Both Sina and Tencent failed for A-share %s", code)
    return None
