"""Real-time quote fetcher for H-shares (Futu) and A-shares (Sina/Tencent HTTP)."""

import logging
import re

import requests
from futu import RET_OK, OpenQuoteContext

from src.config.settings import OPEND_HOST, OPEND_PORT

logger = logging.getLogger(__name__)

_REQUEST_TIMEOUT: float = 2.0
# No proxy for real-time Sina/Tencent — HK direct connection is faster
_PROXIES: dict | None = None


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
    ctx = OpenQuoteContext(host=OPEND_HOST, port=OPEND_PORT)
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
    finally:
        ctx.close()


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
        resp = requests.get(url, headers=headers, timeout=_REQUEST_TIMEOUT, proxies=_PROXIES)
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
        resp = requests.get(url, timeout=_REQUEST_TIMEOUT, proxies=_PROXIES)
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
    ctx = OpenQuoteContext(host=OPEND_HOST, port=OPEND_PORT)
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
    finally:
        ctx.close()


def get_a_snapshots_batch(codes: list[str]) -> dict[str, dict]:
    """
    Fetch real-time snapshots for multiple A-shares in one Sina HTTP call.

    Args:
        codes: List of A-share codes, e.g. ["601939", "601398"].

    Returns:
        Dict mapping code -> snapshot dict, falling back to individual fetches on error.
    """
    if not codes:
        return {}

    headers = {"Referer": "http://finance.sina.com.cn"}
    result: dict[str, dict] = {}

    # Chunk to keep URL length reasonable and avoid Sina throttling
    _CHUNK = 50
    for i in range(0, len(codes), _CHUNK):
        chunk = codes[i : i + _CHUNK]
        symbols = [_a_code_to_sina_symbol(c) for c in chunk]
        url = f"http://hq.sinajs.cn/list={','.join(symbols)}"
        try:
            resp = requests.get(url, headers=headers, timeout=8.0, proxies=_PROXIES)
            resp.encoding = "gb2312"

            for line in resp.text.strip().split("\n"):
                match = re.search(r'hq_str_(\w+)="(.+)"', line)
                if not match:
                    continue
                symbol = match.group(1)
                fields = match.group(2).split(",")

                for code in chunk:
                    if _a_code_to_sina_symbol(code) == symbol:
                        if len(fields) >= 10:
                            price = float(fields[3])
                            prev_close = float(fields[2])
                            # Before market open, price=0 but prev_close has data
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
                        break
        except Exception as e:
            logger.warning("Sina chunk %d–%d failed: %s", i, i + len(chunk), e)

    # Fall back to Tencent for missing codes (cap at 20 to avoid timeout)
    missing = [c for c in codes if c not in result]
    if missing:
        logger.info("Sina missing %d codes, Tencent fallback (max 20)", len(missing))
    for code in missing[:20]:
        snap = _fetch_tencent(code)
        if snap:
            result[code] = snap

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
