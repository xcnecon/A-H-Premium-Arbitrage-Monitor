"""Configuration settings for the A/H Premium Arbitrage Monitor.

Loads user-configurable values and secrets from environment variables
(with .env file support via python-dotenv). Constants remain hardcoded.
"""

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ─── Futu OpenD gateway connection (user-configurable) ───
OPEND_HOST: str = os.getenv("OPEND_HOST", "127.0.0.1")
OPEND_PORT: int = int(os.getenv("OPEND_PORT", "11111"))

# ─── Database (user-configurable directory) ───
_db_dir_env: str | None = os.getenv("AH_ARB_DB_DIR")
DB_DIR: Path = Path(_db_dir_env) if _db_dir_env else Path.home() / ".ah-arb"
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH: Path = DB_DIR / "data.db"

# ─── Constants (not configurable via env) ───
DEFAULT_LOOKBACK_DAYS: int = 180

# Market overlap hours (UTC+8) — both A-share and H-share markets open
MARKET_OVERLAP_START: str = "09:30"
MARKET_OVERLAP_END: str = "15:00"

# K-line cache sync
# First sync fetches all available history; subsequent runs only delta
KLINE_HISTORY_START: str = "2000-01-01"
SYNC_DELAY_FUTU: float = 0.5  # seconds between Futu API calls during sync
SYNC_DELAY_A: float = 0.3  # seconds between A-share API calls during sync

# Default FX rate (CNH per 1 HKD) — used when all FX sources fail
DEFAULT_FX_RATE: float = 0.9170

# Alert defaults
ALERT_MAX_PER_MINUTE: int = 5  # global rate limit
MAX_ALERTS_PER_STOCK: int = 3  # max crossover thresholds per stock
ALERT_BUFFER_PCT: float = float(os.getenv("ALERT_BUFFER_PCT", "0.1"))  # hysteresis buffer (pp)

# ─── China proxy for A-share APIs (user-configurable) ───
_proxy_url: str | None = os.getenv("A_SHARE_PROXY_URL")
A_SHARE_PROXY: dict | None = {"http": _proxy_url, "https": _proxy_url} if _proxy_url else None

# ─── Yahoo Finance / general HTTPS proxy (user-configurable) ───
# For users in mainland China where Yahoo is blocked, or for routing through a specific proxy.
# Set YAHOO_PROXY_URL in .env (e.g. "http://127.0.0.1:7890" or "socks5://127.0.0.1:1080")
_yahoo_proxy: str | None = os.getenv("YAHOO_PROXY_URL")
YAHOO_PROXY: dict | None = {"http": _yahoo_proxy, "https": _yahoo_proxy} if _yahoo_proxy else None

# Network timeouts for external APIs (seconds)
YAHOO_TIMEOUT: float = float(os.getenv("YAHOO_TIMEOUT", "10"))

# ─── Thread pool size for parallel historical sync (user-configurable) ───
SYNC_A_WORKERS: int = int(os.getenv("SYNC_A_WORKERS", "10"))
SYNC_H_WORKERS: int = int(os.getenv("SYNC_H_WORKERS", "4"))

# ─── Secrets (no defaults — must be set in env / .env) ───
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

if not TELEGRAM_BOT_TOKEN:
    logger.warning("TELEGRAM_BOT_TOKEN is not set — Telegram alerts will be disabled")
if not TELEGRAM_CHAT_ID:
    logger.warning("TELEGRAM_CHAT_ID is not set — Telegram alerts will be disabled")
