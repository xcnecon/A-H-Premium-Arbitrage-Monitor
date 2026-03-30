"""Singleton Futu OpenQuoteContext for the application lifetime.

OpenQuoteContext creation involves TCP handshake + InitConnect protocol +
thread spawning (~10-50ms). Reusing a single context eliminates this
overhead on every snapshot call. The context is thread-safe (RLock-based).
"""

import logging
import threading

from futu import OpenQuoteContext

from src.config.settings import OPEND_HOST, OPEND_PORT

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_ctx: OpenQuoteContext | None = None


def get_quote_ctx() -> OpenQuoteContext:
    """Return the shared OpenQuoteContext, creating it on first call."""
    global _ctx
    with _lock:
        if _ctx is None:
            logger.info("Creating singleton OpenQuoteContext -> %s:%s", OPEND_HOST, OPEND_PORT)
            _ctx = OpenQuoteContext(host=OPEND_HOST, port=OPEND_PORT)
        return _ctx


def close_quote_ctx() -> None:
    """Close the shared context (call at app shutdown)."""
    global _ctx
    with _lock:
        if _ctx is not None:
            logger.info("Closing singleton OpenQuoteContext")
            _ctx.close()
            _ctx = None
