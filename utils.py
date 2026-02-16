import datetime
import logging
import os
import re
import time
from math import floor, ceil


class _SecretFilter(logging.Filter):
    """Strip private keys and sensitive hex strings from log output."""

    def __init__(self):
        super().__init__()
        key = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
        # Build list of patterns to redact
        self._patterns: list[re.Pattern] = []
        if key:
            # Match the raw key (with or without 0x prefix)
            bare = key.removeprefix("0x")
            self._patterns.append(re.compile(re.escape(key)))
            if bare != key:
                self._patterns.append(re.compile(re.escape(bare)))

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        for pat in self._patterns:
            if pat.search(msg):
                record.msg = pat.sub("[REDACTED]", str(record.msg))
                record.args = None
        return True


def setup_logger(name: str, level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")
        )
        handler.addFilter(_SecretFilter())
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


def compute_opportunity_score(
    reward_rate: float,
    book_depth_usdc: float,
    current_spread: float,
    max_spread: float,
) -> float:
    """
    Rank a market by LP opportunity.

    Higher score = high reward relative to competition, moderate spread vacancy.
    spread_vacancy is capped at 1.0 so markets with spreads wider than max_spread
    are NOT boosted (they indicate no active LPs and adverse selection risk).
    """
    reward_ratio = reward_rate / max(book_depth_usdc, 1.0)
    spread_vacancy = min(current_spread / max(max_spread, 0.001), 1.0)
    return reward_ratio * spread_vacancy


def is_peak_hours() -> bool:
    """Check if current time is during US market hours (high fill risk).

    Uses PEAK_HOURS_START/END from config (local timezone, UTC+8).
    """
    import config

    hour = datetime.datetime.now().hour
    start = config.PEAK_HOURS_START  # e.g., 22
    end = config.PEAK_HOURS_END      # e.g., 7

    # Handle overnight range (e.g., 22:00 -> 07:00)
    if start > end:
        return hour >= start or hour < end
    return start <= hour < end


def get_size_multiplier() -> float:
    """Return order size multiplier based on US market activity patterns."""
    import config
    return config.PEAK_SIZE_MULTIPLIER if is_peak_hours() else config.OFF_PEAK_SIZE_MULTIPLIER


def round_price_down(price: float, tick_size: float) -> float:
    """Round price DOWN to nearest tick (for BUY orders)."""
    # round(x, 8) before floor to fix float imprecision (e.g. 58.999... → 59.0)
    return round(floor(round(price / tick_size, 8)) * tick_size, 6)


def round_price_up(price: float, tick_size: float) -> float:
    """Round price UP to nearest tick (for SELL orders)."""
    return round(ceil(round(price / tick_size, 8)) * tick_size, 6)


def clamp_price(price: float, tick_size: float) -> float:
    """Ensure price is within valid CLOB range [tick_size, 1 - tick_size]."""
    return max(tick_size, min(price, 1.0 - tick_size))


def calculate_book_depth_in_range(
    bids: list,
    asks: list,
    midpoint: float,
    max_spread: float,
) -> float:
    """
    Sum the USDC value of orders within max_spread of midpoint.

    bids/asks are OrderSummary objects with .price and .size as strings.
    """
    depth = 0.0
    lower_bound = midpoint - max_spread
    upper_bound = midpoint + max_spread

    for bid in (bids or []):
        p = float(bid.price)
        s = float(bid.size)
        if p >= lower_bound:
            depth += p * s

    for ask in (asks or []):
        p = float(ask.price)
        s = float(ask.size)
        if p <= upper_bound:
            depth += p * s

    return depth


def safe_api_call(func, *args, retries: int = 3, **kwargs):
    """Retry wrapper for CLOB API calls with exponential backoff."""
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            err_str = str(e)
            # Don't retry client errors (4xx)
            if "400" in err_str or "401" in err_str or "403" in err_str or "404" in err_str:
                raise
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
