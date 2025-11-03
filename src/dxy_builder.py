# dxy_builder.py
# Builds a DXY candle from six OANDA source pairs once all have arrived for the same minute.
# Plug directly into DataCollector: after each publish_candle(), call await on_source_candle(nc, candle)

import asyncio
import time
from datetime import datetime, timezone
from typing import Dict, Set

from quantflow_publisher import publish_candle
from nats.aio.client import Client as NATS

# ===== Configuration ==========================================================

PAIRS: Set[str] = {
    "EUR/USD", "USD/JPY", "GBP/USD", "USD/CAD", "USD/SEK", "USD/CHF"
}

TIMEFRAME = "1m"
EXCHANGE_OUT = "OANDA"
SYMBOL_OUT = "DXY"
PRICE_MODE = "mid"

CTX_TTL_SEC = 300  # cleanup window

_K = 50.14348112
_WEIGHTS = {
    "EUR/USD": -0.576,
    "USD/JPY": 0.136,
    "GBP/USD": -0.119,
    "USD/CAD": 0.091,
    "USD/SEK": 0.042,
    "USD/CHF": 0.036,
}


# ===== Helpers ================================================================

def _as_aware_utc(x):
    """Convert string or naive datetime to timezone-aware UTC datetime."""
    if isinstance(x, datetime):
        return x if x.tzinfo else x.replace(tzinfo=timezone.utc)
    dt = datetime.fromisoformat(str(x).replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# ===== Core Builder ===========================================================

class _MinuteContext:
    __slots__ = ("created", "pairs", "created_ts")

    def __init__(self):
        self.created = False
        self.pairs: Dict[str, dict] = {}
        self.created_ts = time.time()


class _DXYBuilder:
    def __init__(self):
        self.ctx: Dict[tuple, _MinuteContext] = {}

    async def on_source_candle(self, nc: NATS, candle: dict) -> None:
        tf = candle.get("timeframe")
        if tf != TIMEFRAME:
            return

        symbol = candle.get("symbol")
        if symbol not in PAIRS:
            return

        close_time = candle.get("close_time")
        if not close_time:
            return

        close_dt = _as_aware_utc(close_time)
        key = (tf, close_dt.isoformat())
        ctx = self.ctx.get(key)
        if ctx is None:
            ctx = _MinuteContext()
            self.ctx[key] = ctx

        ctx.pairs[symbol] = candle
        if ctx.created:
            self._maybe_cleanup()
            return

        # wait for all 6 pairs
        if not PAIRS.issubset(ctx.pairs.keys()):
            self._maybe_cleanup()
            return

        # All 6 pairs ready — build DXY
        ref = ctx.pairs["EUR/USD"]
        open_time_dt = _as_aware_utc(ref["open_time"])
        price_mode = candle.get("price_mode", PRICE_MODE)

        def _field(name: str) -> float:
            val = _K
            for p in PAIRS:
                v = ctx.pairs[p][name]
                val *= (float(v) ** _WEIGHTS[p])
            return float(val)

        dxy_o = _field("open")
        dxy_h = _field("high")
        dxy_l = _field("low")
        dxy_c = _field("close")

        dxy_candle = {
            "exchange": EXCHANGE_OUT,
            "symbol": SYMBOL_OUT,
            "base_currency": "DXY",
            "quote_currency": "DXY",
            "timeframe": tf,
            "open_time": open_time_dt,
            "open": dxy_o,
            "high": dxy_h,
            "low": dxy_l,
            "close": dxy_c,
            "volume": 0,
            "close_time": close_dt,
            "price_mode": price_mode,
        }

        await publish_candle(nc, candle=dxy_candle)
        ctx.created = True
        self._maybe_cleanup()

    def _maybe_cleanup(self):
        now = time.time()
        expired = [k for k, c in self.ctx.items() if now - c.created_ts > CTX_TTL_SEC]
        for k in expired:
            self.ctx.pop(k, None)


# ===== Singleton ==============================================================
_BUILDER = _DXYBuilder()

async def on_source_candle(nc: NATS, candle: dict) -> None:
    await _BUILDER.on_source_candle(nc, candle)
