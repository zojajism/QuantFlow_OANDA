import asyncio
import json
import logging
import random
from datetime import datetime, timezone, timedelta, time
from typing import Dict, Tuple, Optional
import datetime as dt
import aiohttp
from nats.aio.client import Client as NATS
from quantflow_publisher import publish_tick, publish_candle
from telegram_notifier import notify_telegram, ChatType
from dxy_builder import on_source_candle as on_dxy_source_candle


# --- lightweight market calendar (New York time for FX session)
try:
    from zoneinfo import ZoneInfo  # py3.9+
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

NY = ZoneInfo("America/New_York")

logger = logging.getLogger(__name__)

# --------------------
# Tunables (defaults; can be overridden via env if you want)
# --------------------
# Base grace used when computing dynamic grace; actual grace = clamp(period_s * GRACE_RATIO, GRACE_MIN, GRACE_MAX)
GRACE_RATIO = 0.20      # 20% of the timeframe length
GRACE_MIN   = 6         # seconds
GRACE_MAX   = 20        # seconds
# Confirm window scales too (how far back we "double-check" before synthesizing)
CONFIRM_RATIO = 0.60    # 60% of the timeframe length
CONFIRM_MIN   = 20      # seconds
CONFIRM_MAX   = 90      # seconds


def _grace_for(period_s: int) -> int:
    g = int(period_s * GRACE_RATIO)
    return max(GRACE_MIN, min(g, GRACE_MAX))


def _confirm_window_for(period_s: int) -> int:
    c = int(period_s * CONFIRM_RATIO)
    return max(CONFIRM_MIN, min(c, CONFIRM_MAX))


# =========================
#   ENV / HOSTS
# =========================
class OandaEnv:
    PRACTICE = "practice"
    LIVE = "live"


def _stream_host(env):
    return "https://stream-fxpractice.oanda.com" if env == OandaEnv.PRACTICE else "https://stream-fxtrade.oanda.com"


def _api_host(env):
    return "https://api-fxpractice.oanda.com" if env == OandaEnv.PRACTICE else "https://api-fxtrade.oanda.com"


# =========================
#   SMALL HELPERS
# =========================
async def _reconnect_backoff(delay):
    jitter = random.uniform(0, 0.5)
    await asyncio.sleep(delay + jitter)
    return min(delay * 2.0, 60.0)


def _parse_rfc3339(ts):
    # Example: "2025-01-01T12:34:56.123456789Z"
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def _display_split(symbol_str):
    parts = symbol_str.strip().upper().split("/")
    return (parts[0], parts[1]) if len(parts) == 2 else (symbol_str.strip().upper(), "")


def _tf_label_to_oanda(tf_label: str) -> str:
    s = tf_label.strip().lower()
    if s.endswith("m"):
        return f"M{int(s[:-1])}"
    if s.endswith("h"):
        return f"H{int(s[:-1])}"
    if s.endswith("d"):
        return "D"
    return "M1"


def _tf_label_to_seconds(tf_label: str) -> int:
    s = tf_label.strip().lower()
    if s.endswith("m"):
        return int(s[:-1]) * 60
    if s.endswith("h"):
        return int(s[:-1]) * 3600
    if s.endswith("d"):
        return 86400
    return 60


def _floor_to_period_utc(dt_utc, period_s):
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    seconds = int((dt_utc - epoch).total_seconds())
    floored = seconds - (seconds % period_s)
    return epoch + timedelta(seconds=floored)


# =========================
#   LIGHTWEIGHT FX CALENDAR
# =========================
def _is_fx_open(now_utc: dt.datetime) -> bool:
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=dt.timezone.utc)
    now_ny = now_utc.astimezone(NY)
    wd = now_ny.weekday()
    t = now_ny.time()
    if wd == 6:  # Sunday
        return t >= dt.time(17, 0)
    if wd in (0, 1, 2, 3):  # Mon-Thu
        return True
    if wd == 4:  # Friday
        return t < dt.time(17, 0)
    return False  # Saturday

# =========================
#   TICK STREAM (unchanged)
# =========================
async def get_oanda_tick_stream(
    instrument,
    display_symbol,
    account_id,
    token,
    nc: NATS,
    env=OandaEnv.LIVE,
    tick_queue: asyncio.Queue = None,
):
    base, quote = _display_split(display_symbol)
    headers = {"Authorization": f"Bearer {token}"}
    params = {"instruments": instrument}
    url = f"{_stream_host(env)}/v3/accounts/{account_id}/pricing/stream"

    delay = 1.0
    while True:
        try:
            timeout = aiohttp.ClientTimeout(sock_connect=20, sock_read=None)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=headers, params=params) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"OANDA stream HTTP {resp.status}: {await resp.text()}")
                    delay = 1.0
                    async for raw in resp.content:
                        if not raw:
                            continue
                        line = raw.decode().strip()
                        if not line:
                            continue
                        try:
                            msg = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if msg.get("type") != "PRICE":
                            continue
                        bids = msg.get("bids") or []
                        asks = msg.get("asks") or []
                        if not bids or not asks:
                            continue
                        bid = float(bids[0]["price"])
                        ask = float(asks[0]["price"])
                        mid = (bid + ask) / 2.0
                        t = _parse_rfc3339(msg["time"])
                        tick_data = {
                            "exchange": "OANDA",
                            "tick_time": t,
                            "symbol": display_symbol,
                            "base_currency": base,
                            "quote_currency": quote,
                            "bid": bid,
                            "ask": ask,
                            "last_price": mid,
                            "high": mid,
                            "low": mid,
                            "volume": 0.0,
                        }
                        await publish_tick(nc, tick=tick_data)
                        await asyncio.sleep(0)
        except Exception as e:
            logger.warning(f"OANDA tick stream error: {e}")
            delay = await _reconnect_backoff(delay)


# =========================
#   CANDLES VIA REST (race-free publishing)
# =========================
async def _fetch_last_completed(
    session: aiohttp.ClientSession,
    host: str,
    instrument: str,
    granularity: str,
    price_mode: str,
    token: str,
) -> tuple[Optional[datetime], Optional[tuple[float, float, float, float]], float]:
    """
    Fetch most recent completed candle for instrument/granularity/price_mode.
    Returns (open_time_utc, (o,h,l,c), volume)
    """
    url = f"{host}/v3/instruments/{instrument}/candles"
    params = {"granularity": granularity, "price": price_mode, "count": 2}
    headers = {"Authorization": f"Bearer {token}"}
    async with session.get(url, headers=headers, params=params) as resp:
        if resp.status != 200:
            raise RuntimeError(f"OANDA candles HTTP {resp.status}: {await resp.text()}")
        data = await resp.json()
        candles = [c for c in data.get("candles", []) if c.get("complete")]
        if not candles:
            return None, None, 0.0
        last = candles[-1]
        t_open = _parse_rfc3339(last["time"])
        pk = {"M": "mid", "B": "bid", "A": "ask"}[price_mode]
        ohlc_raw = last.get(pk)
        if not ohlc_raw:
            return None, None, 0.0
        ohlc = (float(ohlc_raw["o"]), float(ohlc_raw["h"]), float(ohlc_raw["l"]), float(ohlc_raw["c"]))
        vol = float(last.get("volume", 0.0))
        return t_open, ohlc, vol


async def get_oanda_candles_rest(
    display_symbol: str,
    instrument: str,
    timeframes,
    price_modes,
    token: str,
    nc: NATS,
    env=OandaEnv.LIVE,
    poll_interval_sec: int = 2,
):
    """
    Race-free policy:
      - Never publish a candle until it is fully closed + dynamic grace.
      - When considering a synthetic, *re-confirm* with REST first.
      - Exactly one publish per (tf, price_mode, open_time) to NATS (no later replacements).
    """
    base, quote = _display_split(display_symbol)
    host = _api_host(env)
    tf_labels = [str(t).lower() for t in timeframes]
    price_modes = [m.upper() for m in price_modes]

    last_open_map: Dict[Tuple[str, str], datetime] = {}
    last_close_px_map: Dict[Tuple[str, str], float] = {}

    delay = 1.0
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            if not _is_fx_open(now_utc):
                await asyncio.sleep(10)
                continue

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
                for tf in tf_labels:
                    og = _tf_label_to_oanda(tf)
                    period_s = _tf_label_to_seconds(tf)
                    GRACE = _grace_for(period_s)
                    CONFIRM_WINDOW = _confirm_window_for(period_s)

                    for pm in price_modes:
                        # Fetch newest completed (if any)
                        real_last_open, real_last_ohlc, real_last_vol = await _fetch_last_completed(
                            session, host, instrument, og, pm, token
                        )

                        key = (tf, pm)
                        last_open = last_open_map.get(key)
                        last_close_px = last_close_px_map.get(key)

                        # Seed with the newest real candle if we don't have any yet
                        if last_open is None:
                            if real_last_open is None or real_last_ohlc is None:
                                continue
                            o, h, l, c = real_last_ohlc
                            await publish_candle(nc, candle={
                                "exchange": "OANDA",
                                "symbol": display_symbol,
                                "base_currency": base,
                                "quote_currency": quote,
                                "timeframe": tf,
                                "open_time": real_last_open,
                                "open": o, "high": h, "low": l, "close": c,
                                "volume": real_last_vol,
                                "close_time": real_last_open + timedelta(seconds=period_s),
                                "price_mode": pm,
                            })
                            
                            # ----------------- DXY Builder ----------------------
                            candle={
                                "exchange": "OANDA",
                                "symbol": display_symbol,
                                "base_currency": base,
                                "quote_currency": quote,
                                "timeframe": tf,
                                "open_time": real_last_open,
                                "open": o, "high": h, "low": l, "close": c,
                                "volume": real_last_vol,
                                "close_time": real_last_open + timedelta(seconds=period_s),
                                "price_mode": pm,
                            }
                            await on_dxy_source_candle(nc, candle)
                            # ----------------- DXY Builder ----------------------

                            last_open_map[key] = real_last_open
                            last_close_px_map[key] = c
                            continue

                        # If we already have a newer real candle, publish it immediately (no grace for real bars)
                        if real_last_open and real_last_open > last_open and real_last_ohlc:
                            # Fill any fully-closed bars BEFORE that (respect grace)
                            expected = last_open + timedelta(seconds=period_s)
                            cutoff = now_utc - timedelta(seconds=GRACE)
                            while expected + timedelta(seconds=period_s) <= cutoff and expected < real_last_open:
                                px = last_close_px
                                open_time = expected
                                await publish_candle(nc, candle={
                                    "exchange": "OANDA",
                                    "symbol": display_symbol,
                                    "base_currency": base,
                                    "quote_currency": quote,
                                    "timeframe": tf,
                                    "open_time": open_time,
                                    "open": px, "high": px, "low": px, "close": px,
                                    "volume": 0.0,
                                    "close_time": open_time + timedelta(seconds=period_s),
                                    "price_mode": pm,
                                })

                                # ----------------- DXY Builder ----------------------
                                candle={
                                    "exchange": "OANDA",
                                    "symbol": display_symbol,
                                    "base_currency": base,
                                    "quote_currency": quote,
                                    "timeframe": tf,
                                    "open_time": open_time,
                                    "open": px, "high": px, "low": px, "close": px,
                                    "volume": 0.0,
                                    "close_time": open_time + timedelta(seconds=period_s),
                                    "price_mode": pm,
                                }
                                await on_dxy_source_candle(nc, candle)
                                # ----------------- DXY Builder ----------------------

                                last_open_map[key] = open_time
                                last_close_px_map[key] = px
                                expected += timedelta(seconds=period_s)

                            # Publish the real newest bar now
                            o, h, l, c = real_last_ohlc
                            await publish_candle(nc, candle={
                                "exchange": "OANDA",
                                "symbol": display_symbol,
                                "base_currency": base,
                                "quote_currency": quote,
                                "timeframe": tf,
                                "open_time": real_last_open,
                                "open": o, "high": h, "low": l, "close": c,
                                "volume": real_last_vol,
                                "close_time": real_last_open + timedelta(seconds=period_s),
                                "price_mode": pm,
                            })

                            # ----------------- DXY Builder ----------------------
                            candle={
                                "exchange": "OANDA",
                                "symbol": display_symbol,
                                "base_currency": base,
                                "quote_currency": quote,
                                "timeframe": tf,
                                "open_time": real_last_open,
                                "open": o, "high": h, "low": l, "close": c,
                                "volume": real_last_vol,
                                "close_time": real_last_open + timedelta(seconds=period_s),
                                "price_mode": pm,
                            }
                            await on_dxy_source_candle(nc, candle)
                            # ----------------- DXY Builder ----------------------

                            last_open_map[key] = real_last_open
                            last_close_px_map[key] = c
                            continue

                        # No newer real bar yet → consider fully-closed bars older than GRACE
                        expected = last_open + timedelta(seconds=period_s)
                        cutoff = now_utc - timedelta(seconds=GRACE)
                        while expected + timedelta(seconds=period_s) <= cutoff:
                            # If the bar just closed recently, double-check before synthesizing
                            recently_closed = (now_utc - (expected + timedelta(seconds=period_s))).total_seconds() <= CONFIRM_WINDOW
                            if recently_closed:
                                conf_open, conf_ohlc, conf_vol = await _fetch_last_completed(
                                    session, host, instrument, og, pm, token
                                )
                                if conf_open and conf_open >= expected and conf_ohlc:
                                    # Fill any older full gaps (if any) before conf_open
                                    fill_ptr = last_open + timedelta(seconds=period_s)
                                    while fill_ptr < conf_open and fill_ptr + timedelta(seconds=period_s) <= cutoff:
                                        px = last_close_px
                                        await publish_candle(nc, candle={
                                            "exchange": "OANDA",
                                            "symbol": display_symbol,
                                            "base_currency": base,
                                            "quote_currency": quote,
                                            "timeframe": tf,
                                            "open_time": fill_ptr,
                                            "open": px, "high": px, "low": px, "close": px,
                                            "volume": 0.0,
                                            "close_time": fill_ptr + timedelta(seconds=period_s),
                                            "price_mode": pm,
                                        })

                                        # ----------------- DXY Builder ----------------------
                                        candle={
                                            "exchange": "OANDA",
                                            "symbol": display_symbol,
                                            "base_currency": base,
                                            "quote_currency": quote,
                                            "timeframe": tf,
                                            "open_time": fill_ptr,
                                            "open": px, "high": px, "low": px, "close": px,
                                            "volume": 0.0,
                                            "close_time": fill_ptr + timedelta(seconds=period_s),
                                            "price_mode": pm,
                                        }
                                        await on_dxy_source_candle(nc, candle)
                                        # ----------------- DXY Builder ----------------------

                                        last_open_map[key] = fill_ptr
                                        last_close_px_map[key] = px
                                        fill_ptr += timedelta(seconds=period_s)

                                    # Publish the confirmed real bar
                                    o, h, l, c = conf_ohlc
                                    await publish_candle(nc, candle={
                                        "exchange": "OANDA",
                                        "symbol": display_symbol,
                                        "base_currency": base,
                                        "quote_currency": quote,
                                        "timeframe": tf,
                                        "open_time": conf_open,
                                        "open": o, "high": h, "low": l, "close": c,
                                        "volume": conf_vol,
                                        "close_time": conf_open + timedelta(seconds=period_s),
                                        "price_mode": pm,
                                    })

                                    # ----------------- DXY Builder ----------------------
                                    candle={
                                        "exchange": "OANDA",
                                        "symbol": display_symbol,
                                        "base_currency": base,
                                        "quote_currency": quote,
                                        "timeframe": tf,
                                        "open_time": conf_open,
                                        "open": o, "high": h, "low": l, "close": c,
                                        "volume": conf_vol,
                                        "close_time": conf_open + timedelta(seconds=period_s),
                                        "price_mode": pm,
                                    }
                                    await on_dxy_source_candle(nc, candle)
                                    # ----------------- DXY Builder ----------------------

                                    last_open_map[key] = conf_open
                                    last_close_px_map[key] = c
                                    expected = conf_open + timedelta(seconds=period_s)
                                    continue

                            # Still no real bar → publish a single synthetic (flat) for this minute
                            px = last_close_px
                            await publish_candle(nc, candle={
                                "exchange": "OANDA",
                                "symbol": display_symbol,
                                "base_currency": base,
                                "quote_currency": quote,
                                "timeframe": tf,
                                "open_time": expected,
                                "open": px, "high": px, "low": px, "close": px,
                                "volume": 0.0,
                                "close_time": expected + timedelta(seconds=period_s),
                                "price_mode": pm,
                            })

                            # ----------------- DXY Builder ----------------------
                            candle={
                                "exchange": "OANDA",
                                "symbol": display_symbol,
                                "base_currency": base,
                                "quote_currency": quote,
                                "timeframe": tf,
                                "open_time": expected,
                                "open": px, "high": px, "low": px, "close": px,
                                "volume": 0.0,
                                "close_time": expected + timedelta(seconds=period_s),
                                "price_mode": pm,
                            }
                            await on_dxy_source_candle(nc, candle)
                            # ----------------- DXY Builder ----------------------

                            last_open_map[key] = expected
                            last_close_px_map[key] = px
                            expected += timedelta(seconds=period_s)

            await asyncio.sleep(poll_interval_sec)
            delay = 1.0

        except (aiohttp.ClientConnectionError, aiohttp.ServerDisconnectedError, asyncio.TimeoutError, OSError) as e:
            notify_telegram("❌ DataCollectorApp-Candle (OANDA REST)\n" + str(e), ChatType.ALERT)
            logger.warning(f"OANDA REST candles error ({display_symbol}): {e}; retrying in ~{delay:.1f}s")
            delay = await _reconnect_backoff(delay)
        except Exception as e:
            notify_telegram("❌ DataCollectorApp-Candle (OANDA REST)\n" + str(e), ChatType.ALERT)
            logger.exception(f"Unexpected OANDA REST candle error ({display_symbol}): {e}")
            delay = await _reconnect_backoff(delay)
