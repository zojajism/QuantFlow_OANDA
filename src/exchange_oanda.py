import asyncio
import json
import logging
import random
from datetime import datetime, timezone, timedelta

import aiohttp
from nats.aio.client import Client as NATS
from quantflow_publisher import publish_tick, publish_candle
from telegram_notifier import notify_telegram, ChatType


logger = logging.getLogger(__name__)


# =========================
#   ENV / HOSTS
# =========================
class OandaEnv:
    PRACTICE = "practice"
    LIVE = "live"


def _stream_host(env):
    if env == OandaEnv.PRACTICE:
        return "https://stream-fxpractice.oanda.com"
    return "https://stream-fxtrade.oanda.com"


# =========================
#   SMALL HELPERS
# =========================
async def _reconnect_backoff(delay):
    # exponential backoff with a little jitter
    jitter = random.uniform(0, 0.5)
    await asyncio.sleep(delay + jitter)
    return min(delay * 2.0, 60.0)


def _parse_rfc3339(ts):
    # Example: "2025-01-01T12:34:56.123456789Z"
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def _display_split(symbol_str):
    # "EUR/USD" -> ("EUR","USD")
    parts = symbol_str.strip().upper().split("/")
    if len(parts) == 2:
        return parts[0], parts[1]
    # fallback: whole symbol as base, no quote
    return symbol_str.strip().upper(), ""


def _to_oanda_instrument(symbol_str):
    # "EUR/USD" -> "EUR_USD"
    return symbol_str.strip().upper().replace("/", "_")


def _floor_to_period_utc(dt_utc, period_s):
    # floor a UTC-aware datetime to the start of its period
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    seconds = int((dt_utc - epoch).total_seconds())
    floored = seconds - (seconds % period_s)
    return epoch + timedelta(seconds=floored)


def _pick_price(bid, ask, mode):
    # mode: "M" (mid), "B" (bid), "A" (ask)
    if mode == "B":
        return bid
    if mode == "A":
        return ask
    return (bid + ask) / 2.0


def _timeframe_to_seconds(tf_label):
    """
    Convert your config labels to seconds:
      '1m','3m','5m','15m','30m','1h','4h','1d'
    """
    s = tf_label.strip().lower()
    if s.endswith("m"):
        return int(s[:-1]) * 60
    if s.endswith("h"):
        return int(s[:-1]) * 3600
    if s.endswith("d"):
        return int(s[:-1]) * 86400
    # default to 60 seconds if unknown
    return 60


# =========================
#   TICK STREAM
# =========================
async def get_oanda_tick_stream(
    instrument,          # OANDA instrument: "EUR_USD" (pass from main, override if needed)
    display_symbol,      # Published as-is to NATS: e.g., "BTC/USDT" or "EUR/USD"
    account_id,
    token,
    nc: NATS,
    env=OandaEnv.LIVE,
    tick_queue: asyncio.Queue = None,
):
    """
    Connects to OANDA streaming pricing and emits ticks.

    - Publishes ticks with 'symbol' == display_symbol (unchanged from config)
    - Optionally pushes simplified tick records onto tick_queue for candle builders
    """
    base, quote = _display_split(display_symbol)
    print_symbol = (f"'{display_symbol}'" + "        ")[:11]

    headers = {"Authorization": f"Bearer {token}"}
    params = {"instruments": instrument}
    url = f"{_stream_host(env)}/v3/accounts/{account_id}/pricing/stream"

    delay = 1.0
    while True:
        try:
            timeout = aiohttp.ClientTimeout(sock_connect=20, sock_read=None)  # streaming: no read timeout
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=headers, params=params) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        raise RuntimeError(f"OANDA stream HTTP {resp.status}: {text}")

                    logger.info(json.dumps({
                        "EventCode": 0,
                        "Message": f"Connected to OANDA pricing stream for {display_symbol} (inst={instrument}, env={env})"
                    }))
                    delay = 1.0  # reset backoff

                    async for raw in resp.content:
                        if not raw:
                            continue
                        line = raw.decode("utf-8").strip()
                        if not line:
                            continue

                        # one JSON object per line
                        try:
                            msg = json.loads(line)
                        except json.JSONDecodeError:
                            continue

                        typ = msg.get("type")
                        if typ == "HEARTBEAT":
                            continue
                        if typ != "PRICE":
                            continue

                        bids = msg.get("bids") or []
                        asks = msg.get("asks") or []
                        if not bids or not asks:
                            continue

                        try:
                            bid = float(bids[0]["price"])
                            ask = float(asks[0]["price"])
                        except Exception:
                            continue

                        t = _parse_rfc3339(msg["time"])
                        
                        last_mid = (bid + ask) / 2.0

                        tick_data = {
                            "exchange": "OANDA",
                            "tick_time": t,
                            "symbol": display_symbol,          # <- keep your config symbol
                            "base_currency": base,
                            "quote_currency": quote,
                            "bid": bid,
                            "ask": ask,
                            "last_price": last_mid,   # FX has no "last trade" - use mid
                            "high": last_mid,
                            "low": last_mid,
                            "volume": 0.0,                    # OANDA does not provide traded volume in ticks
                        }

                        logger.info(json.dumps({
                            "EventType": "Tick",
                            "exchange": "OANDA",
                            "symbol": print_symbol,
                            "tick_time": tick_data["tick_time"].isoformat(),
                            "last_price": tick_data["last_price"]
                        }))

                        await publish_tick(nc, tick=tick_data)

                        # also push to candle builders if requested
                        if tick_queue is not None:
                            try:
                                tick_rec = {"t": t, "bid": bid, "ask": ask}
                                tick_queue.put_nowait(tick_rec)
                            except asyncio.QueueFull:
                                # drop if overloaded; do not block the stream
                                pass

                        await asyncio.sleep(0)

        except (aiohttp.ClientConnectionError, aiohttp.ServerDisconnectedError, asyncio.TimeoutError, OSError) as e:
            notify_telegram("❌ DataCollectorApp-Tick (OANDA)\n" + str(e), ChatType.ALERT)
            logger.warning(f"OANDA tick stream dropped ({display_symbol}): {e}; reconnecting in ~{delay:.1f}s")
            delay = await _reconnect_backoff(delay)
            continue
        except Exception as e:
            notify_telegram("❌ DataCollectorApp-Tick (OANDA)\n" + str(e), ChatType.ALERT)
            logger.exception(f"Unexpected OANDA tick error ({display_symbol}): {e}")
            delay = await _reconnect_backoff(delay)
            continue


# =========================
#   CANDLE BUILDER (from ticks)
# =========================
class _SimpleCandleState:
    def __init__(self):
        self.has_tick = False
        self.open = 0.0
        self.high = 0.0
        self.low = 0.0
        self.close = 0.0
        self.volume = 0  # number of ticks


async def get_oanda_candles_from_ticks(
    display_symbol,       # Published as-is to NATS
    timeframes,           # e.g., ["1m","3m","5m","1h","1d"]  (your config)
    price_modes,          # e.g., ["M"] or ["M","B","A"]
    nc: NATS = None,
    tick_queue: asyncio.Queue = None,
):
    """
    Builds closed candles from incoming ticks (produced by get_oanda_tick_stream).

    - Consumes from tick_queue (non-blocking for the producer)
    - For each timeframe label and price mode, maintains a single rolling candle
    - On boundary switch, finalizes previous candle and publishes it
    - Candle "volume" = number of ticks in that interval (OANDA semantics)
    - Publishes with the *same timeframe labels* you configured (e.g., "1m")
    """
    if tick_queue is None:
        raise ValueError("tick_queue is required")

    # normalize labels and pre-compute periods
    tf_labels = [str(t).strip() for t in timeframes]
    tf_periods = {t: _timeframe_to_seconds(t) for t in tf_labels}
    price_modes = [m.upper() for m in price_modes]

    base, quote = _display_split(display_symbol)
    print_symbol = (f"'{display_symbol}'" + "        ")[:11]

    # one candle state per (timeframe_label, price_mode)
    states = {}
    starts = {}

    for tf in tf_labels:
        for m in price_modes:
            key = (tf, m)
            states[key] = _SimpleCandleState()
            starts[key] = None  # open_time (UTC) for current rolling candle

    while True:
        try:
            tick = await asyncio.wait_for(tick_queue.get(), timeout=5.0)
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            notify_telegram("❌ DataCollectorApp-Candle (from ticks)\n" + str(e), ChatType.ALERT)
            logger.exception(f"Error reading tick_queue: {e}")
            continue

        try:
            t = tick["t"]          # datetime UTC
            bid = float(tick["bid"])
            ask = float(tick["ask"])
        except Exception:
            continue

        for tf in tf_labels:
            period_s = tf_periods[tf]
            start = _floor_to_period_utc(t, period_s)

            for m in price_modes:
                key = (tf, m)
                st = states[key]
                cur_start = starts[key]

                price = _pick_price(bid, ask, m)

                # first tick for this candle
                if cur_start is None:
                    starts[key] = start
                    st.has_tick = True
                    st.open = price
                    st.high = price
                    st.low = price
                    st.close = price
                    st.volume = 1
                    continue

                # still inside same candle
                if start == cur_start:
                    if not st.has_tick:
                        st.open = price
                        st.high = price
                        st.low = price
                        st.close = price
                        st.volume = 1
                        st.has_tick = True
                    else:
                        if price > st.high:
                            st.high = price
                        if price < st.low:
                            st.low = price
                        st.close = price
                        st.volume += 1
                    continue

                # boundary crossed -> finalize previous candle
                if st.has_tick:
                    close_time = cur_start + timedelta(seconds=period_s)
                    candle_data = {
                        "exchange": "OANDA",
                        "symbol": display_symbol,      # <- keep your config symbol
                        "base_currency": base,
                        "quote_currency": quote,
                        "timeframe": tf,               # <- keep your config timeframe label (e.g., "1m")
                        "open_time": cur_start,        # UTC start of candle
                        "open": float(st.open),
                        "high": float(st.high),
                        "low": float(st.low),
                        "close": float(st.close),
                        "volume": float(st.volume),    # number of ticks
                        "close_time": close_time,
                        "price_mode": m,               # "M"/"B"/"A" (optional field)
                    }

                    logger.info(json.dumps({
                        "EventType": "Candle",
                        "exchange": "OANDA",
                        "symbol": print_symbol,
                        "timeframe": tf,
                        "close_time": close_time.isoformat(),
                        "close": candle_data["close"]
                    }))

                    if nc is not None:
                        await publish_candle(nc, candle=candle_data)

                # start new candle with current tick
                starts[key] = start
                st.has_tick = True
                st.open = price
                st.high = price
                st.low = price
                st.close = price
                st.volume = 1

        await asyncio.sleep(0)


# =========================
#   CONVENIENCE
# =========================
def make_tick_queue(maxsize=5000):
    return asyncio.Queue(maxsize=maxsize)


async def run_oanda_ticks_and_candles(
    display_symbol,       # e.g., "EUR/USD" or "BTC/USDT" (published as-is)
    account_id,
    token,
    nc: NATS,
    env=OandaEnv.LIVE,
    instrument=None,      # if None, will be derived from display_symbol
    timeframes=None,      # your config list
    price_modes=None,     # e.g., ["M"] or ["M","B","A"]
):
    """
    Simple helper to start both tasks together.
    """
    if timeframes is None:
        timeframes = ["1m"]
    if price_modes is None:
        price_modes = ["M"]
    if instrument is None:
        instrument = _to_oanda_instrument(display_symbol)

    q = make_tick_queue()

    producer = get_oanda_tick_stream(
        instrument=instrument,
        display_symbol=display_symbol,
        account_id=account_id,
        token=token,
        nc=nc,
        env=env,
        tick_queue=q,
    )
    consumer = get_oanda_candles_from_ticks(
        display_symbol=display_symbol,
        timeframes=timeframes,
        price_modes=price_modes,
        nc=nc,
        tick_queue=q,
    )

    await asyncio.gather(producer, consumer)
