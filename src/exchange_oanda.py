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


def _api_host(env):
    if env == OandaEnv.PRACTICE:
        return "https://api-fxpractice.oanda.com"
    return "https://api-fxtrade.oanda.com"


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
    return symbol_str.strip().upper(), ""


def _to_oanda_instrument(symbol_str):
    # "EUR/USD" -> "EUR_USD"
    return symbol_str.strip().upper().replace("/", "_")


def _tf_label_to_oanda(tf_label: str) -> str:
    """
    Map your labels to OANDA granularity.
      '1m','3m','5m','15m','30m','1h','4h','1d'
    """
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
#   TICK STREAM (unchanged)
# =========================
async def get_oanda_tick_stream(
    instrument,          # "EUR_USD"
    display_symbol,      # Published as-is to NATS
    account_id,
    token,
    nc: NATS,
    env=OandaEnv.LIVE,
    tick_queue: asyncio.Queue = None,
):
    """
    Connects to OANDA streaming pricing and emits ticks.

    - Publishes ticks with 'symbol' == display_symbol (unchanged from config)
    - Optionally pushes simplified tick records onto tick_queue (unused now)
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
                            "symbol": display_symbol,
                            "base_currency": base,
                            "quote_currency": quote,
                            "bid": bid,
                            "ask": ask,
                            "last_price": last_mid,
                            "high": last_mid,   # numeric to satisfy publish_tick
                            "low": last_mid,
                            "volume": 0.0,      # OANDA ticks have no traded volume
                        }

                        logger.info(json.dumps({
                            "EventType": "Tick",
                            "exchange": "OANDA",
                            "symbol": print_symbol,
                            "tick_time": tick_data["tick_time"].isoformat(),
                            "last_price": tick_data["last_price"]
                        }))

                        await publish_tick(nc, tick=tick_data)

                        # optional queue (not used now)
                        if tick_queue is not None:
                            try:
                                tick_queue.put_nowait({"t": t, "bid": bid, "ask": ask})
                            except asyncio.QueueFull:
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
#   CANDLES VIA OANDA REST
# =========================
async def get_oanda_candles_rest(
    display_symbol: str,
    instrument: str,            # "EUR_USD"
    timeframes,                 # e.g., ["1m","3m","5m","1h","4h","1d"]
    price_modes,                # ["M"] or ["M","B","A"]
    token: str,
    nc: NATS,
    env=OandaEnv.LIVE,
    poll_interval_sec: int = 2,
):
    """
    Poll OANDA REST for CLOSED candles and publish them.
    - Keeps your display_symbol and timeframe labels unchanged
    - Publishes a candle only once per (timeframe, price_mode) open_time
    """
    base, quote = _display_split(display_symbol)
    print_symbol = (f"'{display_symbol}'" + "        ")[:11]
    host = _api_host(env)

    tf_labels = [str(t).lower() for t in timeframes]
    price_modes = [m.upper() for m in price_modes]

    # Track last published candle open_time per (tf_label, price_mode)
    last_open_map = {}  # key: (tf_label, price_mode) -> datetime

    delay = 1.0
    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=20)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                for tf in tf_labels:
                    og = _tf_label_to_oanda(tf)
                    period_s = _tf_label_to_seconds(tf)

                    for pm in price_modes:
                        url = f"{host}/v3/instruments/{instrument}/candles"
                        params = {
                            "granularity": og,   # e.g., "M1","M5","H1","D"
                            "price": pm,         # "M" | "B" | "A"
                            "count": 2,          # get last 1-2 to be safe
                        }
                        headers = {"Authorization": f"Bearer {token}"}

                        async with session.get(url, headers=headers, params=params) as resp:
                            if resp.status != 200:
                                text = await resp.text()
                                raise RuntimeError(f"OANDA candles HTTP {resp.status}: {text}")

                            data = await resp.json()
                            candles = [c for c in data.get("candles", []) if c.get("complete")]
                            if not candles:
                                continue

                            last = candles[-1]
                            t_open = _parse_rfc3339(last["time"])
                            key = (tf, pm)

                            # Publish only once per open_time
                            if last_open_map.get(key) == t_open:
                                continue

                            # choose OHLC set based on price mode
                            price_key = {"M": "mid", "B": "bid", "A": "ask"}[pm]
                            ohlc = last.get(price_key)
                            if not ohlc:
                                continue

                            open_p = float(ohlc["o"])
                            high_p = float(ohlc["h"])
                            low_p  = float(ohlc["l"])
                            close_p= float(ohlc["c"])
                            vol    = float(last.get("volume", 0.0))
                            close_time = t_open + timedelta(seconds=period_s)

                            candle_data = {
                                "exchange": "OANDA",
                                "symbol": display_symbol,   # keep your label
                                "base_currency": base,
                                "quote_currency": quote,
                                "timeframe": tf,            # keep your label, e.g., "1m"
                                "open_time": t_open,
                                "open": open_p,
                                "high": high_p,
                                "low": low_p,
                                "close": close_p,
                                "volume": vol,              # OANDA volume = number of price updates
                                "close_time": close_time,
                                "price_mode": pm,
                            }

                            logger.info(json.dumps({
                                "EventType": "Candle",
                                "exchange": "OANDA",
                                "symbol": print_symbol,
                                "timeframe": tf,
                                "close_time": close_time.isoformat(),
                                "close": candle_data["close"]
                            }))

                            await publish_candle(nc, candle=candle_data)
                            last_open_map[key] = t_open

                # small sleep between full cycles
                await asyncio.sleep(poll_interval_sec)
                delay = 1.0  # reset backoff on successful cycle

        except (aiohttp.ClientConnectionError, aiohttp.ServerDisconnectedError, asyncio.TimeoutError, OSError) as e:
            notify_telegram("❌ DataCollectorApp-Candle (OANDA REST)\n" + str(e), ChatType.ALERT)
            logger.warning(f"OANDA REST candles error ({display_symbol}): {e}; retrying in ~{delay:.1f}s")
            delay = await _reconnect_backoff(delay)
            continue
        except Exception as e:
            notify_telegram("❌ DataCollectorApp-Candle (OANDA REST)\n" + str(e), ChatType.ALERT)
            logger.exception(f"Unexpected OANDA REST candle error ({display_symbol}): {e}")
            delay = await _reconnect_backoff(delay)
            continue
