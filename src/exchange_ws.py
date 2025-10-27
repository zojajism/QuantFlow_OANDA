import json
import asyncio
import random
import websockets
from datetime import datetime, timezone
import logging
from nats.aio.client import Client as NATS
from quantflow_publisher import publish_candle, publish_tick
from telegram_notifier import notify_telegram, ChatType


logger = logging.getLogger(__name__)

# ---- minimal backoff helper -------------------------------------------------
async def _reconnect_backoff(delay: float) -> float:
    jitter = random.uniform(0, 0.5)
    await asyncio.sleep(delay + jitter)
    return min(delay * 2, 60.0)  # cap at 60s


# ---- TICKER -----------------------------------------------------------------
async def get_binance_ticker_ws(symbol: str, base_currency: str, quote_currency: str, nc: NATS):
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@ticker"
    c_symbol = f"{base_currency.upper()}/{quote_currency.upper()}"
    printSymbol = (f"'{c_symbol}'" + "        ")[:11]

    delay = 1.0
    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=30,   # keepalive every 30s
                ping_timeout=25,    # wait up to 25s for pong
                close_timeout=5,
                open_timeout=20,
                max_queue=2000,     # avoid backpressure closes
                max_size=None       # don't drop larger frames
            ) as ws:

                logger.info(json.dumps({
                    "EventCode": 0,
                    "Message": f"Connected to Binance ticker stream for {c_symbol}"
                }))

                # reset backoff after a successful connect
                delay = 1.0

                async for message in ws:
                    msg = json.loads(message)

                    ticker_data = {
                        "exchange": "Binance",
                        "tick_time": datetime.fromtimestamp(int(msg['E']) / 1000, tz=timezone.utc),
                        "symbol": c_symbol,
                        "base_currency": base_currency.upper(),
                        "quote_currency": quote_currency.upper(),
                        "bid": float(msg['b']),
                        "ask": float(msg['a']),
                        "last_price": float(msg['c']),
                        "high": float(msg['h']),
                        "low": float(msg['l']),
                        "volume": float(msg['v']),
                    }

                    logger.info(json.dumps({
                        "EventType": "Tick",
                        "exchange": "Binance",
                        "symbol": printSymbol,
                        "tick_time": ticker_data["tick_time"].isoformat(),
                        "last_price": ticker_data["last_price"]
                    }))

                    # Do not block the recv loop
                    await publish_tick(nc, tick=ticker_data)
                    await asyncio.sleep(0)  # yield to event loop
        except (websockets.ConnectionClosedError,
                websockets.ConnectionClosedOK,
                asyncio.TimeoutError,
                OSError) as e:
            notify_telegram("❌ DataCollectorApp-Tick \n" + str(e), ChatType.ALERT)
            logger.warning(f"Ticker WS dropped ({c_symbol}): {e}; reconnecting in ~{delay:.1f}s")
            delay = await _reconnect_backoff(delay)
            continue
        except Exception as e:
            notify_telegram("❌ DataCollectorApp-Tick \n" + str(e), ChatType.ALERT)
            logger.exception(f"Unexpected ticker error ({c_symbol}): {e}")
            delay = await _reconnect_backoff(delay)
     
            continue


# ---- CANDLE -----------------------------------------------------------------
async def get_binance_candle_ws(symbol: str, base_currency: str, quote_currency: str, timeframe: str, nc: NATS):
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@kline_{timeframe}"
    c_symbol = f"{base_currency.upper()}/{quote_currency.upper()}"
    printSymbol = (f"'{c_symbol}'" + "        ")[:11]

    delay = 1.0
    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=30,
                ping_timeout=25,
                close_timeout=5,
                open_timeout=20,
                max_queue=2000,
                max_size=None
            ) as ws:

                logger.info(json.dumps({
                    "EventCode": 0,
                    "Message": f"Connected to Binance candle stream for Symbol: {c_symbol}, Timeframe: {timeframe}"
                }))

                delay = 1.0

                async for message in ws:
                    msg = json.loads(message)
                    k = msg['k']

                    # Only closed candles
                    if k['x']:
                        candle_data = {
                            "exchange": "Binance",
                            "symbol": c_symbol,
                            "base_currency": base_currency.upper(),
                            "quote_currency": quote_currency.upper(),
                            "timeframe": k['i'],
                            "open_time": datetime.fromtimestamp(int(k['t']) / 1000, tz=timezone.utc),
                            "open": float(k['o']),
                            "high": float(k['h']),
                            "low": float(k['l']),
                            "close": float(k['c']),
                            "volume": float(k['v']),
                            "close_time": datetime.fromtimestamp(int(k['T']) / 1000, tz=timezone.utc),
                        }

                        logger.info(json.dumps({
                            "EventType": "Candle",
                            "exchange": "Binance",
                            "symbol": printSymbol,
                            "timeframe": timeframe,
                            "close_time": candle_data["close_time"].isoformat(),
                            "close": candle_data["close"]
                        }))

                        await publish_candle(nc, candle=candle_data)
                        await asyncio.sleep(0)
        except (websockets.ConnectionClosedError,
                websockets.ConnectionClosedOK,
                asyncio.TimeoutError,
                OSError) as e:
            notify_telegram("❌ DataCollectorApp-Candle \n" + str(e), ChatType.ALERT)
            logger.warning(f"Candle WS dropped ({c_symbol} {timeframe}): {e}; reconnecting in ~{delay:.1f}s")
            delay = await _reconnect_backoff(delay)
            continue
        except Exception as e:
            notify_telegram("❌ DataCollectorApp-Candle \n" + str(e), ChatType.ALERT)
            logger.exception(f"Unexpected candle error ({c_symbol} {timeframe}): {e}")
            delay = await _reconnect_backoff(delay)
            continue
