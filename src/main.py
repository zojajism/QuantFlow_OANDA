import asyncio
import json
import os
from pathlib import Path

import yaml
from dotenv import load_dotenv
from logger_config import setup_logger
from nats.aio.client import Client as NATS
from NATS_setup import ensure_streams_from_yaml
from telegram_notifier import (
    notify_telegram,
    ChatType,
    start_telegram_notifier,
    close_telegram_notifier,
    ChatType,
)

# --- OANDA exchange adapter (ticks + candles via REST)
from exchange_oanda import (
    get_oanda_tick_stream,
    get_oanda_candles_rest,  # <-- new: fetch closed candles from OANDA REST
    OandaEnv,
)

CONFIG_PATH = Path("/data/config.yaml")
if not CONFIG_PATH.exists():
    CONFIG_PATH = Path(__file__).resolve().parent / "data" / "config.yaml"


def to_oanda_instrument(symbol_str: str) -> str:
    """Convert 'EUR/USD' -> 'EUR_USD' (OANDA format)."""
    return symbol_str.strip().upper().replace("/", "_")


async def main():
    try:
        # --- Load .env (Docker volume first, then local)
        env_path = Path("/data/.env")
        if not env_path.exists():
            env_path = Path(__file__).resolve().parent / "data" / ".env"
        load_dotenv(dotenv_path=env_path)

        logger = setup_logger()
        logger.info(
            json.dumps(
                {
                    "EventCode": 0,
                    "Message": "Starting QuantFlow_DataCollector (OANDA live)…",
                }
            )
        )

        await start_telegram_notifier()
        notify_telegram("❇️ Data Collector App started (OANDA)…", ChatType.ALERT)

        # --- Load config
        if not CONFIG_PATH.exists():
            raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

        with CONFIG_PATH.open("r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f) or {}

        # Your same config structure
        symbols_cfg = [str(s) for s in config_data.get("symbols", [])]
        timeframes = [str(t).lower() for t in config_data.get("timeframes", [])]  # e.g., ["1m","3m","1h","4h","1d"]

        # --- Env & credentials
        # Set OANDA_ENV=live or practice in .env
        env_flag = (os.getenv("OANDA_ENV") or "live").strip().lower()
        oanda_env = OandaEnv.LIVE if env_flag == "live" else OandaEnv.PRACTICE

        oanda_token = os.getenv("OANDA_API_TOKEN")
        oanda_account = os.getenv("OANDA_ACCOUNT_ID")

        if not oanda_token or not oanda_account:
            raise RuntimeError("Missing OANDA_API_TOKEN or OANDA_ACCOUNT_ID in .env")

        # --- NATS
        nats_url = os.getenv("NATS_URL")
        nats_user = os.getenv("NATS_USER")
        nats_pass = os.getenv("NATS_PASS")

        if not nats_url:
            raise RuntimeError("Missing NATS_URL in .env")

        nc = NATS()
        await nc.connect(servers=[nats_url], user=nats_user, password=nats_pass)

        # Ensure streams (same as your current flow)
        await ensure_streams_from_yaml(nc, "streams.yaml")

        # --- Build tasks
        ticker_tasks = []
        candle_tasks = []

        # We’ll use mid-price candles by default; you can add ["B","A"] later if you want
        price_modes = ["M"]

        for symbol in symbols_cfg:
            instrument = to_oanda_instrument(symbol)  # e.g., "EUR_USD"

            # Ticker stream (producer) — optional but you already run it
            ticker_tasks.append(
                get_oanda_tick_stream(
                    instrument=instrument,
                    display_symbol=symbol,
                    account_id=oanda_account,
                    token=oanda_token,
                    nc=nc,
                    env=oanda_env,
                    tick_queue=None,  # no need to feed candle builders now
                )
            )

            # Candle fetcher from OANDA REST for ALL requested timeframes of this symbol
            if timeframes:
                candle_tasks.append(
                    get_oanda_candles_rest(
                        display_symbol=symbol,
                        instrument=instrument,
                        timeframes=timeframes,   # e.g., ["1m","3m","5m","1h","4h","1d"]
                        price_modes=price_modes, # ["M"] now; can become ["M","B","A"]
                        token=oanda_token,
                        nc=nc,
                        env=oanda_env,
                        poll_interval_sec=2,     # small polling loop; adjust if needed
                    )
                )

        # --- Run
        await asyncio.gather(*candle_tasks, *ticker_tasks)
        

    finally:
        notify_telegram("⛔️ Data Collector App stopped.", ChatType.ALERT)
        await close_telegram_notifier()


if __name__ == "__main__":
    asyncio.run(main())
