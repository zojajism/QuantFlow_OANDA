# telegram_notifier.py (patches)

import os, asyncio, time, json, html
from enum import Enum
from typing import Dict, Any, Optional
import aiohttp

class ChatType(str, Enum):
    ALERT = "alert"
    INFO  = "info"

# remove module-level _TELEGRAM_TOKEN and CHAT_IDS lookups

def _get_token() -> Optional[str]:
    return os.getenv("TELEGRAM_BOT_TOKEN")

def _get_chat_id(chat_type: ChatType) -> Optional[str]:
    if chat_type is ChatType.ALERT:
        return os.getenv("TELEGRAM_CHAT_ID_ALERT")
    return os.getenv("TELEGRAM_CHAT_ID_INFO")

_QUEUE: asyncio.Queue[Dict[str, Any]] = asyncio.Queue(maxsize=1000)
_SESSION: Optional[aiohttp.ClientSession] = None
_MIN_INTERVAL = 0.25
_LAST_SENT: Dict[str, float] = {}

async def _ensure_session():
    global _SESSION
    if _SESSION is None:
        _SESSION = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5))

async def _send_telegram_text(chat_id: str, text: str):
    token = _get_token()
    if not token or not chat_id:
        raise RuntimeError("Telegram token or chat_id missing")

    await _ensure_session()
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    # OPTIONAL: use plain text first to avoid HTML entity errors
    # payload["parse_mode"] = "HTML"

    async with _SESSION.post(url, json=payload) as r:
        if r.status >= 400:
            body = await r.text()
            raise RuntimeError(f"Telegram HTTP {r.status}: {body[:200]}")

async def _worker():
    while True:
        item = await _QUEUE.get()
        chat_id, text = item["chat_id"], item["text"]

        last = _LAST_SENT.get(chat_id, 0.0)
        dt = time.time() - last
        if dt < _MIN_INTERVAL:
            await asyncio.sleep(_MIN_INTERVAL - dt)

        # retry a few times, but LOG on failure
        delay = 0.5
        for attempt in range(5):
            try:
                await _send_telegram_text(chat_id, text)
                _LAST_SENT[chat_id] = time.time()
                break
            except Exception as e:
                if attempt == 4:
                    print(f"[telegram_notifier] send failed: {e}")  # <-- visible error
                else:
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 5.0)

        _QUEUE.task_done()

async def start_telegram_notifier():
    asyncio.create_task(_worker(), name="telegram-notifier")

def notify_telegram(text: str, chat_type: ChatType, meta: Optional[Dict[str, Any]] = None):
    chat_id = _get_chat_id(chat_type)
    if not chat_id:
        print(f"[telegram_notifier] missing chat id for {chat_type.value}")
        return

    if meta:
        # safer: no HTML to avoid “can't parse entities” errors
        text = f"{text}\n{json.dumps(meta, ensure_ascii=False, indent=2)}"

    try:
        _QUEUE.put_nowait({"chat_id": chat_id, "text": text})
    except asyncio.QueueFull:
        print("[telegram_notifier] queue full; dropping message")

async def close_telegram_notifier():
    await _QUEUE.join()
    global _SESSION
    if _SESSION:
        await _SESSION.close()
        _SESSION = None
