"""
Reusable NATS JetStream setup helper for QuantFlow.
- Loads configuration from streams.yaml
- Handles enum casing differences
- Compatible with multiple nats-py versions
- Falls back to minimal stream creation if full StreamConfig fails
"""

from __future__ import annotations
import json
import logging
import os
import re
from inspect import signature
from typing import Any, List

import yaml
from nats.aio.client import Client as NATS
from nats.js.api import StreamConfig, RetentionPolicy, StorageType, DiscardPolicy
from nats.js.errors import BadRequestError

logger = logging.getLogger(__name__)

# ✅ Load .env early
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    logger.warning("[WARN] python-dotenv not installed. NATS URL will fall back to default.")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _enum_get(enum_cls, name: str, fallback):
    """Return enum member by case-insensitive name, else fallback."""
    try:
        return getattr(enum_cls, str(name).upper())
    except AttributeError:
        return fallback


def _get_js(nc: NATS):
    """Return a JetStream context across nats-py versions."""
    try:
        return nc.js
    except AttributeError:
        return nc.jetstream()


def _build_stream_config_kwargs(
    *,
    name,
    subjects,
    storage,
    retention,
    max_msgs,
    max_bytes,
    max_age,
    discard,
    dupe_window,
    replicas,
):
    """
    Build kwargs compatible with the installed nats-py StreamConfig signature.
    Handles replicas + duplicates/duplicate_window differences across versions.
    """
    params = signature(StreamConfig).parameters.keys()
    kwargs = dict(
        name=name,
        subjects=subjects,
        storage=storage,
        retention=retention,
        max_msgs=max_msgs,
        max_bytes=max_bytes,
        max_age=max_age,
        discard=discard,
    )
    if "replicas" in params:
        kwargs["replicas"] = replicas
    if "duplicates" in params:
        kwargs["duplicates"] = dupe_window
    elif "duplicate_window" in params:
        kwargs["duplicate_window"] = dupe_window
    return kwargs


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

_SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([KMGTP]?B)?\s*$", re.I)
_SIZE_MULT = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3,
              "TB": 1024**4, "PB": 1024**5}

def parse_size(value: Any) -> int:
    """Parse sizes like 2GB, 512MB, 100KB -> bytes. 0/None -> 0."""
    if value in (None, 0, "0"):
        return 0
    if isinstance(value, int):
        return value
    m = _SIZE_RE.match(str(value))
    if not m:
        raise ValueError(f"Invalid size: {value}")
    num = float(m.group(1))
    unit = (m.group(2) or "B").upper()
    return int(num * _SIZE_MULT[unit])


_DUR_RE = re.compile(r"(?i)(\d+(?:\.\d+)?)(ms|s|m|h|d)")
_MULT_NS = {"ms": 1_000_000, "s": 1_000_000_000,
            "m": 60 * 1_000_000_000, "h": 3600 * 1_000_000_000,
            "d": 24 * 3600 * 1_000_000_000}

def parse_duration_ns(expr: Any) -> int:
    """Parse '2m', '24h', '168h', '500ms' into nanoseconds. 0/None -> 0."""
    if expr in (None, 0, "0"):
        return 0
    s = str(expr).strip()
    total = 0
    for num, unit in _DUR_RE.findall(s):
        total += int(float(num) * _MULT_NS[unit.lower()])
    if total == 0:
        raise ValueError(f"Invalid duration: {expr}")
    return total


# ---------------------------------------------------------------------------
# Core Functions
# ---------------------------------------------------------------------------

async def ensure_stream_exists(
    nc: NATS,
    stream_name: str,
    subjects: List[str],
    *,
    storage: str = "file",
    retention: str = "limits",
    max_msgs: int = 0,
    max_bytes: int | str = 0,
    max_age: int | str = 0,
    discard: str = "old",
    dupe_window: int | str = "2m",
    replicas: int = 1,
) -> None:
    js = _get_js(nc)
    try:
        await js.stream_info(stream_name)
        logger.info(
                json.dumps({
                        "EventCode": 0,
                        "Message": f"[NATS] Stream '{stream_name}' already exists."
                    })
            )
        return
    except Exception:
        pass

    cfg_kwargs = _build_stream_config_kwargs(
        name=stream_name,
        subjects=subjects,
        storage=_enum_get(StorageType, storage, StorageType.FILE),
        retention=_enum_get(RetentionPolicy, retention, RetentionPolicy.LIMITS),
        max_msgs=max_msgs,
        max_bytes=parse_size(max_bytes),
        max_age=parse_duration_ns(max_age),
        discard=_enum_get(DiscardPolicy, discard, DiscardPolicy.OLD),
        dupe_window=parse_duration_ns(dupe_window),
        replicas=replicas,
    )

    logger.debug(f"[NATS] StreamConfig kwargs for '{stream_name}': {cfg_kwargs}")

    try:
        cfg = StreamConfig(**cfg_kwargs)
        await js.add_stream(cfg)
        logger.info(f"[NATS] Stream '{stream_name}' created (full config).")
        return
    except (BadRequestError, TypeError) as e:
        logger.warning(
            f"[NATS] Failed to create stream '{stream_name}' with full config: {e}. "
            f"Retrying with minimal config."
        )

    # Fallback: minimal add_stream (works on older servers/clients)
    await js.add_stream(name=stream_name, subjects=subjects)
    logger.info(f"[NATS] Stream '{stream_name}' created (minimal config).")


async def ensure_streams_from_yaml(nc: NATS, path: str) -> None:
    if not os.path.exists(path):
        raise FileNotFoundError(f"streams.yaml not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    streams = cfg.get("streams", [])
    if not isinstance(streams, list) or not streams:
        raise ValueError("No 'streams' list found in YAML.")
    for s in streams:
        await ensure_stream_exists(
            nc,
            stream_name=s["name"],
            subjects=s["subjects"],
            storage=s.get("storage", "file"),
            retention=s.get("retention", "limits"),
            max_msgs=int(s.get("max_msgs", 0)),
            max_bytes=s.get("max_bytes", 0),
            max_age=s.get("max_age", 0),
            discard=s.get("discard", "old"),
            dupe_window=s.get("dupe_window", "2m"),
            replicas=int(s.get("replicas", 1)),
        )


def get_nats_url() -> str:
    """Resolve NATS URL from ENV or fallback."""
    return os.getenv("NATS_URL", "nats://127.0.0.1:4222")
