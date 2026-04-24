"""Redis cache layer with configurable TTL.

Environment variables
---------------------
REDIS_URL          – full redis:// connection string (required for caching)
CACHE_TTL_HOURS    – default cache lifetime in hours (default: 48 = 2 days)

If REDIS_URL is not set, every operation silently no-ops so the app still
works without Redis.
"""

import os
import json
import asyncio
from typing import Optional

import redis.asyncio as aioredis

REDIS_URL: str = os.getenv("REDIS_URL", "")
CACHE_TTL_HOURS: int = int(os.getenv("CACHE_TTL_HOURS", "48"))
CACHE_TTL_SECONDS: int = CACHE_TTL_HOURS * 3600

_redis: Optional[aioredis.Redis] = None


async def connect_cache() -> None:
    """Initialise the async Redis client (call once at startup)."""
    global _redis
    if not REDIS_URL:
        print("[CACHE] REDIS_URL not set – caching disabled.")
        return
    try:
        _redis = aioredis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        await _redis.ping()
        print(f"[CACHE] Connected to Redis. TTL = {CACHE_TTL_HOURS}h ({CACHE_TTL_SECONDS}s)")
    except Exception as e:
        print(f"[CACHE] Failed to connect to Redis: {e} – caching disabled.")
        _redis = None


async def close_cache() -> None:
    """Gracefully close the Redis connection."""
    global _redis
    if _redis:
        await _redis.aclose()
        _redis = None


async def cache_get(key: str) -> Optional[dict | list]:
    """Return cached value (parsed JSON) or None on miss / error."""
    if not _redis:
        return None
    try:
        raw = await _redis.get(key)
        if raw is not None:
            return json.loads(raw)
    except Exception as e:
        print(f"[CACHE] GET error for '{key}': {e}")
    return None


async def cache_set(key: str, value, ttl: Optional[int] = None) -> None:
    """Store a JSON-serialisable value with an optional custom TTL (seconds)."""
    if not _redis:
        return
    try:
        await _redis.set(
            key,
            json.dumps(value, default=str),
            ex=ttl or CACHE_TTL_SECONDS,
        )
    except Exception as e:
        print(f"[CACHE] SET error for '{key}': {e}")


async def cache_delete(key: str) -> None:
    """Delete a single cache key."""
    if not _redis:
        return
    try:
        await _redis.delete(key)
    except Exception as e:
        print(f"[CACHE] DEL error for '{key}': {e}")


async def cache_delete_pattern(pattern: str) -> None:
    """Delete all keys matching a glob pattern (e.g. 'carrier:*')."""
    if not _redis:
        return
    try:
        cursor = None
        while cursor != 0:
            cursor, keys = await _redis.scan(cursor=cursor or 0, match=pattern, count=200)
            if keys:
                await _redis.delete(*keys)
    except Exception as e:
        print(f"[CACHE] DEL-PATTERN error for '{pattern}': {e}")

