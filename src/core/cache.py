"""
cache.py — Redis cache management for the Voice AI service.

══════════════════════════════════════════════════════════════════════════
  REDIS WRITE CONTRACT
══════════════════════════════════════════════════════════════════════════
  This module is the ONLY component in the Voice AI service that writes
  to Redis. It does so exactly ONCE — during startup (warm_up_cache()).

  After startup, Redis is STRICTLY READ-ONLY from the Voice AI's perspective.
  All subsequent cache updates are owned exclusively by the:

      Redis Cache Synchronisation Service
      (ingestion/cache_sync/main.py)

  That service consumes change events published by the scraper pipeline
  and updates only the affected `car:{id}` keys in real-time.

  ┌──────────────────────────────────────────────────────────────────────┐
  │  Scraper → ChromaDB → Redis Stream → Cache Sync Service → Redis      │
  │                                                                       │
  │  Voice AI reads Redis. Voice AI NEVER writes Redis post-startup.     │
  └──────────────────────────────────────────────────────────────────────┘
══════════════════════════════════════════════════════════════════════════

Redis key schema
────────────────
  Key:   car:{id}
  Value: JSON  { "id", "document", "metadata": { "name", "price", ...},
                 "available": true | null }

  available == null  →  car has been soft-deleted by the sync service
                         (no longer in inventory). Retriever filters these out.
  available == true  →  car is active in the inventory.
"""

import json
import logging
import asyncio
from typing import Any
import redis.asyncio as redis

from .config import settings

logger = logging.getLogger(__name__)

# ── Redis Connection Pool ──────────────────────────────────────────────────────
redis_pool = redis.from_url(settings.REDIS_URL, decode_responses=True)


async def warm_up_cache() -> None:
    """
    Pre-populate the Redis cache on Voice AI startup.

    Queries ChromaDB to fetch the most commonly requested vehicles and
    stores them under `car:{id}` keys so the retriever can serve them
    from Redis (fast-path) without hitting ChromaDB on every request.

    Warmed datasets:
        1. All vehicles priced at or below 250,000 SAR (budget segment).
        2. GV80 models (top-searched premium SUV).
        3. G90 models (flagship sedan).

    Deduplication is applied before writing to Redis — a vehicle that
    appears in multiple query results is only stored once.

    TTL: 86400 seconds (24 hours) — matching the Cache Sync Service TTL
    so that warm and sync-updated entries expire at the same cadence.

    NOTE: This is the ONLY write path in the Voice AI service.
    After this function returns, all Redis writes are delegated to the
    Redis Cache Synchronisation Service (ingestion/cache_sync/).
    """
    logger.info("[Cache] Starting Redis warm-up cache...")

    from agent.retriever import SimpleRetriever

    try:
        retriever = SimpleRetriever(top_k=50)

        # 1. Budget vehicles (≤ 250,000 SAR)
        logger.info("[Cache] Fetching budget vehicles (price ≤ 250,000 SAR)...")
        budget_cars = await retriever.retrieve(query="car", price_max=250000)
        logger.info("[Cache] Budget vehicles fetched: %d", len(budget_cars))

        # 2. GV80 premium SUV models
        logger.info("[Cache] Fetching GV80 models...")
        gv80_cars = await retriever.retrieve(query="GV80")
        logger.info("[Cache] GV80 models fetched: %d", len(gv80_cars))

        # 3. G90 flagship sedan models
        logger.info("[Cache] Fetching G90 models...")
        g90_cars = await retriever.retrieve(query="G90")
        logger.info("[Cache] G90 models fetched: %d", len(g90_cars))

        all_cars = budget_cars + gv80_cars + g90_cars

        if not all_cars:
            logger.warning(
                "[Cache] No vehicles returned during warm-up — "
                "ChromaDB collection may be empty. Skipping Redis population."
            )
            return

        # Deduplicate by ID (last occurrence wins — all are equivalent at startup)
        unique_cars: dict[str, Any] = {car["id"]: car for car in all_cars}
        logger.info(
            "[Cache] Deduplication complete: %d unique vehicle(s) to cache (from %d total).",
            len(unique_cars),
            len(all_cars),
        )

        # Write to Redis using a pipeline for minimal round-trips
        async with redis_pool.pipeline(transaction=True) as pipe:
            for car_id, car in unique_cars.items():
                cache_key = f"car:{car_id}"
                # Ensure the cached entry has the `available` field set to True
                # so that the retriever fast-path can filter soft-deleted cars.
                payload = dict(car)
                payload.setdefault("available", True)
                if "metadata" in payload:
                    payload["metadata"].setdefault("availability", "available")
                pipe.setex(cache_key, 86400, json.dumps(payload))

            await pipe.execute()

        logger.info(
            "[Cache] Warm-up complete — %d vehicle(s) loaded into Redis.",
            len(unique_cars),
        )

    except Exception as exc:
        logger.error(
            "[Cache] Warm-up failed with error: %s. "
            "Voice AI will fall back to ChromaDB for all queries until Redis is populated "
            "by the Cache Sync Service.",
            exc,
            exc_info=True,
        )


async def close_cache() -> None:
    """Close the async Redis connection pool on Voice AI shutdown."""
    try:
        await redis_pool.aclose()
        logger.info("[Cache] Redis connection pool closed.")
    except Exception as exc:
        logger.warning("[Cache] Error closing Redis connection pool: %s", exc)
