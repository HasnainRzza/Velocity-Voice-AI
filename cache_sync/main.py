"""
main.py — Cache Sync Service entry point.

This module is the top-level runnable for the Redis Cache Synchronisation
Service. It wires together configuration, Redis client, handler, and consumer
and then enters the consume loop indefinitely.

How to run (from inside ingestion/):
    python cache_sync/main.py
    python -m cache_sync          # if __main__.py is added

The service handles SIGTERM and SIGINT for graceful shutdown — the current
batch of messages is completed before the process exits.

Logging:
    Structured logs are emitted to stdout with ISO timestamps.
    Set LOG_LEVEL=DEBUG for verbose per-message traces.
"""

import asyncio
import logging
import os
import signal
import sys

import redis

# Allow cache_sync to resolve ingestion-level imports (src.*, config.*)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cache_sync.config import CacheSyncSettings
from cache_sync.handler import CacheSyncHandler
from cache_sync.event_consumer import EventConsumer

# ── Logging setup ──────────────────────────────────────────────────────────────
_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
from config.logger import setup_async_logging

setup_async_logging(log_file_name="cache_sync.log", level=getattr(logging, _LOG_LEVEL, logging.INFO))
logger = logging.getLogger(__name__)


async def run(settings: CacheSyncSettings) -> None:
    """
    Initialise all components and start the consume loop.

    This coroutine runs indefinitely until:
        - A SIGTERM/SIGINT signal triggers graceful shutdown.
        - An unrecoverable error occurs.
    """
    shutdown_event = asyncio.Event()

    # ── Signal handlers for graceful shutdown ──────────────────────────────
    def _request_shutdown(sig: int, _frame: object) -> None:
        sig_name = signal.Signals(sig).name
        logger.info(
            "[Main] Received signal %s — requesting graceful shutdown...", sig_name
        )
        shutdown_event.set()

    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)

    # ── Redis client ───────────────────────────────────────────────────────
    logger.info("[Main] Connecting to Redis at %s...", settings.REDIS_URL)
    redis_client = redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        socket_connect_timeout=10,
        socket_timeout=10,
        retry_on_timeout=True,
    )

    try:
        redis_client.ping()
        logger.info("[Main] Redis connection established.")
    except (redis.ConnectionError, redis.TimeoutError) as exc:
        logger.critical(
            "[Main] Cannot connect to Redis at startup: %s. "
            "Ensure Redis is running on %s before starting the Cache Sync Service.",
            exc,
            settings.REDIS_URL,
        )
        sys.exit(1)

    # ── Component wiring ───────────────────────────────────────────────────
    handler = CacheSyncHandler(redis_client, settings)
    consumer = EventConsumer(redis_client, handler, settings, shutdown_event)

    # ── Startup: create consumer group and recover PEL ─────────────────────
    logger.info("[Main] Running startup sequence...")
    try:
        await consumer.start_async()
    except Exception as exc:
        logger.critical(
            "[Main] Startup sequence failed: %s", exc, exc_info=True
        )
        redis_client.close()
        sys.exit(1)

    logger.info("[Main] Cache Sync Service is READY — listening for inventory events.")
    logger.info(
        "[Main] Stream: %s | Group: %s | Consumer: %s",
        settings.STREAM_KEY,
        settings.CONSUMER_GROUP,
        settings.CONSUMER_NAME,
    )

    # ── Main consume loop ──────────────────────────────────────────────────
    try:
        await consumer.consume_loop()
    except Exception as exc:
        logger.error(
            "[Main] Consume loop exited with error: %s", exc, exc_info=True
        )
    finally:
        logger.info("[Main] Closing Redis connection...")
        try:
            redis_client.close()
        except Exception:
            pass
        logger.info("[Main] Cache Sync Service stopped.")


def main() -> None:
    """Parse settings, log config, and run the async event loop."""
    settings = CacheSyncSettings()
    settings.log_config()

    try:
        asyncio.run(run(settings))
    except KeyboardInterrupt:
        logger.info("[Main] Interrupted — exiting.")
        sys.exit(130)
    except SystemExit:
        raise
    except Exception as exc:
        logger.critical("[Main] Fatal error: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
