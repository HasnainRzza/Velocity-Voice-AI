"""
config.py — CacheSyncSettings.

All configuration for the Cache Sync Service is sourced from environment
variables with safe defaults, making the service portable across local,
staging, and production environments without code changes.

Environment variables:
    REDIS_URL                  Redis connection URL (default: redis://localhost:6379/0)
    EVENTS_STREAM_KEY          Stream to consume (default: genesis:inventory:events)
    CACHE_SYNC_CONSUMER_GROUP  Consumer group name (default: cache-sync)
    CACHE_SYNC_CONSUMER_NAME   Unique consumer name (default: <hostname>-cache-sync)
    CACHE_SYNC_BATCH_SIZE      Messages per XREADGROUP call (default: 10)
    CACHE_SYNC_BLOCK_MS        XREADGROUP block timeout ms (default: 5000)
    CACHE_SYNC_MAX_RETRIES     Per-message handler retries (default: 3)
    CACHE_SYNC_RETRY_DELAY_SEC Seconds between retries (default: 1.0)
    CACHE_SYNC_DLQ_KEY         Dead-letter stream key (default: genesis:inventory:events:dlq)
    CACHE_SYNC_CAR_TTL_SEC     TTL for car:{id} keys in seconds (default: 86400 = 24h)
    CACHE_SYNC_AUTOCLAIM_MS    Claim messages idle longer than this (default: 1800000 = 30min)
    CACHE_SYNC_AUTOCLAIM_COUNT Max messages claimed per XAUTOCLAIM call (default: 50)
"""

import os
import socket
import logging
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load .env from the ingestion project root (parent of cache_sync/)
_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_ENV_PATH)


class CacheSyncSettings:
    """
    Immutable configuration snapshot for the Cache Sync Service.

    All values are resolved once at construction time from environment
    variables. Using a class (instead of module-level constants) allows
    easy dependency injection and unit-testing with overridden settings.
    """

    def __init__(self) -> None:
        # ── Redis connection ───────────────────────────────────────────────
        self.REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

        # ── Stream / consumer group ────────────────────────────────────────
        self.STREAM_KEY: str = os.getenv(
            "EVENTS_STREAM_KEY", "genesis:inventory:events"
        )
        self.CONSUMER_GROUP: str = os.getenv(
            "CACHE_SYNC_CONSUMER_GROUP", "cache-sync"
        )
        # Default consumer name is hostname-based — safe for multi-instance deployments
        default_consumer = f"{socket.gethostname()}-cache-sync"
        self.CONSUMER_NAME: str = os.getenv(
            "CACHE_SYNC_CONSUMER_NAME", default_consumer
        )

        # ── Consumption parameters ─────────────────────────────────────────
        self.BATCH_SIZE: int = int(os.getenv("CACHE_SYNC_BATCH_SIZE", "10"))
        self.BLOCK_MS: int = int(os.getenv("CACHE_SYNC_BLOCK_MS", "5000"))

        # ── Retry / dead-letter ────────────────────────────────────────────
        self.MAX_RETRIES: int = int(os.getenv("CACHE_SYNC_MAX_RETRIES", "3"))
        self.RETRY_DELAY_SEC: float = float(
            os.getenv("CACHE_SYNC_RETRY_DELAY_SEC", "1.0")
        )
        self.DLQ_KEY: str = os.getenv(
            "CACHE_SYNC_DLQ_KEY", "genesis:inventory:events:dlq"
        )

        # ── Redis cache TTL ────────────────────────────────────────────────
        self.CAR_TTL_SEC: int = int(os.getenv("CACHE_SYNC_CAR_TTL_SEC", "86400"))

        # ── XAUTOCLAIM settings (orphaned message recovery) ────────────────
        # Claim messages that have been idle (un-ACK'd) longer than this
        self.AUTOCLAIM_IDLE_MS: int = int(
            os.getenv("CACHE_SYNC_AUTOCLAIM_MS", "1800000")  # 30 minutes
        )
        self.AUTOCLAIM_COUNT: int = int(
            os.getenv("CACHE_SYNC_AUTOCLAIM_COUNT", "50")
        )

    def log_config(self) -> None:
        """Log all active settings at INFO level (sensitive values masked)."""
        logger.info(
            "[Config] CacheSyncSettings loaded | "
            "stream=%s | group=%s | consumer=%s | batch=%d | block_ms=%d | "
            "max_retries=%d | car_ttl=%ds | autoclaim_idle=%dms",
            self.STREAM_KEY,
            self.CONSUMER_GROUP,
            self.CONSUMER_NAME,
            self.BATCH_SIZE,
            self.BLOCK_MS,
            self.MAX_RETRIES,
            self.CAR_TTL_SEC,
            self.AUTOCLAIM_IDLE_MS,
        )
