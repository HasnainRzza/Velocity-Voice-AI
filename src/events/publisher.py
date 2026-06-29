"""
publisher.py — RedisEventPublisher.

Responsible for:
  • Connecting to Redis Streams using a synchronous redis-py client.
  • Publishing an InventoryEvent to the configured stream after every
    successful ChromaDB upsert or delete in the scraper pipeline.
  • Retrying transient connection errors with exponential back-off.
  • Never failing the scraper pipeline — if Redis is unavailable the
    error is logged as a warning and execution continues (ChromaDB is
    the source of truth; the Cache Sync Service will catch up on the
    next scraper run once Redis recovers).
  • Capping stream length with MAXLEN ~ to prevent unbounded growth.

Usage (from main.py):
    publisher = RedisEventPublisher.from_settings()
    try:
        publisher.publish_delta(delta_result)
    finally:
        publisher.close()
"""

import logging
import time
from typing import Any

import redis

from src.events.schema import InventoryEvent
from config.settings import (
    REDIS_URL,
    EVENTS_STREAM_KEY,
    EVENTS_MAX_LEN,
    EVENTS_PUBLISH_MAX_RETRIES,
    EVENTS_PUBLISH_RETRY_DELAY_SEC,
    DEFAULT_COLLECTION,
)

logger = logging.getLogger(__name__)


class RedisEventPublisher:
    """
    Publishes InventoryEvent objects to a Redis Stream.

    Responsibilities:
        - Build properly formatted stream entries.
        - Handle transient Redis failures with configurable retry logic.
        - Never propagate exceptions to the caller — log and return False.
        - Report per-run publishing statistics for observability.

    Design notes:
        - Uses a synchronous redis-py client because the ingestion pipeline
          is synchronous (asyncio not available in the scraper context).
        - The stream is capped at EVENTS_MAX_LEN entries using approximate
          MAXLEN trimming to avoid unbounded Redis memory growth.
        - Events are published only AFTER ChromaDB operations succeed, so
          every event corresponds to a confirmed persistent state change.
    """

    def __init__(
        self,
        redis_url: str,
        stream_key: str,
        max_len: int = 10_000,
        max_retries: int = 3,
        retry_delay_sec: float = 1.0,
        collection: str = DEFAULT_COLLECTION,
    ) -> None:
        self._stream_key = stream_key
        self._max_len = max_len
        self._max_retries = max_retries
        self._retry_delay = retry_delay_sec
        self._collection = collection
        self._client: redis.Redis = redis.from_url(redis_url, decode_responses=True)
        logger.info(
            "[Publisher] Initialised | stream=%s | max_len=%s | retries=%s",
            stream_key,
            max_len,
            max_retries,
        )

    # ── Factory ───────────────────────────────────────────────────────────

    @classmethod
    def from_settings(cls) -> "RedisEventPublisher":
        """Create a publisher using values from config/settings.py."""
        return cls(
            redis_url=REDIS_URL,
            stream_key=EVENTS_STREAM_KEY,
            max_len=EVENTS_MAX_LEN,
            max_retries=EVENTS_PUBLISH_MAX_RETRIES,
            retry_delay_sec=EVENTS_PUBLISH_RETRY_DELAY_SEC,
            collection=DEFAULT_COLLECTION,
        )

    # ── Public API ────────────────────────────────────────────────────────

    def publish(self, event: InventoryEvent) -> bool:
        """
        Publish a single InventoryEvent to the Redis Stream.

        Retries on ConnectionError / TimeoutError with exponential back-off.
        After all retries are exhausted, logs an error and returns False
        without raising — the scraper pipeline must not fail because Redis
        is temporarily unavailable.

        Args:
            event: The InventoryEvent to publish.

        Returns:
            True if the event was successfully published, False otherwise.
        """
        fields = event.to_stream_fields()

        for attempt in range(1, self._max_retries + 1):
            try:
                stream_id = self._client.xadd(
                    self._stream_key,
                    fields,
                    maxlen=self._max_len,
                    approximate=True,
                )
                logger.info(
                    "[Publisher] Published event | stream_id=%s | op=%s | doc_id=%s | event_id=%s",
                    stream_id,
                    event.operation,
                    event.document_id,
                    event.event_id,
                )
                return True

            except (redis.ConnectionError, redis.TimeoutError) as exc:
                delay = self._retry_delay * attempt
                if attempt < self._max_retries:
                    logger.warning(
                        "[Publisher] Attempt %d/%d failed for doc_id=%s — retrying in %.1fs. Error: %s",
                        attempt,
                        self._max_retries,
                        event.document_id,
                        delay,
                        exc,
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        "[Publisher] All %d attempts exhausted for doc_id=%s. "
                        "Event NOT published — Redis may be down. Error: %s",
                        self._max_retries,
                        event.document_id,
                        exc,
                    )

            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "[Publisher] Unexpected error publishing doc_id=%s: %s",
                    event.document_id,
                    exc,
                    exc_info=True,
                )
                break  # non-retriable error

        return False

    def publish_delta(self, delta_result: dict[str, Any]) -> dict[str, int]:
        """
        Publish InventoryEvents for all changes detected in a scraper run.

        This method is called AFTER sync_delta_to_chroma() succeeds, so
        every event published here corresponds to a confirmed ChromaDB state.

        Publishes:
            - "upsert" event for each new addition.
            - "upsert" event for each updated vehicle.
            - "delete" event for each removed vehicle.

        Args:
            delta_result: The dict returned by process_scrape_result().

        Returns:
            dict with keys "published", "failed", "skipped".
        """
        published = 0
        failed = 0

        # ── Additions ─────────────────────────────────────────────────────
        additions: list[dict[str, Any]] = delta_result.get("additions", [])
        for car in additions:
            car_id = str(car.get("id", ""))
            if not car_id:
                logger.warning("[Publisher] Skipping addition with no id: %s", car)
                continue
            event = InventoryEvent(
                operation="upsert",
                collection=self._collection,
                document_id=car_id,
                document=car,
                metadata={
                    "name": str(car.get("name", "")),
                    "price": int(car.get("price", 0) or 0),
                },
            )
            if self.publish(event):
                published += 1
            else:
                failed += 1

        # ── Updates ───────────────────────────────────────────────────────
        updates: list[dict[str, Any]] = delta_result.get("updates", [])
        for update in updates:
            car = update.get("car_data", {})
            car_id = str(car.get("id", ""))
            if not car_id:
                logger.warning("[Publisher] Skipping update with no id: %s", update)
                continue
            event = InventoryEvent(
                operation="upsert",
                collection=self._collection,
                document_id=car_id,
                document=car,
                metadata={
                    "name": str(car.get("name", "")),
                    "price": int(car.get("price", 0) or 0),
                },
            )
            if self.publish(event):
                published += 1
            else:
                failed += 1

        # ── Deletions ─────────────────────────────────────────────────────
        deletions: list[dict[str, Any]] = delta_result.get("deletions", [])
        for car in deletions:
            car_id = str(car.get("id", ""))
            if not car_id:
                logger.warning("[Publisher] Skipping deletion with no id: %s", car)
                continue
            event = InventoryEvent(
                operation="delete",
                collection=self._collection,
                document_id=car_id,
                document=None,   # deleted — no document
                metadata=None,   # deleted — availability implicitly null
            )
            if self.publish(event):
                published += 1
            else:
                failed += 1

        stats = {
            "published": published,
            "failed": failed,
            "total": len(additions) + len(updates) + len(deletions),
        }
        logger.info(
            "[Publisher] Delta publish complete | published=%d | failed=%d | total=%d",
            stats["published"],
            stats["failed"],
            stats["total"],
        )
        return stats

    def close(self) -> None:
        """Close the underlying Redis connection pool."""
        try:
            self._client.close()
            logger.info("[Publisher] Redis connection closed.")
        except Exception as exc:  # noqa: BLE001
            logger.warning("[Publisher] Error closing Redis connection: %s", exc)
