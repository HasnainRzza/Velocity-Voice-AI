"""
handler.py — CacheSyncHandler.

Responsible for applying a single InventoryEvent to the Redis cache.
This is the only component in the entire system that WRITES to Redis
post-startup (the Voice AI warm_up_cache() is the only startup writer).

Supported operations
────────────────────
  upsert  — Build the `car:{id}` Redis entry from the event document/metadata
             and store it with a configurable TTL (SETEX).

  delete  — SOFT-DELETE: instead of removing the Redis key, the handler
             marks the vehicle as unavailable by setting `available` and
             `metadata.availability` to null (JSON null). This preserves
             the car record in Redis so:
               • The Voice AI can still recognise the car ID.
               • Retriever fast-path filters it out via `available is null`.
               • Historical data is retained for auditing.

Idempotency
───────────
  Both operations are fully idempotent:
  • upsert always overwrites the existing key (SETEX is atomic).
  • delete (soft) always sets the same null fields regardless of current state.
  Duplicate events therefore never corrupt the cache.

Redis key format (preserved from warm_up_cache)
────────────────────────────────────────────────
  Key:   car:{document_id}
  Value: JSON string matching the shape expected by the Voice AI retriever:
           {
             "id":       str,
             "document": str,   # natural-language text for the car
             "metadata": {
               "name":         str,
               "price":        int,
               "availability": str | null,   # null = deleted
               ...
             },
             "available": true | null        # null = soft-deleted
           }
"""

import json
import logging
import sys
import os
from typing import Any

import redis

# Allow cache_sync to import from the parent ingestion package
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.events.schema import InventoryEvent
from src.processor.text_formatter import convert_car_to_text
from cache_sync.config import CacheSyncSettings

logger = logging.getLogger(__name__)


class CacheSyncHandler:
    """
    Applies InventoryEvents to the Redis `car:{id}` cache.

    Attributes:
        _redis:    Synchronous redis.Redis client (shared with consumer).
        _settings: CacheSyncSettings instance.
    """

    def __init__(self, redis_client: redis.Redis, settings: CacheSyncSettings) -> None:
        self._redis = redis_client
        self._ttl = settings.CAR_TTL_SEC
        logger.info(
            "[Handler] Initialised | car_ttl=%ds", self._ttl
        )

    # ── Public dispatch ───────────────────────────────────────────────────

    def handle(self, event: InventoryEvent) -> None:
        """
        Route the event to the correct handler method.

        Args:
            event: A fully deserialised InventoryEvent.

        Raises:
            ValueError:    if the operation type is unrecognised.
            redis.RedisError: propagated to the consumer for retry logic.
        """
        logger.info(
            "[Handler] Processing event | op=%s | doc_id=%s | event_id=%s",
            event.operation,
            event.document_id,
            event.event_id,
        )

        if event.operation == "upsert":
            self._handle_upsert(event)
        elif event.operation == "delete":
            self._handle_delete(event)
        else:
            raise ValueError(
                f"[Handler] Unknown operation '{event.operation}' for doc_id={event.document_id}"
            )

    # ── Upsert ────────────────────────────────────────────────────────────

    def _handle_upsert(self, event: InventoryEvent) -> None:
        """
        Create or replace a `car:{id}` Redis key.

        The document text is (re-)generated from the raw car dict using the
        same convert_car_to_text() function used during ingestion, ensuring
        the Redis entry is always consistent with ChromaDB's document.
        """
        key = f"car:{event.document_id}"

        # Build the natural-language document text from the raw car dict
        document_text: str = ""
        if event.document:
            try:
                document_text = convert_car_to_text(event.document)
            except Exception as exc:
                logger.warning(
                    "[Handler] Could not generate document text for doc_id=%s: %s",
                    event.document_id,
                    exc,
                )

        # Build the metadata dict — use what the event carries, fallback to car fields
        metadata: dict[str, Any] = {}
        if event.metadata:
            metadata = dict(event.metadata)
        elif event.document:
            metadata = {
                "name": str(event.document.get("name", "")),
                "price": int(event.document.get("price", 0) or 0),
            }

        # Mark as available (explicitly — so the retriever fast-path can filter)
        metadata["availability"] = "available"

        # Build the full Redis payload matching the Voice AI retriever's expected schema
        payload: dict[str, Any] = {
            "id": event.document_id,
            "document": document_text,
            "metadata": metadata,
            "available": True,  # True = in inventory; null = soft-deleted
        }

        self._redis.setex(key, self._ttl, json.dumps(payload, ensure_ascii=False))
        logger.info(
            "[Handler] UPSERT | key=%s | name=%s | price=%s | ttl=%ds",
            key,
            metadata.get("name", ""),
            metadata.get("price", ""),
            self._ttl,
        )

    # ── Soft-delete ───────────────────────────────────────────────────────

    def _handle_delete(self, event: InventoryEvent) -> None:
        """
        Soft-delete a `car:{id}` key by marking availability as null.

        The Redis key is preserved so the Voice AI can still recognise
        the car ID in past conversations. The retriever's fast-path skips
        cars with `available == null` so soft-deleted cars never appear
        in search results.

        If the key does not yet exist in Redis (e.g. the car was never
        warmed into the cache), a minimal placeholder is created so the
        record is consistently marked as unavailable.
        """
        key = f"car:{event.document_id}"
        existing_str: str | None = self._redis.get(key)  # type: ignore[assignment]

        if existing_str:
            # Enrich the existing record with null-availability markers
            try:
                car_data: dict[str, Any] = json.loads(existing_str)
            except json.JSONDecodeError:
                logger.warning(
                    "[Handler] Corrupted Redis value for %s — replacing with placeholder.", key
                )
                car_data = {"id": event.document_id}

            # Set availability to null at both top-level and inside metadata
            car_data["available"] = None
            metadata = car_data.get("metadata", {})
            metadata["availability"] = None   # JSON null — explicitly deleted
            car_data["metadata"] = metadata

            self._redis.setex(key, self._ttl, json.dumps(car_data, ensure_ascii=False))
            logger.info(
                "[Handler] SOFT-DELETE | key=%s | availability set to null | ttl=%ds",
                key,
                self._ttl,
            )
        else:
            # Car was not in Redis — create a minimal null-availability placeholder
            placeholder: dict[str, Any] = {
                "id": event.document_id,
                "document": "",
                "metadata": {
                    "availability": None,   # null — car is no longer available
                },
                "available": None,          # null — explicitly deleted
            }
            self._redis.setex(key, self._ttl, json.dumps(placeholder, ensure_ascii=False))
            logger.info(
                "[Handler] SOFT-DELETE (placeholder) | key=%s | car not found in cache; "
                "created null-availability entry | ttl=%ds",
                key,
                self._ttl,
            )
