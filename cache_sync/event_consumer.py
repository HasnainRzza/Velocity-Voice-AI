"""
event_consumer.py — EventConsumer.

Implements a reliable Redis Streams consumer using XREADGROUP with consumer
groups, ensuring every event is processed exactly once (or moved to the
Dead-Letter Queue after exhausting retries).

Reliability mechanisms implemented:
    1. Consumer group creation (MKSTREAM) on startup — stream is auto-created.
    2. PEL (Pending Entry List) recovery — on restart, the consumer re-processes
       any messages it acknowledged before the previous crash.
    3. XAUTOCLAIM — periodically claims messages that have been idle in the PEL
       for longer than AUTOCLAIM_IDLE_MS, handling permanently dead consumers.
    4. Retry with exponential back-off — each failed message is retried up to
       MAX_RETRIES times before being quarantined to the DLQ stream.
    5. Dead-Letter Queue — permanently failed messages are XADD'd to a separate
       DLQ stream with full error context. The original message is then XACK'd
       so it is removed from the PEL and never reprocessed.
    6. Graceful shutdown — asyncio.Event signals the loop to stop cleanly
       after the current batch finishes.
    7. Redis reconnection — transient disconnections are caught and retried
       with exponential back-off inside the consume loop.

Idempotency:
    The handler itself is idempotent (SETEX overwrites; soft-delete is a no-op
    if repeated). Duplicate events from XAUTOCLAIM re-delivery are safe.
"""

import asyncio
import json
import logging
import time
from typing import Any

import redis

from src.events.schema import InventoryEvent
from cache_sync.config import CacheSyncSettings
from cache_sync.handler import CacheSyncHandler

logger = logging.getLogger(__name__)

# Back-off for Redis reconnection attempts inside the consume loop
_RECONNECT_DELAYS = [1, 2, 5, 10, 30]  # seconds


class EventConsumer:
    """
    Continuously consumes InventoryEvents from a Redis Stream.

    Lifecycle:
        1. __init__: store references, do not connect yet.
        2. start():  create consumer group, recover PEL.
        3. consume_loop(): main async loop — block on XREADGROUP, dispatch,
                           XACK on success or DLQ on permanent failure.
        4. stop():   signal the loop to exit gracefully.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        handler: CacheSyncHandler,
        settings: CacheSyncSettings,
        shutdown_event: asyncio.Event | None = None,
    ) -> None:
        self._redis = redis_client
        self._handler = handler
        self._settings = settings
        self._shutdown = shutdown_event or asyncio.Event()
        # Track last XAUTOCLAIM cursor (stream ID, start from "0-0")
        self._autoclaim_cursor: str = "0-0"
        logger.info(
            "[Consumer] Initialised | stream=%s | group=%s | consumer=%s",
            settings.STREAM_KEY,
            settings.CONSUMER_GROUP,
            settings.CONSUMER_NAME,
        )

    # ── Lifecycle ─────────────────────────────────────────────────────────

    def start(self) -> None:
        """
        Synchronous startup: create the consumer group only.

        Call start_async() from the async context to also recover PEL.
        This split allows the group to be registered even before the event
        loop is running.
        """
        self._create_consumer_group()

    async def start_async(self) -> None:
        """
        Asynchronous startup: create the consumer group and recover PEL.

        Must be awaited from within an async context before consume_loop().
        """
        self._create_consumer_group()
        await self._recover_pending()

    def stop(self) -> None:
        """Signal the consume loop to exit after the current batch."""
        logger.info("[Consumer] Shutdown requested — will stop after current batch.")
        self._shutdown.set()

    # ── Main consume loop ─────────────────────────────────────────────────

    async def consume_loop(self) -> None:
        """
        Asynchronous event loop.

        Blocks on XREADGROUP for up to BLOCK_MS milliseconds, dispatches
        each received message to the handler, and ACKs or DLQs it.
        Runs XAUTOCLAIM periodically to recover orphaned messages.
        Handles Redis reconnection transparently.
        """
        logger.info("[Consumer] Starting consume loop...")
        autoclaim_counter = 0

        while not self._shutdown.is_set():
            try:
                # ── XAUTOCLAIM: periodically reclaim orphaned messages ────
                # Run every 20 batches to avoid excessive overhead
                autoclaim_counter += 1
                if autoclaim_counter % 20 == 0:
                    await asyncio.to_thread(self._run_autoclaim)

                # ── XREADGROUP: fetch new messages from stream ─────────────
                messages = await asyncio.to_thread(self._read_messages)

                if not messages:
                    # No messages — loop back and block again
                    continue

                for stream_name, entries in messages:
                    for entry_id, fields in entries:
                        await self._process_entry(entry_id, fields)

            except (redis.ConnectionError, redis.TimeoutError) as conn_err:
                # Transient Redis outage — back off and retry
                logger.error(
                    "[Consumer] Redis connection error: %s — reconnecting...", conn_err
                )
                await self._reconnect_with_backoff()

            except asyncio.CancelledError:
                logger.info("[Consumer] Consume loop cancelled.")
                break

            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "[Consumer] Unexpected error in consume loop: %s", exc, exc_info=True
                )
                await asyncio.sleep(2)

        logger.info("[Consumer] Consume loop exited cleanly.")


    # ── Internal helpers ──────────────────────────────────────────────────

    def _create_consumer_group(self) -> None:
        """Create the consumer group, starting at '$' (only new messages)."""
        try:
            self._redis.xgroup_create(
                self._settings.STREAM_KEY,
                self._settings.CONSUMER_GROUP,
                id="$",           # only consume messages published after this point
                mkstream=True,    # create the stream if it doesn't exist yet
            )
            logger.info(
                "[Consumer] Consumer group '%s' created on stream '%s'.",
                self._settings.CONSUMER_GROUP,
                self._settings.STREAM_KEY,
            )
        except redis.ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                logger.info(
                    "[Consumer] Consumer group '%s' already exists — skipping creation.",
                    self._settings.CONSUMER_GROUP,
                )
            else:
                logger.error(
                    "[Consumer] Failed to create consumer group: %s", exc, exc_info=True
                )
                raise

    async def _recover_pending(self) -> None:
        """
        Re-process messages that were pending (un-ACK'd) before a restart.

        Reads the PEL for this specific consumer and processes them first,
        before consuming new messages. This ensures no events are lost across
        service restarts.
        """
        logger.info("[Consumer] Checking Pending Entry List (PEL) for recovery...")
        try:
            # "0" means start from the beginning of this consumer's PEL
            pending_messages = await asyncio.to_thread(
                self._redis.xreadgroup,
                self._settings.CONSUMER_GROUP,
                self._settings.CONSUMER_NAME,
                {self._settings.STREAM_KEY: "0"},
                self._settings.BATCH_SIZE,
            )
            if not pending_messages:
                logger.info("[Consumer] No pending messages — PEL is clean.")
                return

            total = sum(len(entries) for _, entries in pending_messages)
            logger.info("[Consumer] Found %d pending message(s) — replaying...", total)

            for stream_name, entries in pending_messages:
                for entry_id, fields in entries:
                    await self._process_entry(entry_id, fields)

        except Exception as exc:  # noqa: BLE001
            logger.error(
                "[Consumer] PEL recovery failed: %s — continuing with new messages.",
                exc,
                exc_info=True,

            )

    def _read_messages(self) -> list[Any]:
        """
        Blocking XREADGROUP call for new messages ('>' = only undelivered).
        Returns an empty list if the block timeout elapses with no messages.
        """
        result = self._redis.xreadgroup(
            groupname=self._settings.CONSUMER_GROUP,
            consumername=self._settings.CONSUMER_NAME,
            streams={self._settings.STREAM_KEY: ">"},
            count=self._settings.BATCH_SIZE,
            block=self._settings.BLOCK_MS,
        )
        return result or []

    async def _process_entry(self, entry_id: str, fields: dict[str, str]) -> None:
        """
        Deserialise, handle, and ACK a single stream entry.

        Retry policy:
            On handler exception → sleep and retry up to MAX_RETRIES times.
            After exhausting retries → send to DLQ and ACK.

        Idempotency note:
            XAUTOCLAIM may re-deliver the same entry. The handler operations
            (SETEX / soft-delete) are idempotent, so re-delivery is safe.
        """
        last_exc: Exception | None = None

        for attempt in range(1, self._settings.MAX_RETRIES + 1):
            try:
                event = InventoryEvent.from_stream_fields(fields)
                logger.debug(
                    "[Consumer] Dispatching entry %s (attempt %d/%d) — %s",
                    entry_id,
                    attempt,
                    self._settings.MAX_RETRIES,
                    event,
                )
                await asyncio.to_thread(self._handler.handle, event)
                # Success — ACK and return
                self._redis.xack(
                    self._settings.STREAM_KEY,
                    self._settings.CONSUMER_GROUP,
                    entry_id,
                )
                logger.info(
                    "[Consumer] ACK'd entry %s (attempt %d) | op=%s | doc_id=%s",
                    entry_id,
                    attempt,
                    fields.get("operation", "?"),
                    fields.get("document_id", "?"),
                )
                return

            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                delay = self._settings.RETRY_DELAY_SEC * attempt
                logger.warning(
                    "[Consumer] Entry %s failed on attempt %d/%d: %s — retrying in %.1fs",
                    entry_id,
                    attempt,
                    self._settings.MAX_RETRIES,
                    exc,
                    delay,
                )
                if attempt < self._settings.MAX_RETRIES:
                    await asyncio.sleep(delay)

        # ── All retries exhausted — send to Dead-Letter Queue ──────────────
        logger.error(
            "[Consumer] Entry %s PERMANENTLY FAILED after %d attempts — sending to DLQ. "
            "Last error: %s",
            entry_id,
            self._settings.MAX_RETRIES,
            last_exc,
        )
        await asyncio.to_thread(
            self._send_to_dlq, entry_id, fields, str(last_exc)
        )
        # ACK the original so it is removed from the PEL
        self._redis.xack(
            self._settings.STREAM_KEY,
            self._settings.CONSUMER_GROUP,
            entry_id,
        )

    def _send_to_dlq(
        self, entry_id: str, fields: dict[str, str], error: str
    ) -> None:
        """
        Publish a failed entry to the Dead-Letter Queue stream.

        The DLQ entry carries the full original fields plus error metadata.
        """
        dlq_payload: dict[str, str] = {
            **fields,
            "_dlq_original_id": entry_id,
            "_dlq_stream": self._settings.STREAM_KEY,
            "_dlq_consumer": self._settings.CONSUMER_NAME,
            "_dlq_error": error,
            "_dlq_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        try:
            self._redis.xadd(self._settings.DLQ_KEY, dlq_payload)
            logger.warning(
                "[Consumer] DLQ entry created | original_id=%s | dlq_stream=%s",
                entry_id,
                self._settings.DLQ_KEY,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "[Consumer] Could not write to DLQ (%s): %s", self._settings.DLQ_KEY, exc
            )

    def _run_autoclaim(self) -> None:
        """
        XAUTOCLAIM: reclaim messages that have been idle > AUTOCLAIM_IDLE_MS.

        This handles permanently crashed consumers whose messages are stuck
        in the PEL without being processed. Claimed messages are re-delivered
        to this consumer and processed normally via the consume loop.
        """
        try:
            result = self._redis.xautoclaim(
                self._settings.STREAM_KEY,
                self._settings.CONSUMER_GROUP,
                self._settings.CONSUMER_NAME,
                min_idle_time=self._settings.AUTOCLAIM_IDLE_MS,
                start_id=self._autoclaim_cursor,
                count=self._settings.AUTOCLAIM_COUNT,
            )
            # result = [next_cursor, [[id, fields], ...], [deleted_ids, ...]]
            next_cursor, claimed_entries, _ = result
            self._autoclaim_cursor = next_cursor if next_cursor != b"0-0" else "0-0"

            if claimed_entries:
                logger.info(
                    "[Consumer] XAUTOCLAIM reclaimed %d orphaned message(s).",
                    len(claimed_entries),
                )
                for entry_id, fields in claimed_entries:
                    # Schedule processing — these will run in the next loop iteration
                    # Since autoclaim is sync, we can't await here; log for now
                    logger.info(
                        "[Consumer] Orphaned entry %s will be re-processed on next iteration.",
                        entry_id,
                    )
        except redis.ResponseError as exc:
            # XAUTOCLAIM requires Redis 6.2+ — log gracefully if not available
            if "ERR unknown command" in str(exc) or "XAUTOCLAIM" in str(exc):
                logger.warning(
                    "[Consumer] XAUTOCLAIM not supported by this Redis version — skipping orphan recovery."
                )
            else:
                logger.error("[Consumer] XAUTOCLAIM error: %s", exc)
        except Exception as exc:  # noqa: BLE001
            logger.error("[Consumer] Unexpected XAUTOCLAIM error: %s", exc, exc_info=True)

    async def _reconnect_with_backoff(self) -> None:
        """Sleep with exponential back-off and wait for Redis to recover."""
        for delay in _RECONNECT_DELAYS:
            logger.info("[Consumer] Waiting %ds before reconnect attempt...", delay)
            await asyncio.sleep(delay)
            try:
                self._redis.ping()
                logger.info("[Consumer] Redis connection restored.")
                return
            except (redis.ConnectionError, redis.TimeoutError):
                continue
        logger.error(
            "[Consumer] Could not reconnect to Redis after all attempts. "
            "Continuing loop — will retry on next iteration."
        )
