"""
schema.py — InventoryEvent dataclass.

Defines the canonical event envelope published to the Redis Stream after
every ChromaDB upsert or delete. All fields are JSON-serialisable so
they can be stored directly as Redis Stream entry fields.

Event lifecycle:
    1. Scraper detects change via delta_worker.
    2. ChromaDB is upserted / deleted (source of truth).
    3. InventoryEvent is published to the stream.
    4. Cache Sync Service consumes event and updates Redis.
"""

import json
import uuid
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Literal

logger = logging.getLogger(__name__)

# Supported operation types
OperationType = Literal["upsert", "delete"]


@dataclass
class InventoryEvent:
    """
    Canonical event envelope for Genesis inventory changes.

    Attributes:
        event_id:    Unique UUID4 string — used for idempotency checks.
        operation:   "upsert" for add/update, "delete" for removal.
        collection:  ChromaDB collection name (e.g. "genesis_inventory").
        document_id: Stable vehicle ID (matches ChromaDB document ID).
        timestamp:   ISO-8601 UTC timestamp of when the event was emitted.
        document:    Full raw car dict (present for upsert, None for delete).
        metadata:    Chroma metadata dict — name, price, etc. (None for delete).
    """

    operation: OperationType
    document_id: str
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    collection: str = "genesis_inventory"
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    document: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None

    # ── Serialisation helpers ──────────────────────────────────────────────

    def to_stream_fields(self) -> dict[str, str]:
        """
        Flatten the event into a dict of string values for Redis XADD.

        Redis Stream fields must be plain strings; nested dicts / None
        values are JSON-encoded so they can be decoded by the consumer.
        """
        raw = asdict(self)
        fields: dict[str, str] = {}
        for key, value in raw.items():
            if value is None:
                fields[key] = "null"
            elif isinstance(value, (dict, list)):
                fields[key] = json.dumps(value, ensure_ascii=False)
            else:
                fields[key] = str(value)
        return fields

    @classmethod
    def from_stream_fields(cls, fields: dict[str, str]) -> "InventoryEvent":
        """
        Reconstruct an InventoryEvent from raw Redis Stream field strings.

        Raises:
            ValueError: if required fields are missing or malformed.
        """
        try:
            operation: OperationType = fields["operation"]  # type: ignore[assignment]
            document_id: str = fields["document_id"]
            event_id: str = fields.get("event_id", str(uuid.uuid4()))
            collection: str = fields.get("collection", "genesis_inventory")
            timestamp: str = fields.get(
                "timestamp", datetime.now(timezone.utc).isoformat()
            )

            raw_document = fields.get("document", "null")
            document: dict[str, Any] | None = (
                json.loads(raw_document) if raw_document not in ("null", "") else None
            )

            raw_metadata = fields.get("metadata", "null")
            metadata: dict[str, Any] | None = (
                json.loads(raw_metadata) if raw_metadata not in ("null", "") else None
            )

            return cls(
                event_id=event_id,
                operation=operation,
                collection=collection,
                document_id=document_id,
                timestamp=timestamp,
                document=document,
                metadata=metadata,
            )
        except (KeyError, json.JSONDecodeError) as exc:
            logger.error("[Schema] Failed to deserialise stream fields: %s | raw=%s", exc, fields)
            raise ValueError(f"Invalid stream fields: {exc}") from exc

    def __repr__(self) -> str:
        return (
            f"InventoryEvent(op={self.operation!r}, id={self.document_id!r}, "
            f"event_id={self.event_id!r}, ts={self.timestamp!r})"
        )
