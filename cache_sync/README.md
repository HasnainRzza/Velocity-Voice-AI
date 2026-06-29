# Redis Cache Synchronisation Service

## Overview

This document describes the architecture, event flow, deployment guide, and
extension points for the **Redis Cache Synchronisation Service** — the component
that keeps Redis permanently in sync with ChromaDB without requiring a Voice AI
service restart.

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                          INGESTION SERVICE                             │
│                                                                        │
│  inventory_scraper.py                                                  │
│       │                                                                │
│       ▼  (detect delta)                                               │
│  delta_worker.py                                                       │
│       │                                                                │
│       ▼  (upsert / delete)                                            │
│  vector_store.py  ──────────────────────► ChromaDB Cloud              │
│       │                                   (source of truth)           │
│       │  (only on ChromaDB success)                                   │
│       ▼                                                                │
│  events/publisher.py                                                   │
│       │  XADD genesis:inventory:events                                │
│       ▼                                                                │
│  Redis Stream: genesis:inventory:events                                │
└───────────────────────────┬────────────────────────────────────────────┘
                            │
                ┌───────────▼────────────┐
                │  CACHE SYNC SERVICE    │
                │  cache_sync/           │
                │                        │
                │  EventConsumer         │
                │   XREADGROUP           │
                │       │                │
                │       ▼                │
                │  CacheSyncHandler      │
                │   upsert → SETEX       │
                │   delete → soft-delete │
                │       │                │
                │       ▼                │
                │  Redis car:{id} keys   │
                └───────────┬────────────┘
                            │  (read-only)
                ┌───────────▼────────────┐
                │    VOICE AI SERVICE    │
                │  src/agent/retriever   │
                │  Redis fast-path       │
                │  + ChromaDB enrichment │
                └────────────────────────┘
```

---

## Data Flow (Step by Step)

| Step | Component | Action |
|------|-----------|--------|
| 1 | `inventory_scraper.py` | Scrapes the Genesis CPO website |
| 2 | `delta_worker.py` | Compares against saved state; finds additions, updates, deletions |
| 3 | `vector_store.py` | Upserts / deletes documents in ChromaDB Cloud |
| 4 | `events/publisher.py` | **Only if step 3 succeeds** — publishes `InventoryEvent` to Redis Stream |
| 5 | `cache_sync/event_consumer.py` | Reads events via `XREADGROUP`; dispatches to handler |
| 6 | `cache_sync/handler.py` | Writes `car:{id}` key to Redis (`SETEX` or soft-delete) |
| 7 | Voice AI `retriever.py` | Reads `car:{id}` keys from Redis; filters soft-deleted entries |

---

## Event Schema

```json
{
  "event_id":    "550e8400-e29b-41d4-a716-446655440000",
  "operation":   "upsert",
  "collection":  "genesis_inventory",
  "document_id": "12345",
  "timestamp":   "2026-06-28T11:40:26+00:00",
  "document": {
    "id":   "12345",
    "name": "GV80 3.5T ROYAL",
    "price": 290000,
    "url":  "https://...",
    "overview": { "TRANSMISSION": "AUTO" },
    "key_features": ["Panoramic roof"]
  },
  "metadata": {
    "name":  "GV80 3.5T ROYAL",
    "price": 290000
  }
}
```

For **delete** events, `document` and `metadata` are `null`:

```json
{
  "event_id":    "...",
  "operation":   "delete",
  "collection":  "genesis_inventory",
  "document_id": "12345",
  "timestamp":   "...",
  "document":    null,
  "metadata":    null
}
```

---

## Redis Key Format

```
Key:   car:{document_id}
Value: JSON
       {
         "id":       "12345",
         "document": "GV80 3.5T ROYAL. Price: 290,000 SAR. ...",
         "metadata": {
           "name":         "GV80 3.5T ROYAL",
           "price":        290000,
           "availability": "available"  |  null
         },
         "available": true  |  null
       }
```

`available: null` + `metadata.availability: null` = **soft-deleted** vehicle.
The Voice AI retriever automatically filters these out.

---

## Delete Behaviour: Soft Delete

When a vehicle is removed from inventory, the scraper publishes a `delete` event.
The Cache Sync handler does **NOT** delete the Redis key. Instead it:

1. Reads the existing `car:{id}` entry.
2. Sets `available = null` at the top level.
3. Sets `metadata.availability = null`.
4. Overwrites the Redis entry (SETEX, same TTL).

**Why soft delete?**
- The Voice AI can say "this vehicle is no longer available" if asked about it directly.
- Historical data is preserved for auditing.
- The retriever fast-path skips `available == null` entries automatically.
- Idempotent: repeating a delete event always produces the same result.

---

## Reliability

### Retry Policy
Each event is retried up to `MAX_RETRIES` times (default: 3) with linear back-off
before being quarantined to the Dead-Letter Queue.

### Dead-Letter Queue (DLQ)
Permanently failed events are written to `genesis:inventory:events:dlq`:
```
_dlq_original_id  : original stream entry ID
_dlq_stream       : source stream key
_dlq_consumer     : consumer that failed
_dlq_error        : last exception message
_dlq_timestamp    : UTC timestamp of DLQ insertion
```

Inspect the DLQ with:
```bash
redis-cli XRANGE genesis:inventory:events:dlq - +
```

### PEL Recovery
On restart, the consumer re-reads any messages that were delivered but not ACK'd
before the previous crash. No events are lost.

### XAUTOCLAIM
Every 20 batch iterations, the consumer runs `XAUTOCLAIM` to reclaim messages
idle in the PEL for > 30 minutes. This handles permanently dead consumers.

### Publisher Failure Mode
If Redis is unavailable during event publishing, the error is **logged as a WARNING**
and the scraper pipeline continues. ChromaDB is the source of truth.

---

## Deployment

### Prerequisites
- Redis >= 6.2 (for `XAUTOCLAIM`; falls back gracefully on older versions)
- Python 3.11+
- `redis>=5.0.0` (already in `ingestion/requirements.txt`)

### Running the Services

**Terminal 1 — Cache Sync Service (long-running)**
```bash
cd ingestion
python cache_sync/main.py
```

**Terminal 2 — Scraper Pipeline (run on schedule)**
```bash
cd ingestion
python main.py
```

**Voice AI (unchanged)**
```bash
cd voice-ai/src
uvicorn main:app --host 0.0.0.0 --port 8000
```

### Recommended Startup Order
1. Redis must be running first.
2. Start the **Cache Sync Service** — it waits for new events.
3. Start the **Voice AI** — warm-up cache runs once at startup.
4. Run the **Scraper Pipeline** on schedule (cron, Task Scheduler, etc.).

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `EVENTS_STREAM_KEY` | `genesis:inventory:events` | Stream key |
| `EVENTS_MAX_LEN` | `10000` | Max stream entries (approximate) |
| `CACHE_SYNC_CONSUMER_GROUP` | `cache-sync` | Consumer group name |
| `CACHE_SYNC_CONSUMER_NAME` | `<hostname>-cache-sync` | Unique consumer name |
| `CACHE_SYNC_BATCH_SIZE` | `10` | Messages per XREADGROUP call |
| `CACHE_SYNC_BLOCK_MS` | `5000` | XREADGROUP block timeout |
| `CACHE_SYNC_MAX_RETRIES` | `3` | Per-message handler retries |
| `CACHE_SYNC_RETRY_DELAY_SEC` | `1.0` | Seconds between retries |
| `CACHE_SYNC_DLQ_KEY` | `genesis:inventory:events:dlq` | Dead-letter stream key |
| `CACHE_SYNC_CAR_TTL_SEC` | `86400` | Redis key TTL (24h) |
| `CACHE_SYNC_AUTOCLAIM_MS` | `1800000` | Orphan claim threshold (30 min) |
| `CACHE_SYNC_AUTOCLAIM_COUNT` | `50` | Max orphans claimed per run |
| `LOG_LEVEL` | `INFO` | Log verbosity |

---

## Adding a New Consumer

Because the event bus is a Redis Stream with consumer groups, any new service
can subscribe without modifying the scraper or the Cache Sync Service.

### Steps

1. **Create a consumer group** for your service:
   ```bash
   redis-cli XGROUP CREATE genesis:inventory:events my-new-service $ MKSTREAM
   ```

2. **Read events** using your own group:
   ```python
   redis_client.xreadgroup(
       groupname="my-new-service",
       consumername="instance-1",
       streams={"genesis:inventory:events": ">"},
       count=10, block=5000,
   )
   ```

3. **ACK** each processed message:
   ```python
   redis_client.xack("genesis:inventory:events", "my-new-service", entry_id)
   ```

Each consumer group maintains its own independent position. The scraper publishes
once; all consumer groups receive every event independently.

---

## Testing Checklist

- [ ] Run scraper — check `redis-cli XLEN genesis:inventory:events`
- [ ] Run cache sync — check `redis-cli KEYS car:*`
- [ ] Simulate a delete, re-run scraper — verify `available=null` in the key
- [ ] Kill cache sync mid-run, restart — verify PEL messages are replayed
- [ ] Check DLQ is empty: `redis-cli XLEN genesis:inventory:events:dlq`
- [ ] Start Voice AI — warm-up succeeds without touching null-availability keys
