"""
events — Inventory change event schema and Redis Stream publisher.

The scraper publishes an InventoryEvent to Redis Streams after every
successful ChromaDB upsert or delete. Downstream consumers (e.g. the
Redis Cache Synchronization Service) subscribe to the same stream and
react to each event independently, without any coupling to the scraper.
"""
