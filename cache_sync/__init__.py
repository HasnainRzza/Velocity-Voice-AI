"""
cache_sync — Redis Cache Synchronisation Service.

This standalone service listens to the Redis Stream published by the
ingestion scraper and keeps the Redis `car:{id}` cache in sync with
ChromaDB in real-time.

Run:
    python -m cache_sync            # from inside ingestion/
    python cache_sync/main.py       # from inside ingestion/

The service is designed to run continuously alongside the Voice AI service.
When the scraper detects and publishes inventory changes, this service
consumes those events and updates only the affected Redis keys within
milliseconds — without rebuilding the entire cache.
"""
