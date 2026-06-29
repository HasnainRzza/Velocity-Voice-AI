"""
main.py — Genesis CPO Inventory Pipeline.

Pipeline stages:
    1. Scrape the Genesis CPO inventory website.
    2. Detect delta vs. previously saved state (additions / updates / deletions).
    3. Sync confirmed changes to ChromaDB (source of truth).
    4. Publish InventoryEvents to the Redis Stream so downstream consumers
       (e.g. Cache Sync Service) can update Redis in real-time.
    5. Persist the new inventory state to disk only after ChromaDB succeeds.

Event publishing (stage 4) is best-effort: if Redis is temporarily unavailable,
a WARNING is logged and the pipeline continues without raising. ChromaDB remains
the source of truth; the Cache Sync Service will reconcile on the next run.
"""

import asyncio
import logging
import sys

from src.storage.vector_store import sync_delta_to_chroma
from src.processor.delta_worker import (
    commit_inventory_state,
    print_ingestion_report,
    process_scrape_result,
)
from src.scraper.inventory_scraper import scrape_full_genesis_inventory
from src.events.publisher import RedisEventPublisher
from config.settings import STATE_FILE

from config.logger import setup_async_logging

setup_async_logging(log_file_name="ingestion_pipeline.log", level=logging.INFO)
logger = logging.getLogger(__name__)


async def run_pipeline() -> dict:
    """Run scrape → delta detection → ChromaDB ingest → event publish end-to-end."""
    logger.info("=" * 60)
    logger.info("GENESIS INVENTORY PIPELINE STARTED")
    logger.info("=" * 60)

    # ── Stage 1: Scrape ────────────────────────────────────────────────────
    logger.info("[1/4] Scraping Genesis CPO inventory...")
    inventory = await scrape_full_genesis_inventory()
    logger.info("[1/4] Scraped %d vehicles.", len(inventory))

    # ── Stage 2: Delta detection ───────────────────────────────────────────
    logger.info("[2/4] Detecting inventory changes...")
    delta_result = process_scrape_result(inventory, state_filepath=str(STATE_FILE))
    print_ingestion_report(delta_result)
    logger.info(
        "[2/4] Delta: +%d additions | ~%d updates | -%d deletions",
        len(delta_result["additions"]),
        len(delta_result["updates"]),
        len(delta_result["deletions"]),
    )

    # ── Stage 3: ChromaDB sync ────────────────────────────────────────────
    logger.info("[3/4] Syncing changes to ChromaDB...")
    try:
        sync_stats = sync_delta_to_chroma(delta_result)
        commit_inventory_state(delta_result["state_filepath"], inventory)

        run_label = "full ingest" if delta_result["is_first_run"] else "delta ingest"
        logger.info(
            "[3/4] ChromaDB sync complete (%s): %d upserted, %d deleted.",
            run_label,
            sync_stats["upserted"],
            sync_stats["deleted"],
        )
        logger.info("[3/4] State saved to %s.", STATE_FILE)
    except Exception as sync_error:
        logger.error(
            "[3/4] ChromaDB sync FAILED. Inventory state was NOT updated — "
            "next run will retry unchanged records. Error: %s",
            sync_error,
            exc_info=True,
        )
        raise

    # ── Stage 4: Publish events to Redis Stream ───────────────────────────
    # NOTE: This stage runs ONLY after ChromaDB succeeds.
    # If Redis is unavailable, we warn and continue — never fail the pipeline.
    logger.info("[4/4] Publishing inventory events to Redis Stream...")
    publisher = RedisEventPublisher.from_settings()
    try:
        publish_stats = publisher.publish_delta(delta_result)
        logger.info(
            "[4/4] Event publish complete: %d published | %d failed | %d total.",
            publish_stats["published"],
            publish_stats["failed"],
            publish_stats["total"],
        )
        if publish_stats["failed"] > 0:
            logger.warning(
                "[4/4] %d event(s) could not be published. "
                "Cache Sync Service will reconcile on the next pipeline run.",
                publish_stats["failed"],
            )
    except Exception as publish_error:
        # Best-effort: log the error but never propagate it
        logger.warning(
            "[4/4] Event publishing encountered an unexpected error: %s. "
            "ChromaDB is intact. Cache Sync Service will reconcile on the next run.",
            publish_error,
            exc_info=True,
        )
        publish_stats = {"published": 0, "failed": 0, "total": 0}
    finally:
        publisher.close()

    logger.info("=" * 60)
    logger.info("GENESIS INVENTORY PIPELINE COMPLETE")
    logger.info("=" * 60)

    return {
        "inventory_count": len(inventory),
        "delta_result": delta_result,
        "sync_stats": sync_stats,
        "publish_stats": publish_stats,
    }


def main() -> None:
    try:
        asyncio.run(run_pipeline())
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user.")
        sys.exit(130)
    except Exception as error:
        logger.error("Pipeline failed: %s", error, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
