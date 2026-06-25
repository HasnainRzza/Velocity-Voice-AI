import asyncio
import sys

from src.storage.vector_store import sync_delta_to_chroma
from src.processor.delta_worker import (
    commit_inventory_state,
    print_ingestion_report,
    process_scrape_result,
)
from src.scraper.inventory_scraper import scrape_full_genesis_inventory
from config.settings import STATE_FILE


async def run_pipeline() -> dict:
    """Run scrape -> delta detection -> Chroma ingestion end to end."""
    print("=" * 60)
    print("GENESIS INVENTORY PIPELINE")
    print("=" * 60)

    print("\n[1/3] Scraping Genesis CPO inventory...")
    inventory = await scrape_full_genesis_inventory()
    print(f"Scraped {len(inventory)} vehicles.")

    print("\n[2/3] Detecting inventory changes...")
    delta_result = process_scrape_result(inventory, state_filepath=str(STATE_FILE))
    print_ingestion_report(delta_result)

    print("\n[3/3] Syncing to Chroma DB...")
    try:
        sync_stats = sync_delta_to_chroma(delta_result)
        commit_inventory_state(delta_result["state_filepath"], inventory)

        run_label = "full ingest" if delta_result["is_first_run"] else "delta ingest"
        print(
            f"\nPipeline complete ({run_label}): "
            f"{sync_stats['upserted']} upserted, {sync_stats['deleted']} deleted."
        )
        print(f"State saved to {STATE_FILE}.")
    except Exception as sync_error:
        print(
            "\nChroma sync failed. Inventory state was NOT updated so the next run "
            f"will retry unchanged records. Error: {sync_error}"
        )
        raise

    return {
        "inventory_count": len(inventory),
        "delta_result": delta_result,
        "sync_stats": sync_stats,
    }


def main() -> None:
    try:
        asyncio.run(run_pipeline())
    except KeyboardInterrupt:
        print("\nPipeline interrupted by user.")
        sys.exit(130)
    except Exception as error:
        print(f"\nPipeline failed: {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
