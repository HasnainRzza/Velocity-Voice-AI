import asyncio

from playwright.async_api import Locator, Page, async_playwright
from src.scraper.detail_scraper import extract_details_concurrently
from config.settings import (
    INVENTORY_URL,
    SITE_BASE_URL,
    CARD_SELECTOR,
    NEXT_BUTTON_SELECTOR,
    MAX_PAGINATION_PAGES,
    PAGE_CHANGE_TIMEOUT_SEC,
    PAGINATION_RETRY_LIMIT,
)

async def _extract_card_record(card: Locator) -> dict | None:
    """Parse one inventory card into a base vehicle record."""
    try:
        link_element = card.locator("a.gi-vehicle-card__link")
        href = await link_element.get_attribute("href")
        if not href:
            return None

        car_url = href if href.startswith("http") else f"{SITE_BASE_URL}{href}"
        name = await card.locator("p.gi-vehicle-card__title").inner_text()
        price_text = await card.locator("span.gi-vehicle-card__price-value").inner_text()
        price = int(price_text.replace(",", "").strip()) if price_text else 0

        return {
            "id": car_url.split("/")[-1],
            "name": name.strip(),
            "price": price,
            "url": car_url,
        }
    except Exception as item_error:
        print(f"Skipping a card due to parsing error: {item_error}")
        return None

async def _first_card_id(page: Page) -> str | None:
    """Return the first visible card ID on the current inventory page."""
    cards = await page.locator(CARD_SELECTOR).all()
    if not cards:
        return None

    href = await cards[0].locator("a.gi-vehicle-card__link").get_attribute("href")
    if not href:
        return None
    return href.split("/")[-1]

async def _wait_for_page_change(
    page: Page,
    previous_first_id: str | None,
    next_button: Locator,
) -> bool:
    """Wait until pagination loads a new set of cards, with click retries."""
    if not previous_first_id:
        await asyncio.sleep(1)
        return True

    for attempt in range(PAGINATION_RETRY_LIMIT + 1):
        elapsed = 0.0
        while elapsed < PAGE_CHANGE_TIMEOUT_SEC:
            await asyncio.sleep(0.5)
            elapsed += 0.5
            current_first_id = await _first_card_id(page)
            if current_first_id and current_first_id != previous_first_id:
                return True

        if attempt < PAGINATION_RETRY_LIMIT and await next_button.count() > 0:
            if await next_button.is_disabled():
                break
            print(f"  Pagination still loading, retrying next click ({attempt + 2}/{PAGINATION_RETRY_LIMIT + 1})...")
            await next_button.click()
            await page.wait_for_load_state("domcontentloaded")
            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass

    return False

async def _scrape_inventory_list(list_page: Page) -> list[dict]:
    """Scrape all inventory list pages with deduplication and stable pagination."""
    await list_page.goto(INVENTORY_URL, wait_until="domcontentloaded")

    base_records: list[dict] = []
    seen_ids: set[str] = set()
    page_number = 1

    while page_number <= MAX_PAGINATION_PAGES:
        print(f"\n--- Scraping Page {page_number} ---")

        try:
            await list_page.wait_for_selector(CARD_SELECTOR, timeout=15000)
        except Exception:
            print("Timeout waiting for inventory cards. Ending pagination.")
            break

        cards = await list_page.locator(CARD_SELECTOR).all()
        print(f"Found {len(cards)} vehicles on this page.")

        new_on_page = 0
        for card in cards:
            record = await _extract_card_record(card)
            if not record or record["id"] in seen_ids:
                continue

            seen_ids.add(record["id"])
            base_records.append(record)
            new_on_page += 1

        print(f"Added {new_on_page} new vehicles ({len(base_records)} total unique).")

        if new_on_page == 0:
            print("No new vehicles found on this page. Ending pagination.")
            break

        next_button = list_page.locator(NEXT_BUTTON_SELECTOR)
        if await next_button.count() == 0:
            print("No 'Next' button found. Reached the last page.")
            break

        if await next_button.is_disabled():
            print("'Next' button is disabled. Reached the last page.")
            break

        first_id_before = await _first_card_id(list_page)
        print("Clicking 'Next Page'...")
        await next_button.click()
        await list_page.wait_for_load_state("domcontentloaded")
        try:
            await list_page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass

        if not await _wait_for_page_change(list_page, first_id_before, next_button):
            print("Pagination did not load new results. Ending pagination.")
            break

        page_number += 1

    return base_records

async def scrape_full_genesis_inventory() -> list[dict]:
    """Scrape Genesis CPO inventory list and detail pages. Returns complete car records."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        list_page = await context.new_page()
        print("Navigating to Genesis CPO Inventory...")
        base_records = await _scrape_inventory_list(list_page)
        await list_page.close()
        print(f"\nPhase 1 Complete! Collected {len(base_records)} unique vehicles across all pages.")

        await context.close()
        await browser.close()

    # ==========================================
    # PHASE 2: Scrape Detail Pages Concurrently
    # ==========================================
    print(f"\nStarting concurrent detail extraction for {len(base_records)} vehicles...")
    complete_inventory = await extract_details_concurrently(base_records, concurrency=5)

    return complete_inventory
