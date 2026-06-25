import asyncio
from playwright.async_api import async_playwright, BrowserContext

async def _detail_worker(queue: asyncio.Queue, results: list, failed_queue: asyncio.Queue, context: BrowserContext):
    page = await context.new_page()
    while True:
        try:
            record, is_retry = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        try:
            await page.goto(record["url"], wait_until="domcontentloaded", timeout=15000)
            
            car_full_data = record.copy()
            car_full_data["overview"] = {}
            car_full_data["key_features"] = []
            car_full_data["status"] = "available"

            try:
                await page.wait_for_selector(".gvd-spec-grid", timeout=3000)
                spec_items = await page.locator(".gvd-spec-item").all()
                for item in spec_items:
                    label = await item.locator(".gvd-spec-item__label").inner_text()
                    value = await item.locator(".gvd-spec-item__value").inner_text()
                    car_full_data["overview"][label.strip()] = value.strip()
            except Exception:
                pass

            try:
                await page.wait_for_selector(".gvd-features__grid", timeout=3000)
                feature_elements = await page.locator(".gvd-features__text").all()
                for feature in feature_elements:
                    text = await feature.inner_text()
                    car_full_data["key_features"].append(text.strip())
            except Exception:
                pass

            results.append(car_full_data)
        except Exception as error:
            print(f"Failed to load details for {record['url']}. Error: {error}")
            if not is_retry:
                # Add to retry queue on first failure
                failed_queue.put_nowait(record)
            else:
                # Flag as sold on second failure, prevent deletion
                failed_record = record.copy()
                failed_record["overview"] = {}
                failed_record["key_features"] = []
                failed_record["status"] = "sold"
                results.append(failed_record)
        finally:
            queue.task_done()
    
    await page.close()

async def extract_details_concurrently(base_records: list[dict], concurrency: int = 5) -> list[dict]:
    results = []
    queue = asyncio.Queue()
    failed_queue = asyncio.Queue()

    for record in base_records:
        queue.put_nowait((record, False))

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        # Pass 1: Standard extraction
        workers = [asyncio.create_task(_detail_worker(queue, results, failed_queue, context)) for _ in range(concurrency)]
        await asyncio.gather(*workers)

        # Pass 2: Retry failed URLs
        if not failed_queue.empty():
            print(f"Retrying {failed_queue.qsize()} failed URLs...")
            while not failed_queue.empty():
                failed_record = failed_queue.get_nowait()
                queue.put_nowait((failed_record, True))
            
            retry_workers = [asyncio.create_task(_detail_worker(queue, results, failed_queue, context)) for _ in range(concurrency)]
            await asyncio.gather(*retry_workers)

        await context.close()
        await browser.close()

    return results
