# Genesis Inventory Ingestion Pipeline

This project contains the data ingestion pipeline for the Genesis CPO Inventory. It consists of scraping vehicles, comparing them against the previous state (Change Data Capture), and storing the extracted data efficiently in a ChromaDB vector database.

## Architecture

1. **Scraping**: `src/scraper/` extracts lists of vehicles and performs highly concurrent parsing of detail pages using Playwright.
2. **Processing**: `src/processor/` formats the parsed HTML into semantic text for embedding and compares hashes of the vehicles to only process additions or updates, skipping unmodified vehicles to save costs.
3. **Storage**: `src/storage/` interfaces with ChromaDB to securely batch-upsert semantic vehicle profiles.

## Folder Structure

- `data/`: Contains local JSON state files representing the inventory.
- `src/config/`: Centralized settings.
- `src/processor/`: CDC delta workers and text formatting logic.
- `src/scraper/`: Asynchronous playwright scraping logic.
- `src/storage/`: Database connection logic.
- `main.py`: The entrypoint to run the entire pipeline.

## Setup and Running

1. Install requirements:
   ```bash
   pip install -r requirements.txt
   playwright install
   ```

2. Configure environment variables. Create a `.env` file in the root of the `ingestion/` directory with your ChromaDB credentials:
   ```env
   CHROMA_TENANT=your-tenant
   CHROMA_DATABASE=your-database
   CHROMA_API_KEY=your-api-key
   ```

3. Run the pipeline:
   ```bash
   python main.py
   ```
