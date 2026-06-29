# Genesis Inventory Ingestion Pipeline

This project contains the data ingestion pipeline for the Genesis CPO Inventory. It consists of scraping vehicles, comparing them against the previous state (Change Data Capture), and storing the extracted data efficiently in a ChromaDB vector database.

## Dependencies

Before running the ingestion pipeline, ensure the following dependencies are running:

### Redis
Redis is used for caching and event streaming. To start a local instance using Docker, run:
```bash
docker run -d -p 6379:6379 redis
```

### ChromaDB
ChromaDB is used as the vector database. Ensure your ChromaDB server is accessible.

## Architecture

1. **Scraping**: `src/scraper/` extracts lists of vehicles and performs highly concurrent parsing of detail pages using Playwright.
2. **Processing**: `src/processor/` formats the parsed HTML into semantic text for embedding and compares hashes of the vehicles to only process additions or updates, skipping unmodified vehicles to save costs.
3. **Storage**: `src/storage/` interfaces with ChromaDB to securely batch-upsert semantic vehicle profiles.

## Modules and Components

The pipeline is split into several focused modules to handle the lifecycle of data ingestion:

### 1. Scraper (`src/scraper/`)
- **`inventory_scraper.py`**: The high-level orchestrator for scraping. It navigates the main inventory site, handles pagination, and collects the list of all available vehicles.
- **`detail_scraper.py`**: Takes the list of vehicles and concurrently visits their detail pages to extract deep metadata (features, packages, specs) using Playwright.

### 2. Processor (`src/processor/`)
- **`delta_worker.py` (Change Data Capture)**: This is the core logic that saves computing resources. It compares the newly scraped inventory against a local JSON state file to identify exactly what has been added, modified, or deleted. It ensures downstream systems only process the "deltas".
- **`text_formatter.py`**: Converts the structured metadata of a vehicle into a rich, semantic text block. This text is heavily optimized so that the LLM/RAG pipeline can retrieve relevant vehicles accurately.

### 3. Events (`src/events/`)
- **`publisher.py` & `schema.py`**: When the `delta_worker` identifies changes, this module publishes standardized events (e.g., `VEHICLE_ADDED`, `VEHICLE_UPDATED`, `VEHICLE_REMOVED`) to a Redis Stream, allowing decoupled services (like the cache sync system) to react in real-time.

### 4. Storage (`src/storage/`)
- **`vector_store.py`**: Interfaces directly with ChromaDB. It takes the semantic text from the `text_formatter`, generates embeddings, and performs batch upserts or deletions based on the delta worker's output.

### 5. Cache Sync (`cache_sync/`)
- An independent module that listens to the Redis event stream and maintains a lightning-fast Redis metadata cache for the voice agent, bypassing ChromaDB for simple lookups.

### Entrypoint
- **`main.py`**: The script that wires all these modules together into a single, cohesive execution pipeline.

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
   REDIS_URL=redis://localhost:6379/0
   EVENTS_STREAM_KEY=genesis:inventory:events
   EVENTS_MAX_LEN=10000
   EVENTS_PUBLISH_MAX_RETRIES=3
   EVENTS_PUBLISH_RETRY_DELAY_SEC=1.0

   ```

3. Run the pipeline:
   ```bash
   python main.py
   ```
