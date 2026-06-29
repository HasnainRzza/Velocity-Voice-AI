import os
from pathlib import Path
from dotenv import load_dotenv

# Base Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Load .env file explicitly using the project root
load_dotenv(PROJECT_ROOT / ".env")

DATA_DIR = PROJECT_ROOT / "data"
STATE_FILE = DATA_DIR / "genesis_inventory.json"
CURR_STATE_FILE = DATA_DIR / "genesis_curr_inventory.json"

# Ensure data dir exists
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Scraping Settings
INVENTORY_URL = "https://genesis-cpo.netlify.app/en/inventory"
SITE_BASE_URL = "https://genesis-cpo.netlify.app"
CARD_SELECTOR = "article.gi-vehicle-card"
NEXT_BUTTON_SELECTOR = 'button.gi-pager__arrow[aria-label="Next page"]'
MAX_PAGINATION_PAGES = 20
PAGE_CHANGE_TIMEOUT_SEC = 15
PAGINATION_RETRY_LIMIT = 2

# Processing & Chroma Settings
BATCH_SIZE = 100
DEFAULT_COLLECTION = "genesis_inventory"

# ── Redis & Event Stream Settings ──────────────────────────────────────────────
# Redis connection URL (shared by the scraper publisher and the Cache Sync Service)
REDIS_URL: str = os.environ["REDIS_URL"]

# Stream key written to by the scraper and consumed by the Cache Sync Service
EVENTS_STREAM_KEY: str = os.environ["EVENTS_STREAM_KEY"]

# Approximate cap on stream length — prevents unbounded Redis memory growth
EVENTS_MAX_LEN: int = int(os.environ["EVENTS_MAX_LEN"])

# Publisher retry settings (transient connection errors only)
EVENTS_PUBLISH_MAX_RETRIES: int = int(os.environ["EVENTS_PUBLISH_MAX_RETRIES"])
EVENTS_PUBLISH_RETRY_DELAY_SEC: float = float(os.environ["EVENTS_PUBLISH_RETRY_DELAY_SEC"])
