import os
from pathlib import Path

# Base Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
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
