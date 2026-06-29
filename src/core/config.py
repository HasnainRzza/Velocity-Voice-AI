import os
from pathlib import Path
from dotenv import load_dotenv

# Base directory is the project root (2 levels up from src/core/config.py)
BASE_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = BASE_DIR / ".env"

# Load the environment variables
load_dotenv(ENV_PATH)

class Settings:
    DEEPGRAM_STT: str = os.getenv("DEEPGRAM_STT", "")
    DEEPGRAM_TTS: str = os.getenv("DEEPGRAM_TTS", "")
    CHROMADB_API_KEY: str = os.getenv("CHROMADB_API_KEY", "")
    CHROMA_DB_TENANT: str = os.getenv("CHROMA_DB_TENANT", "")
    CHROMA_DB_DATABASE: str = os.getenv("CHROMA_DB_DATABASE", "")
    GROQ_API_KEY: str = os.getenv("GROQ_API_KEY", "")
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

settings = Settings()
