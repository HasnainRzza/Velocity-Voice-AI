import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
import chromadb


def configure_chroma_env() -> None:
    """Load environment variables from the voice-ai .env file and align them with Chroma's expected names."""
    env_path = Path(__file__).resolve().parent / ".env"
    load_dotenv(env_path, override=False)

    mappings = {
        "CHROMA_API_KEY": ("CHROMADB_API_KEY", "CHROMA_API_KEY"),
        "CHROMA_TENANT": ("CHROMA_DB_TENANT", "CHROMA_TENANT"),
        "CHROMA_DATABASE": ("CHROMA_DB_DATABASE", "CHROMA_DATABASE"),
    }

    for target, sources in mappings.items():
        if os.getenv(target):
            continue
        for source in sources:
            value = os.getenv(source)
            if value:
                os.environ[target] = value
                break


def get_chroma_client() -> chromadb.CloudClient:
    """Create a Chroma Cloud client using the environment configuration."""
    configure_chroma_env()
    return chromadb.CloudClient(
        tenant=os.getenv("CHROMA_TENANT"),
        database=os.getenv("CHROMA_DATABASE"),
        api_key=os.getenv("CHROMA_API_KEY"),
    )


def get_collection(collection_name: str | None = None) -> Any:
    """Return a Chroma collection, creating it if it does not exist yet."""
    client = get_chroma_client()
    name = collection_name or os.getenv("CHROMA_COLLECTION", "genesis_inventory")
    return client.get_or_create_collection(
        name=name,
        metadata={"source": "voice_ai_retriever"},
    )


def check_chroma_service(collection_name: str | None = None) -> dict[str, Any]:
    """Verify that the Chroma Cloud service is reachable and the collection is usable."""
    try:
        collection = get_collection(collection_name)
        count = collection.count()
        return {
            "ok": True,
            "collection": collection.name,
            "count": count,
        }
    except Exception as exc:  # pragma: no cover - defensive path
        return {
            "ok": False,
            "error": str(exc),
        }
