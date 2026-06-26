import os
from typing import Any

from .chroma_service import get_collection


class SimpleRetriever:
    """A thin wrapper around Chroma collections for semantic document retrieval."""

    def __init__(self, collection_name: str | None = None, top_k: int = 4) -> None:
        self.collection_name = collection_name or os.getenv("CHROMA_COLLECTION", "genesis_inventory")
        self.top_k = top_k
        self.collection = get_collection(self.collection_name)

    def retrieve(self, query: str, price_max: float | None = None, car_name: str | None = None) -> list[dict[str, Any]]:
        """Retrieve the most relevant documents for a user query using server-side embeddings and metadata filtering."""
        if not query.strip() and price_max is None and car_name is None:
            return []

        conditions = []
        if price_max is not None:
            conditions.append({"price": {"$lte": price_max}})
        
        where = None
        if len(conditions) == 1:
            where = conditions[0]
        elif len(conditions) > 1:
            where = {"$and": conditions}

        # Chroma requires at least one query text. We combine query and car_name for semantic matching.
        query_parts = []
        if query.strip():
            query_parts.append(query.strip())
        if car_name and car_name.strip():
            query_parts.append(car_name.strip())
            
        query_text = " ".join(query_parts) if query_parts else "car"

        kwargs = {
            "query_texts": [query_text],
            "n_results": self.top_k,
        }
        if where:
            kwargs["where"] = where

        print(f"\n[Semantic Search] Querying: '{query_text}' | Filters: {where}")
        results = self.collection.query(**kwargs)

        documents = results.get("documents", [[]])[0]
        metadatas = results.get("metadatas", [[]])[0]
        distances = results.get("distances", [[]])[0]
        ids = results.get("ids", [[]])[0]

        retrieved: list[dict[str, Any]] = []
        for doc_id, document, metadata, distance in zip(ids, documents, metadatas, distances, strict=False):
            retrieved.append(
                {
                    "id": doc_id,
                    "document": document,
                    "metadata": metadata or {},
                    "distance": distance,
                }
            )

        print(f"[Semantic Search] Retrieved {len(retrieved)} results:")
        for r in retrieved:
            print(f"  - [{r['distance']:.3f}] {r['metadata'].get('name', 'Unknown')}: {r['document'][:60]}...")

        return retrieved
