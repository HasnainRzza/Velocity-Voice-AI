import os
import asyncio
import json
from typing import Any

from core.cache import redis_pool
from .chroma_service import get_collection

# ──────────────────────────────────────────────────────────────────────────────
# ChromaDB metadata schema (confirmed via inspection):
#   "name"  – full car name string, e.g. "GV80 3.5T ROYAL", "GV80 COUPE 3.5T ROYAL"
#   "price" – integer price in SAR, e.g. 290000
#
# TRUE Hybrid search strategy (both layers always work together):
#
#   Step 1 – ChromaDB (semantic layer):
#     • Runs a vector similarity search with the query text.
#     • Applies a native price $lte filter inside the where clause.
#     • Over-fetches (top_k * OVERFETCH) to allow post-filtering.
#     • Post-filters results by partial car_name match on metadata["name"]
#       (ChromaDB does not support $contains on strings natively).
#     → Produces semantically ranked candidates with distance scores.
#
#   Step 2 – Redis (metadata layer, optional enrichment):
#     • Looks up each ChromaDB result's ID in the Redis cache.
#     • If a cached record exists, it replaces the ChromaDB result so we get
#       the freshest stored snapshot (Redis is the source of truth for warmup data).
#     • Any ChromaDB results not in Redis are kept as-is.
#     → Enriches results with cached metadata without losing semantic ordering.
#
#   Final output: results ranked by ChromaDB distance (semantic relevance),
#                 data sourced from Redis where available.
#
# Redis-only fast path (pure metadata queries):
#   • If query is empty AND only metadata filters are given (car_name / price),
#     Redis alone can answer the question instantly without a vector search.
# ──────────────────────────────────────────────────────────────────────────────

_OVERFETCH = 5  # multiplier for Chroma fetch count before name post-filtering


def _name_matches(metadata_name: str, partial: str) -> bool:
    """Return True if *partial* appears anywhere inside the stored car name (case-insensitive)."""
    return partial.strip().lower() in metadata_name.lower()


class SimpleRetriever:
    """ChromaDB + Redis hybrid retriever.

    Always uses ChromaDB for semantic ranking. Redis is used to:
    - Enrich / replace ChromaDB results with cached car records (same ID).
    - Serve as a fast-path when there is no free-text query (pure metadata lookup).
    """

    def __init__(self, collection_name: str | None = None, top_k: int = 4) -> None:
        self.collection_name = collection_name or os.getenv("CHROMA_COLLECTION", "genesis_inventory")
        self.top_k = top_k
        self.collection = get_collection(self.collection_name)

    # ── Public entry point ────────────────────────────────────────────────────

    async def retrieve(
        self,
        query: str,
        price_max: float | None = None,
        car_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """Hybrid retrieval: ChromaDB semantic search + Redis metadata enrichment.

        Args:
            query:     Free-text semantic search query.
            price_max: Optional upper price bound (inclusive, SAR).
            car_name:  Optional partial/full car name to filter by (case-insensitive).
        """
        if not query.strip() and price_max is None and car_name is None:
            return []

        # ── Fast path: pure metadata query (no free-text) ────────────────────
        # When there is no semantic query, Redis can answer instantly from its
        # already-filtered in-memory scan. ChromaDB vector search is skipped.
        if not query.strip():
            try:
                results = await self._redis_metadata_search(car_name, price_max)
                if results:
                    print(f"[Redis Fast-Path] Returning {len(results)} result(s) (no semantic query).")
                    return results[: self.top_k]
            except Exception as redis_err:
                print(f"[Redis Fast-Path] Unavailable ({redis_err.__class__.__name__}), falling back to ChromaDB.")

        # ── Hybrid path: ChromaDB semantic search + Redis enrichment ─────────
        # Step 1: semantic search in ChromaDB (always runs when query is given)
        chroma_results = await asyncio.to_thread(
            self._chroma_semantic_retrieve, query, price_max, car_name
        )

        if not chroma_results:
            return []

        # Step 2: enrich with Redis cache (replace records where cached data exists)
        try:
            enriched = await self._redis_enrich(chroma_results)
            print(f"[Hybrid] ChromaDB semantic: {len(chroma_results)} | After Redis enrichment: {len(enriched)}")
            return enriched
        except Exception as redis_err:
            print(f"[Hybrid] Redis enrichment skipped ({redis_err.__class__.__name__}), using ChromaDB results as-is.")
            return chroma_results

    # ── Redis helpers ─────────────────────────────────────────────────────────

    async def _redis_metadata_search(
        self, car_name: str | None, price_max: float | None
    ) -> list[dict[str, Any]]:
        """Scan Redis cache and return cars matching name (partial) and/or price filter.

        Soft-deleted cars (available == null, set by the Cache Sync Service) are
        automatically excluded from results so they never surface to the Voice AI.
        """
        keys = await redis_pool.keys("car:*")
        results = []
        for key in keys:
            data_str = await redis_pool.get(key)
            if not data_str:
                continue
            car = json.loads(data_str)

            # Skip soft-deleted vehicles (Cache Sync Service sets available=null on delete)
            if car.get("available") is None:
                continue

            meta = car.get("metadata", {})

            # Skip cars where the metadata availability is explicitly null
            if meta.get("availability") is None:
                continue

            # Apply car_name partial filter
            if car_name and car_name.strip():
                if not _name_matches(meta.get("name", ""), car_name):
                    continue

            # Apply price filter
            if price_max is not None:
                if meta.get("price", float("inf")) > price_max:
                    continue

            results.append(car)

        # Sort by price ascending when filtering by budget
        if price_max is not None and not car_name:
            results.sort(key=lambda x: x.get("metadata", {}).get("price", float("inf")))

        return results

    async def _redis_enrich(self, chroma_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Replace ChromaDB results with their cached Redis version (if available).

        Preserves the semantic distance ranking from ChromaDB. Only the stored
        document/metadata payload is swapped from Redis when a matching car:ID key exists.
        Results not found in Redis are returned unchanged.
        """
        enriched = []
        for result in chroma_results:
            car_id = result["id"]
            cached_str = await redis_pool.get(f"car:{car_id}")
            if cached_str:
                cached = json.loads(cached_str)
                # Keep ChromaDB's distance score but use Redis's data payload
                enriched.append({
                    "id": car_id,
                    "document": cached.get("document", result["document"]),
                    "metadata": cached.get("metadata", result["metadata"]),
                    "distance": result["distance"],  # semantic score preserved
                    "source": "redis+chroma",
                })
            else:
                result["source"] = "chroma"
                enriched.append(result)
        return enriched

    # ── ChromaDB semantic retrieval ───────────────────────────────────────────

    def _chroma_semantic_retrieve(
        self,
        query: str,
        price_max: float | None,
        car_name: str | None,
    ) -> list[dict[str, Any]]:
        """Synchronous ChromaDB vector search with price filter and partial name post-filter.

        - Combines query + car_name into the embedding query for better recall.
        - Applies price as a native Chroma where clause ($lte).
        - Over-fetches by _OVERFETCH then post-filters on partial name match.
        """
        # Build embedding query text
        query_parts: list[str] = []
        if query.strip():
            query_parts.append(query.strip())
        if car_name and car_name.strip():
            query_parts.append(car_name.strip())
        query_text = " ".join(query_parts) if query_parts else "car"

        # Price filter (natively supported by ChromaDB where clause)
        where: dict | None = None
        if price_max is not None:
            where = {"price": {"$lte": int(price_max)}}

        # Over-fetch to allow name post-filtering without losing top_k results
        fetch_n = self.top_k * _OVERFETCH if car_name else self.top_k

        kwargs: dict[str, Any] = {
            "query_texts": [query_text],
            "n_results": fetch_n,
        }
        if where:
            kwargs["where"] = where

        print(f"\n[ChromaDB] Semantic query='{query_text}' | price_max={price_max} | fetch_n={fetch_n}")
        results = self.collection.query(**kwargs)

        documents = results.get("documents", [[]])[0]
        metadatas = results.get("metadatas", [[]])[0]
        distances = results.get("distances", [[]])[0]
        ids = results.get("ids", [[]])[0]

        retrieved: list[dict[str, Any]] = []
        for doc_id, document, metadata, distance in zip(ids, documents, metadatas, distances, strict=False):
            retrieved.append({
                "id": doc_id,
                "document": document,
                "metadata": metadata or {},
                "distance": distance,
            })

        # Post-filter: partial car_name match on metadata["name"]
        if car_name and car_name.strip():
            before = len(retrieved)
            retrieved = [
                r for r in retrieved
                if _name_matches(r["metadata"].get("name", ""), car_name)
            ]
            print(f"[ChromaDB] Name post-filter '{car_name}': {before} -> {len(retrieved)} result(s)")

        return retrieved[: self.top_k]
