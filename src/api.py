from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from agent.chroma_service import check_chroma_service
from agent.retriever import SimpleRetriever

app = FastAPI(title="Voice AI Retrieval API")


class QueryRequest(BaseModel):
    query: str
    top_k: int = 4


@app.get("/health")
def health() -> dict[str, object]:
    return check_chroma_service()


@app.post("/retrieve")
def retrieve_documents(payload: QueryRequest) -> dict[str, object]:
    if not payload.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    retriever = SimpleRetriever(top_k=payload.top_k)
    results = retriever.retrieve(payload.query)
    return {
        "query": payload.query,
        "results": results,
    }
