from agent.chroma_service import check_chroma_service
from agent.retriever import SimpleRetriever


def main() -> None:
    print("Checking Chroma service...")
    status = check_chroma_service()
    print(status)

    if not status.get("ok"):
        print("Chroma service is unavailable; cannot run retrieval.")
        return

    retriever = SimpleRetriever(top_k=10)
    query = input("Enter a query: ").strip()
    if not query:
        print("No query provided.")
        return

    results = retriever.retrieve(query)
    print(f"\nFound {len(results)} result(s):")
    for idx, item in enumerate(results, start=1):
        print(f"\n[{idx}] {item['id']}")
        print(item["document"])
        print(f"Metadata: {item['metadata']}")
        print(f"Distance: {item['distance']}")


if __name__ == "__main__":
    main()
