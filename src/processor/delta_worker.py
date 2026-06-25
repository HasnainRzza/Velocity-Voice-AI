import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any

from src.processor.text_formatter import convert_car_to_text, extract_vehicle_facets, normalize_car_state
from config.settings import STATE_FILE

def load_json(filepath: str) -> list[dict[str, Any]]:
    """Safely load JSON inventory files."""
    if not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else []

def save_json(filepath: str, payload: Any) -> None:
    """Persist JSON payloads to disk."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, ensure_ascii=False)

def generate_car_hash(car: dict[str, Any]) -> str:
    """Create a deterministic hash of the full normalized vehicle state."""
    normalized = normalize_car_state(car)
    state_string = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(state_string.encode("utf-8")).hexdigest()

def find_exact_changes(old_car: dict[str, Any], new_car: dict[str, Any]) -> dict[str, Any]:
    """Deep-compare normalized values and return a structured delta."""
    old_state = normalize_car_state(old_car)
    new_state = normalize_car_state(new_car)
    changes: dict[str, Any] = {}

    if old_state["name"] != new_state["name"]:
        changes["name"] = {"old": old_state["name"], "new": new_state["name"]}

    if old_state["price"] != new_state["price"]:
        changes["price"] = {"old": old_state["price"], "new": new_state["price"]}

    if old_state["url"] != new_state["url"]:
        changes["url"] = {"old": old_state["url"], "new": new_state["url"]}

    old_overview = old_state["overview"]
    new_overview = new_state["overview"]
    overview_changes: dict[str, Any] = {}

    for key in sorted(set(old_overview) | set(new_overview)):
        old_value = old_overview.get(key)
        new_value = new_overview.get(key)
        if old_value != new_value:
            overview_changes[key] = {"old": old_value, "new": new_value}

    if overview_changes:
        changes["overview"] = overview_changes

    old_features = set(old_state["key_features"])
    new_features = set(new_state["key_features"])
    added = sorted(new_features - old_features)
    removed = sorted(old_features - new_features)

    if added or removed:
        changes["features"] = {}
        if added:
            changes["features"]["added"] = added
        if removed:
            changes["features"]["removed"] = removed

    return changes

def compute_detailed_deltas(
    old_inventory: list[dict[str, Any]],
    new_inventory: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Compare two inventories and return additions, updates, and deletions."""
    old_map = {car["id"]: car for car in old_inventory if car.get("id")}
    new_map = {car["id"]: car for car in new_inventory if car.get("id")}

    additions: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []
    deletions: list[dict[str, Any]] = []

    for car_id, new_car in new_map.items():
        if car_id not in old_map:
            additions.append(new_car)
            continue

        old_car = old_map[car_id]
        if generate_car_hash(old_car) != generate_car_hash(new_car):
            updates.append({
                "car_data": new_car,
                "changes": find_exact_changes(old_car, new_car),
            })

    for old_id, old_car in old_map.items():
        if old_id not in new_map:
            deletions.append(old_car)

    return additions, updates, deletions

def build_chroma_metadata(
    car: dict[str, Any],
    action: str,
    changes: dict[str, Any] | None = None,
) -> dict[str, str | int]:
    """Build strictly isolated Chroma metadata payload."""
    facets = extract_vehicle_facets(car)

    metadata: dict[str, str | int] = {
        "price": int(facets.get("price", 0)),
        "name": str(facets.get("name", "")),
    }

    return metadata

def build_ingestion_item(
    car: dict[str, Any],
    action: str,
    changes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create one ingestion work item for the text + Chroma pipeline."""
    car_id = car.get("id")
    if not car_id:
        raise ValueError("Every car record must include a stable 'id' field.")

    return {
        "id": str(car_id),
        "text": convert_car_to_text(car),
        "metadata": build_chroma_metadata(car, action, changes),
        "action": action,
        "changes": changes or {},
    }

def build_ingestion_items(
    additions: list[dict[str, Any]],
    updates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build ingestion items only for new or changed vehicles."""
    items: list[dict[str, Any]] = []

    for car in additions:
        items.append(build_ingestion_item(car, action="add"))

    for update in updates:
        items.append(
            build_ingestion_item(
                update["car_data"],
                action="update",
                changes=update["changes"],
            )
        )

    return items

def process_scrape_result(
    new_inventory: list[dict[str, Any]],
    state_filepath: str = str(STATE_FILE),
) -> dict[str, Any]:
    """
    Compare freshly scraped inventory with the saved state ledger.
    """
    old_inventory = load_json(state_filepath)
    is_first_run = not old_inventory

    additions, updates, deletions = compute_detailed_deltas(old_inventory, new_inventory)

    if is_first_run:
        additions = list(new_inventory)
        updates = []
        deletions = []

    ingestion_items = build_ingestion_items(additions, updates)

    return {
        "is_first_run": is_first_run,
        "additions": additions,
        "updates": updates,
        "deletions": deletions,
        "ingestion_items": ingestion_items,
        "state_filepath": state_filepath,
        "new_inventory": new_inventory,
    }

def commit_inventory_state(state_filepath: str, new_inventory: list[dict[str, Any]]) -> None:
    """Persist the latest scraped inventory after a successful Chroma sync."""
    save_json(state_filepath, new_inventory)

def print_ingestion_report(result: dict[str, Any]) -> None:
    """Print a human-readable summary of scrape delta processing."""
    print("\n=== INVENTORY DELTA REPORT ===")
    print(f"Run type: {'FIRST RUN (full ingest)' if result['is_first_run'] else 'DELTA RUN (changed only)'}")

    print(f"\n[-] CARS REMOVED ({len(result['deletions'])})")
    for car in result["deletions"]:
        print(f"    -> {car.get('name', 'Unknown')} (ID: {car['id']})")

    print(f"\n[~] UPDATES DETECTED ({len(result['updates'])})")
    for update in result["updates"]:
        car_name = update["car_data"].get("name", "Unknown")
        changes = update["changes"]
        print(f"    -> {car_name} ({update['car_data']['id']}):")

        if "price" in changes:
            old_p = changes["price"]["old"]
            new_p = changes["price"]["new"]
            diff = new_p - old_p
            direction = "Dropped" if diff < 0 else "Increased"
            print(f"       - Price {direction}: {old_p} -> {new_p} (Diff: {abs(diff)})")

        if "overview" in changes:
            for field, delta in changes["overview"].items():
                print(f"       - {field}: {delta['old']} -> {delta['new']}")

        if "features" in changes:
            if changes["features"].get("added"):
                print(f"       - Features added: {len(changes['features']['added'])}")
            if changes["features"].get("removed"):
                print(f"       - Features removed: {len(changes['features']['removed'])}")

    print(f"\n[+] NEW INVENTORY ({len(result['additions'])})")
    for car in result["additions"][:5]:
        print(f"    -> {car.get('name', 'Unknown')} (SAR {car.get('price', 0)})")
    if len(result["additions"]) > 5:
        print(f"    ... and {len(result['additions']) - 5} more vehicles.")

    print(f"\n[>] CHROMA INGESTION ITEMS ({len(result['ingestion_items'])})")
    for item in result["ingestion_items"][:5]:
        print(f"    -> {item['action']} {item['metadata']['name']} (ID: {item['id']})")
    if len(result["ingestion_items"]) > 5:
        print(f"    ... and {len(result['ingestion_items']) - 5} more items.")
