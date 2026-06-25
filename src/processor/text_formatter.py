import re
from typing import Any

# Maps scraped overview labels to stable Chroma metadata keys.
OVERVIEW_METADATA_MAP = {
    "TRANSMISSION": "transmission",
    "FUEL TYPE": "fuel_type",
    "ENGINE": "engine",
    "EXTERIOR": "exterior_color",
    "INTERIOR": "interior_color",
    "MILEAGE": "mileage",
}


def normalize_car_state(car: dict[str, Any]) -> dict[str, Any]:
    """Normalize a car record into a stable shape for text formatting and comparison."""
    overview = car.get("overview") or {}
    if not isinstance(overview, dict):
        overview = {}

    features = car.get("key_features") or []
    if not isinstance(features, list):
        features = []

    return {
        "id": str(car.get("id", "")).strip(),
        "name": str(car.get("name", "")).strip(),
        "price": int(car.get("price", 0) or 0),
        "url": str(car.get("url", "")).strip(),
        "overview": {str(k).strip(): str(v).strip() for k, v in sorted(overview.items())},
        "key_features": sorted(str(f).strip() for f in features if str(f).strip()),
    }


def _normalize_transmission(value: str) -> str:
    cleaned = value.strip()
    if cleaned.upper() == "AUTO":
        return "Automatic"
    return cleaned


def _parse_mileage_km(mileage: str) -> int | None:
    match = re.search(r"([\d,]+)", mileage)
    if not match:
        return None
    try:
        return int(match.group(1).replace(",", ""))
    except ValueError:
        return None


def extract_vehicle_facets(car: dict[str, Any]) -> dict[str, str | int]:
    """Extract filterable vehicle attributes for Chroma metadata / hybrid search."""
    normalized = normalize_car_state(car)
    overview = normalized["overview"]

    facets: dict[str, str | int] = {
        "car_id": normalized["id"],
        "name": normalized["name"],
        "price": normalized["price"],
    }

    if normalized["name"]:
        facets["model"] = normalized["name"].split()[0]

    for overview_key, metadata_key in OVERVIEW_METADATA_MAP.items():
        value = overview.get(overview_key, "").strip()
        if not value:
            continue

        if metadata_key == "transmission":
            value = _normalize_transmission(value)

        facets[metadata_key] = value

    mileage_km = _parse_mileage_km(str(facets.get("mileage", "")))
    if mileage_km is not None:
        facets["mileage_km"] = mileage_km

    return facets


def convert_car_to_text(car: dict[str, Any]) -> str:
    """Convert a car record into clean natural-language text for embedding."""
    normalized = normalize_car_state(car)
    overview = normalized["overview"]
    sentences: list[str] = []

    if normalized["name"]:
        sentences.append(f"{normalized['name']}.")

    if normalized["price"]:
        sentences.append(f"Price: {normalized['price']:,} SAR.")

    mileage = overview.get("MILEAGE")
    if mileage:
        sentences.append(f"Mileage: {mileage}.")

    fuel_type = overview.get("FUEL TYPE")
    if fuel_type:
        sentences.append(f"Fuel type: {fuel_type}.")

    transmission = overview.get("TRANSMISSION")
    if transmission:
        sentences.append(f"Transmission: {_normalize_transmission(transmission)}.")

    engine = overview.get("ENGINE")
    if engine:
        sentences.append(f"Engine: {engine}.")

    exterior = overview.get("EXTERIOR")
    if exterior:
        sentences.append(f"Exterior color: {exterior}.")

    interior = overview.get("INTERIOR")
    if interior:
        sentences.append(f"Interior: {interior}.")

    if normalized["key_features"]:
        sentences.append(f"Key features: {', '.join(normalized['key_features'])}.")

    return " ".join(sentences)


def convert_cars_to_text(cars: list[dict[str, Any]]) -> list[str]:
    """Convert multiple car records into embedding-ready text documents."""
    return [convert_car_to_text(car) for car in cars]
