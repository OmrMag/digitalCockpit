import json
import csv
import os
import requests
from Morta_Config import API_KEY, HUB_ID, EXPORTS_DIR

BASE_URL = "https://api.morta.io"
HEADERS  = {"Authorization": f"Bearer {API_KEY}"}


def get_hub_tags() -> list[dict]:
    response = requests.get(f"{BASE_URL}/v1/hub/{HUB_ID}/tags", headers=HEADERS, timeout=10)
    print(f"[Morta] Status: {response.status_code}")
    if response.status_code != 200:
        return []
    data = response.json().get("data", [])
    return data if isinstance(data, list) else data.get("tags", data.get("items", []))


def extract_tags(tags: list[dict]) -> list[dict]:
    """Extract tag name and full id as tag_reference_id."""
    result = []
    for tag_group in tags:
        for cell in tag_group.get("cells", []):
            if "value" in cell and "id" in cell:
                result.append({
                    "name":             cell["value"],
                    "tag_reference_id": cell["id"],
                })
    return result


def save_json(tags: list[dict], filename: str = "morta_tags.json"):
    """Save as a list (consistent with CSV and re-importable)."""
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w") as f:
        json.dump(tags, f, indent=2)
    print(f"Saved {len(tags)} tags to {path}")


def save_csv(tags: list[dict], filename: str = "morta_tags.csv"):
    if not tags:
        print("No tags to save.")
        return
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "tag_reference_id"])
        writer.writeheader()
        writer.writerows(tags)
    print(f"Saved {len(tags)} tags to {path}")


if __name__ == "__main__":
    if not HUB_ID:
        print("[Morta] No active hub set. Select a Morta hub in the sidebar first.")
        exit(1)

    raw_tags = get_hub_tags()
    if raw_tags:
        tags = extract_tags(raw_tags)
        save_json(tags)
        save_csv(tags)
    else:
        print("[Morta] No tags returned from Morta API.")