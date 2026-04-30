import json
import csv
import os
import requests
from Revizto_Config import BASE_URL, EXPORTS_DIR
from Revizto_MasterScript import get_headers


def get_user_licenses() -> list[dict]:
    """Get all licenses available to the current user."""
    response = requests.get(
        f"{BASE_URL}/user/licenses",
        headers=get_headers(),
        timeout=10,
    )
    if response.status_code != 200:
        print(f"[Revizto] Error fetching licenses: {response.status_code}")
        return []
    try:
        return [
            {"id": l["id"], "uuid": l["uuid"], "name": l["name"]}
            for l in response.json()["data"]["entities"]
        ]
    except (KeyError, TypeError) as e:
        print(f"[Revizto] Unexpected response structure: {e}")
        return []


def save_json(licenses: list[dict], filename: str = "revizto_user_licenses.json"):
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w") as f:
        json.dump(licenses, f, indent=2)
    print(f"Saved {len(licenses)} licenses to {path}")


def save_csv(licenses: list[dict], filename: str = "revizto_user_licenses.csv"):
    if not licenses:
        print("No licenses to save.")
        return
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "uuid", "name"])
        writer.writeheader()
        writer.writerows(licenses)
    print(f"Saved {len(licenses)} licenses to {path}")


if __name__ == "__main__":
    licenses = get_user_licenses()
    if licenses:
        print(json.dumps(licenses, indent=4))
        save_json(licenses)
        save_csv(licenses)
    else:
        print("[Revizto] No licenses found.")