import json
import csv
import os
import requests
from Revizto_Config import BASE_URL, EXPORTS_DIR
from Revizto_MasterScript import get_headers
from Revizto_GET_User_Licenses import get_user_licenses


def get_license_uuid() -> str:
    """Resolve primary license UUID from API."""
    licenses = get_user_licenses()
    return licenses[0]["uuid"] if licenses else ""


def get_project_roles(license_uuid: str = None) -> list[dict]:
    """Get all project roles in the license."""
    if not license_uuid:
        license_uuid = get_license_uuid()
    if not license_uuid:
        print("[Revizto] No license UUID available.")
        return []
    response = requests.get(
        f"{BASE_URL}/license/{license_uuid}/role/list",
        headers=get_headers(),
        timeout=10,
    )
    if response.status_code != 200:
        print(f"[Revizto] Error fetching roles: {response.status_code}")
        return []
    try:
        return [
            {"id": r["id"], "uuid": r["uuid"], "name": r["name"]}
            for r in response.json()["data"].values()
        ]
    except (KeyError, TypeError) as e:
        print(f"[Revizto] Unexpected response structure: {e}")
        return []


def save_json(roles: list[dict], filename: str = "revizto_project_roles.json"):
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w") as f:
        json.dump(roles, f, indent=2)
    print(f"Saved {len(roles)} roles to {path}")


def save_csv(roles: list[dict], filename: str = "revizto_project_roles.csv"):
    if not roles:
        print("No roles to save.")
        return
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "uuid", "name"])
        writer.writeheader()
        writer.writerows(roles)
    print(f"Saved {len(roles)} roles to {path}")


if __name__ == "__main__":
    roles = get_project_roles()
    if roles:
        print(json.dumps(roles, indent=4))
        save_json(roles)
        save_csv(roles)
    else:
        print("[Revizto] No roles found.")