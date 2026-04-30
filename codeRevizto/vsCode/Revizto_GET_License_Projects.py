import json
import csv
import os
import requests
from Revizto_Config import BASE_URL, REGION, EXPORTS_DIR
from Revizto_MasterScript import get_headers
from Revizto_GET_User_Licenses import get_user_licenses


def get_projects() -> list[dict]:
    """Get all projects across all user licenses."""
    licenses = get_user_licenses()
    if not licenses:
        print("[Revizto] No licenses found.")
        return []

    all_projects = []
    seen = set()
    for lic in licenses:
        lic_uuid = lic.get("uuid", "")
        lic_name = lic.get("name", lic_uuid)
        response = requests.get(
            f"{BASE_URL}/project/list/{lic_uuid}/paged",
            headers=get_headers(),
            timeout=10,
        )
        if response.status_code != 200:
            print(f"[Revizto] Error fetching projects for license '{lic_name}': {response.status_code}")
            continue
        try:
            projects = response.json()["data"]["data"]
        except (KeyError, TypeError):
            continue
        for p in projects:
            uuid = p.get("uuid", "")
            if uuid and uuid not in seen:
                seen.add(uuid)
                all_projects.append({
                    "id":     p.get("id", ""),
                    "uuid":   uuid,
                    "title":  p.get("title", ""),
                    "region": REGION,
                })
        print(f"[Revizto] License '{lic_name}' — {len(projects)} project(s)")

    return all_projects


def save_json(projects: list[dict], filename: str = "revizto_license_projects.json"):
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w") as f:
        json.dump(projects, f, indent=2)
    print(f"Saved {len(projects)} projects to {path}")


def save_csv(projects: list[dict], filename: str = "revizto_license_projects.csv"):
    if not projects:
        print("No projects to save.")
        return
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "uuid", "title", "region"])
        writer.writeheader()
        writer.writerows(projects)
    print(f"Saved {len(projects)} projects to {path}")


if __name__ == "__main__":
    projects = get_projects()
    if projects:
        print(json.dumps(projects, indent=4))
        save_json(projects)
        save_csv(projects)
    else:
        print("[Revizto] No projects found.")