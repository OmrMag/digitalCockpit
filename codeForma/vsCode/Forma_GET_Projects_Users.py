import requests
import json
import csv
import os

from Forma_MasterScript import get_token
from Forma_GET_Account_Projects import get_projects
from Forma_Config import EXPORTS_DIR

BASE_URL = "https://developer.api.autodesk.com"


def get_users_for_project(project_id):
    token = get_token()
    url = f"{BASE_URL}/construction/admin/v1/projects/{project_id}/users"
    headers = {"Authorization": f"Bearer {token}"}
    all_users, offset, limit = [], 0, 200

    while True:
        response = requests.get(url, headers=headers, params={"limit": limit, "offset": offset})
        if response.status_code == 200:
            data  = response.json()
            batch = data.get("results", [])
            if not batch:
                break
            all_users.extend(batch)
            offset += len(batch)
            if len(batch) < limit:
                break
        else:
            print(f"  Error {response.status_code} for project {project_id}: {response.text}")
            break

    return all_users


if __name__ == "__main__":
    acc_projects    = get_projects(status="active", platform="acc")
    bim360_projects = get_projects(status="active", platform="bim360")
    all_projects    = acc_projects + bim360_projects

    print(f"Found {len(all_projects)} projects — fetching users...\n")

    all_rows = []
    for project in all_projects:
        project_id   = project.get("id")
        project_name = project.get("name")
        platform     = project.get("platform")

        print(f"  [{platform}] {project_name}")
        users = get_users_for_project(project_id)
        print(f"    → {len(users)} user(s)")

        for u in users:
            all_rows.append({
                "project_id":    project_id,
                "project_name":  project_name,
                "platform":      platform,
                "user_id":       u.get("id"),
                "name":          u.get("name"),
                "email":         u.get("email"),
                "status":        u.get("status"),
                "company_name":  u.get("companyName"),
                "access_levels": ", ".join(u.get("accessLevels", [])),
                "added_on":      u.get("addedOn"),
            })

    print(f"\nTotal rows: {len(all_rows)}")

    json_path = os.path.join(EXPORTS_DIR, "forma_projects_users.json")
    with open(json_path, "w") as f:
        json.dump(all_rows, f, indent=4)
    print(f"\nSaved {json_path}")

    csv_fields = ["project_id", "project_name", "platform", "user_id", "name", "email",
                  "status", "company_name", "access_levels", "added_on"]
    csv_path = os.path.join(EXPORTS_DIR, "forma_projects_users.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Saved {csv_path}")