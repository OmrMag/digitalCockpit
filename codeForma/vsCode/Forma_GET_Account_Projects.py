import requests
import json
import csv
import os

from Forma_Config import ACCOUNT_ID, EXPORTS_DIR
from Forma_MasterScript import get_token


def get_projects(status=None, platform=None, limit=200):
    token = get_token()
    url = f"https://developer.api.autodesk.com/construction/admin/v1/accounts/{ACCOUNT_ID}/projects"
    headers = {"Authorization": f"Bearer {token}"}

    params = {"limit": limit}
    if status:
        params["filter[status]"] = status
    if platform:
        params["filter[platform]"] = platform

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json().get("results", [])
    else:
        print(f"[Forma] Error {response.status_code}: {response.text}")
        return []


if __name__ == "__main__":
    acc_projects    = get_projects(status="active", platform="acc")
    bim360_projects = get_projects(status="active", platform="bim360")
    projects = acc_projects + bim360_projects

    print(f"Found {len(projects)} project(s)  (ACC: {len(acc_projects)}, BIM360: {len(bim360_projects)})\n")

    fields = ["id", "name", "status", "platform", "createdAt"]
    col_widths = [38, 50, 10, 8, 26]

    header = "  ".join(f"{fields[i]:<{col_widths[i]}}" for i in range(len(fields)))
    print(header)
    print("-" * len(header))
    for p in projects:
        row = "  ".join(f"{str(p.get(fields[i], '') or ''):<{col_widths[i]}}" for i in range(len(fields)))
        print(row)

    json_path = os.path.join(EXPORTS_DIR, "forma_account_projects.json")
    with open(json_path, "w") as f:
        json.dump(projects, f, indent=4)
    print(f"\nSaved {json_path}")

    csv_path = os.path.join(EXPORTS_DIR, "forma_account_projects.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(projects)
    print(f"Saved {csv_path}")