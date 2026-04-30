import requests
import json
import csv
import os

from Forma_MasterScript import get_token
from Forma_Config import ACCOUNT_ID, PROJECT_ID, EXPORTS_DIR

BASE_URL         = "https://developer.api.autodesk.com"
MODEL_EXTENSIONS = {"rvt", "ifc", "nwd", "nwc", "dwg", "dgn", "fbx", "obj", "skp"}


def get_top_folders(project_id):
    token = get_token()
    url = f"{BASE_URL}/project/v1/hubs/b.{ACCOUNT_ID}/projects/b.{project_id}/topFolders"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get("data", [])
    print(f"[Forma] Error: {response.status_code} {response.text}")
    return []


def get_folder_contents(project_id, folder_id):
    token = get_token()
    url = f"{BASE_URL}/data/v1/projects/b.{project_id}/folders/{folder_id}/contents"
    headers = {"Authorization": f"Bearer {token}"}
    all_items, page = [], 0
    while True:
        response = requests.get(url, headers=headers, params={"page[number]": page, "page[limit]": 200})
        if response.status_code == 200:
            data  = response.json()
            batch = data.get("data", [])
            all_items.extend(batch)
            if not batch or not data.get("links", {}).get("next"):
                break
            page += 1
        else:
            break
    return all_items


def get_version_urn(project_id, item_id):
    token = get_token()
    url = f"{BASE_URL}/data/v1/projects/b.{project_id}/items/{item_id}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return (response.json()
                        .get("data", {})
                        .get("relationships", {})
                        .get("tip", {})
                        .get("data", {})
                        .get("id"))
    return None


def traverse_folder(project_id, folder_id, folder_path, all_models):
    for item in get_folder_contents(project_id, folder_id):
        attrs = item.get("attributes", {})
        name  = attrs.get("displayName") or attrs.get("name", "")

        if item.get("type") == "folders":
            traverse_folder(project_id, item.get("id"), f"{folder_path}/{name}", all_models)

        elif item.get("type") == "items":
            ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
            if ext in MODEL_EXTENSIONS:
                item_id     = item.get("id")
                version_urn = get_version_urn(project_id, item_id)
                parts       = folder_path.split("/")
                all_models.append({
                    "folder_parent":     parts[1] if len(parts) > 1 else "",
                    "folder_main":       parts[2] if len(parts) > 2 else "",
                    "folder_sub":        parts[3] if len(parts) > 3 else "",
                    "extension":         ext,
                    "MODEL_NAME":        name.rsplit(".", 1)[0] if "." in name else name,
                    "MODEL_ITEM_ID":     item_id,
                    "MODEL_VERSION_URN": version_urn or "",
                })
                print(f"  Found: {name}")


if __name__ == "__main__":
    if not PROJECT_ID:
        print("[Forma] No active project set. Select a Forma/ACC project in the sidebar first.")
        exit(1)

    print(f"Scanning project {PROJECT_ID} for models...\n")

    all_models = []
    for folder in get_top_folders(PROJECT_ID):
        traverse_folder(PROJECT_ID, folder.get("id"), folder.get("attributes", {}).get("displayName", ""), all_models)

    print(f"\nFound {len(all_models)} model(s)\n")

    csv_fields = ["folder_parent", "folder_main", "folder_sub", "extension",
                  "MODEL_NAME", "MODEL_ITEM_ID", "MODEL_VERSION_URN"]

    csv_path = os.path.join(EXPORTS_DIR, "forma_project_models.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()
        writer.writerows(all_models)
    print(f"Saved {csv_path}")

    json_path = os.path.join(EXPORTS_DIR, "forma_project_models.json")
    with open(json_path, "w") as f:
        json.dump(all_models, f, indent=4)
    print(f"Saved {json_path}")