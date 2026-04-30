import requests
import json
import csv
import os

from Forma_Config import ACCOUNT_ID, PROJECT_ID, EXPORTS_DIR
from Forma_MasterScript import get_token


def get_top_folders(project_id):
    token = get_token()
    url = f"https://developer.api.autodesk.com/project/v1/hubs/b.{ACCOUNT_ID}/projects/b.{project_id}/topFolders"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get("data", [])
    print(f"[Forma] Error getting top folders: {response.status_code} {response.text}")
    return []


def get_folder_contents(project_id, folder_id):
    token = get_token()
    url = f"https://developer.api.autodesk.com/data/v1/projects/b.{project_id}/folders/{folder_id}/contents"
    headers = {"Authorization": f"Bearer {token}"}
    all_items, page = [], 0
    while True:
        params = {"page[number]": page, "page[limit]": 200}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            batch = data.get("data", [])
            if not batch:
                break
            all_items.extend(batch)
            if not data.get("links", {}).get("next"):
                break
            page += 1
        else:
            print(f"  [Forma] Error {response.status_code} on folder {folder_id}: {response.text}")
            break
    return all_items


def traverse_folder(project_id, folder_id, folder_path, all_rows):
    contents = get_folder_contents(project_id, folder_id)
    for item in contents:
        item_type = item.get("type")
        attrs = item.get("attributes", {})
        name = attrs.get("displayName") or attrs.get("name", "")

        if item_type == "folders":
            sub_path = f"{folder_path}/{name}"
            print(f"  Folder: {sub_path}")
            traverse_folder(project_id, item.get("id"), sub_path, all_rows)

        elif item_type == "items":
            parts = folder_path.split("/")
            folder_parent = parts[1] if len(parts) > 1 else ""
            folder_main   = parts[2] if len(parts) > 2 else ""
            folder_sub    = parts[3] if len(parts) > 3 else ""
            extension = name.rsplit(".", 1)[-1].lower() if "." in name else ""
            name = name.rsplit(".", 1)[0] if "." in name else name
            all_rows.append({
                "folder_parent":    folder_parent,
                "folder_main":      folder_main,
                "folder_sub":       folder_sub,
                "name":             name,
                "extension":        extension,
                "created_by":       attrs.get("createUserName"),
                "created_at":       attrs.get("createTime"),
                "last_modified_by": attrs.get("lastModifiedUserName"),
                "last_modified_at": attrs.get("lastModifiedTime"),
                "folder_path":      folder_path,
            })


if __name__ == "__main__":
    if not PROJECT_ID:
        print("[Forma] No active project set. Select a Forma/ACC project in the sidebar first.")
        exit(1)

    print(f"Fetching top folders for project {PROJECT_ID}...\n")
    top_folders = get_top_folders(PROJECT_ID)
    print(f"Found {len(top_folders)} top folder(s): {[f['attributes']['displayName'] for f in top_folders]}\n")

    all_rows = []
    for folder in top_folders:
        folder_id   = folder.get("id")
        folder_name = folder.get("attributes", {}).get("displayName", "")
        print(f"Traversing: {folder_name}")
        traverse_folder(PROJECT_ID, folder_id, folder_name, all_rows)

    print(f"\nTotal files found: {len(all_rows)}")

    json_path = os.path.join(EXPORTS_DIR, "forma_project_files.json")
    with open(json_path, "w") as f:
        json.dump(all_rows, f, indent=4)
    print(f"\nSaved {json_path}")

    csv_fields = ["folder_parent", "folder_main", "folder_sub", "name", "extension",
                  "created_by", "created_at", "last_modified_by", "last_modified_at", "folder_path"]
    csv_path = os.path.join(EXPORTS_DIR, "forma_project_files.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Saved {csv_path}")