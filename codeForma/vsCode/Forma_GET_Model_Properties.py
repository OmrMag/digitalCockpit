import requests
import json
import csv
import os
import time
import gzip

from Forma_MasterScript import get_3legged_token as _get_3legged_token, get_token as get_2legged_token
from Forma_Config import ACCOUNT_ID, PROJECT_ID, MODEL_ITEM_ID, MODEL_NAME, EXPORTS_DIR

BASE_URL  = "https://developer.api.autodesk.com"
INDEX_URL = f"{BASE_URL}/construction/index/v2"


def get_token():
    """Get best available token: 3-legged if cached, otherwise 2-legged."""
    try:
        return _get_3legged_token()
    except Exception as e:
        print(f"[Forma] 3-legged token unavailable ({e}), falling back to 2-legged.")
        return get_2legged_token()


def get_top_folders(project_id):
    token = get_token()
    url = f"{BASE_URL}/project/v1/hubs/b.{ACCOUNT_ID}/projects/b.{project_id}/topFolders"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get("data", [])
    print(f"[Forma] Error getting top folders: {response.status_code} {response.text}")
    return []


def get_folder_contents(project_id, folder_id):
    token = get_token()
    url = f"{BASE_URL}/data/v1/projects/b.{project_id}/folders/{folder_id}/contents"
    headers = {"Authorization": f"Bearer {token}"}
    all_items = []
    page = 0
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
            break
    return all_items


def find_model_item(project_id, folder_id, model_name):
    """Recursively search folders for a file matching model_name."""
    contents = get_folder_contents(project_id, folder_id)
    for item in contents:
        item_type = item.get("type")
        attrs     = item.get("attributes", {})
        name      = attrs.get("displayName") or attrs.get("name", "")

        if item_type == "folders":
            result = find_model_item(project_id, item.get("id"), model_name)
            if result:
                return result
        elif item_type == "items":
            clean_name = name.rsplit(".", 1)[0] if "." in name else name
            if clean_name.lower() == model_name.lower():
                return item
    return None


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
    print(f"[Forma] Error getting version URN: {response.status_code} {response.text}")
    return None


def get_index_id(project_id, version_urn):
    token = get_token()
    url = f"{INDEX_URL}/projects/{project_id}/indexes:batch-status"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    body = {"versions": [{"versionUrn": version_urn}]}
    response = requests.post(url, headers=headers, json=body)
    if response.status_code == 200:
        indexes = response.json().get("indexes", [])
        if indexes:
            return indexes[0].get("indexId"), indexes[0].get("state")
    print(f"[Forma] Error getting index: {response.status_code} {response.text}")
    return None, None


def poll_index(project_id, index_id):
    token = get_token()
    url = f"{INDEX_URL}/projects/{project_id}/indexes/{index_id}"
    headers = {"Authorization": f"Bearer {token}"}

    print("  Waiting for index to be ready", end="", flush=True)
    while True:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data  = response.json()
            state = data.get("state")
            print(".", end="", flush=True)
            if state == "FINISHED":
                print(" done!")
                return data
            elif state == "FAILED":
                print(f" failed! {data.get('errors')}")
                return None
        time.sleep(5)


def decompress_response(response):
    try:
        return gzip.decompress(response.content).decode("utf-8")
    except:
        return response.content.decode("utf-8")


def download_fields(fields_url, token):
    is_s3 = "s3.amazonaws.com" in fields_url
    headers = {} if is_s3 else {"Authorization": f"Bearer {token}"}
    response = requests.get(fields_url, headers=headers, allow_redirects=True)
    if response.status_code == 200:
        content = decompress_response(response)
        fields = {}
        for line in content.strip().splitlines():
            if line:
                field = json.loads(line)
                fields[field["key"]] = field.get("name", field["key"])
        return fields
    print(f"[Forma] Error downloading fields: {response.status_code}")
    return {}


def download_properties(properties_url, token):
    is_s3 = "s3.amazonaws.com" in properties_url
    headers = {} if is_s3 else {"Authorization": f"Bearer {token}"}
    response = requests.get(properties_url, headers=headers, allow_redirects=True)
    if response.status_code == 200:
        content = decompress_response(response)
        objects = []
        for line in content.strip().splitlines():
            if line:
                objects.append(json.loads(line))
        return objects
    print(f"[Forma] Error downloading properties: {response.status_code} {response.text}")
    return []


if __name__ == "__main__":
    if not PROJECT_ID:
        print("[Forma] No active project set. Select a Forma/ACC project in the sidebar first.")
        exit(1)

    if not MODEL_ITEM_ID and not MODEL_NAME:
        print("[Forma] No model configured. Select a model on the Extractions page first.")
        exit(1)

    if MODEL_ITEM_ID:
        item_id = MODEL_ITEM_ID
        print(f"Using item ID from config: {item_id}")
    else:
        print(f"Searching for '{MODEL_NAME}' in project {PROJECT_ID}...\n")
        top_folders = get_top_folders(PROJECT_ID)
        model_item  = None
        for folder in top_folders:
            model_item = find_model_item(PROJECT_ID, folder.get("id"), MODEL_NAME)
            if model_item:
                break

        if not model_item:
            print(f"Model '{MODEL_NAME}' not found. Check the name in Forma_Config.py and try again.")
            exit()

        item_id = model_item.get("id")
        print(f"Found: {model_item['attributes'].get('displayName')}  (item_id: {item_id})")

    version_urn = get_version_urn(PROJECT_ID, item_id)
    if not version_urn:
        print("Could not retrieve version URN.")
        exit()
    print(f"Version URN: {version_urn}\n")

    print("Requesting index...")
    index_id, state = get_index_id(PROJECT_ID, version_urn)
    if not index_id:
        print("Could not get index ID.")
        exit()
    print(f"Index ID: {index_id}  (state: {state})\n")

    index_data = poll_index(PROJECT_ID, index_id)
    if not index_data:
        exit()

    fields_url     = index_data.get("fieldsUrl")
    properties_url = index_data.get("propertiesUrl")

    print("\nDownloading fields...")
    token  = get_token()
    fields = download_fields(fields_url, token)
    print(f"  {len(fields)} fields found")

    print(f"Properties URL: {properties_url}")
    print("Downloading properties...")
    objects = download_properties(properties_url, token)
    print(f"  {len(objects)} objects found\n")

    all_rows = []
    for obj in objects:
        row = {
            "lmvId":      obj.get("lmvId"),
            "externalId": obj.get("externalId"),
        }
        props = obj.get("props", {})
        for key, value in props.items():
            field_name = fields.get(key, key)
            row[field_name] = value
        all_rows.append(row)

    json_path = os.path.join(EXPORTS_DIR, "forma_model_properties.json")
    with open(json_path, "w") as f:
        json.dump(all_rows, f, indent=4)
    print(f"Saved {json_path}")

    if all_rows:
        all_keys = []
        for row in all_rows:
            for key in row.keys():
                if key not in all_keys:
                    all_keys.append(key)

        csv_path = os.path.join(EXPORTS_DIR, "forma_model_properties.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"Saved {csv_path}")