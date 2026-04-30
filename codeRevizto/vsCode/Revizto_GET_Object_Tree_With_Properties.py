import requests
import json
import csv
import os
from Revizto_Config import BASE_URL, EXPORTS_DIR, PROJECT_UUID
from Revizto_MasterScript import get_headers
from Revizto_GET_Scenes_and_Models_with_Properties import get_scenes_and_models, flatten_scenes


def get_object_tree(project_uuid: str, scene_revizto_id: int, model_revizto_id: int) -> list:
    """Get all objects in a model, handling pagination."""
    url = f"{BASE_URL}/project/{project_uuid}/object-properties/get-tree"
    all_objects = []
    last_object_id = None

    while True:
        params = {
            "sceneReviztoId": scene_revizto_id,
            "modelReviztoId": model_revizto_id,
            "limit": 50000,
            "angularUnit": "deg",
            "linearUnit": "mm",
        }
        if last_object_id is not None:
            params["lastObjectReviztoId"] = last_object_id

        response = requests.get(url, headers=get_headers(), params=params, timeout=60)
        print(f"[Revizto] Status: {response.status_code} | Objects so far: {len(all_objects)}")

        if response.status_code != 200:
            print(f"[Revizto] Error: {response.text}")
            break

        data = response.json().get("data", [])
        if not data:
            break

        all_objects.extend(data)
        last_object_id = data[-1]["objectReviztoId"]

        if len(data) < 50000:
            break

    return all_objects


def flatten_objects(objects: list, model_revizto_id: int) -> list[dict]:
    """Flatten objects and their properties into rows for CSV."""
    rows = []
    for obj in objects:
        for prop in obj.get("properties", []):
            rows.append({
                "modelReviztoId":      model_revizto_id,
                "objectReviztoId":     obj.get("objectReviztoId"),
                "nodeType":            obj.get("nodeType"),
                "categoryDisplayName": prop.get("categoryDisplayName"),
                "displayName":         prop.get("displayName"),
                "value":               prop.get("value"),
            })
    return rows


def save_json(data, filename: str = "revizto_object_tree.json"):
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved to {path}")


def save_csv(rows: list[dict], filename: str = "revizto_object_tree.csv"):
    if not rows:
        print("No data to save.")
        return
    path = os.path.join(EXPORTS_DIR, filename)
    fieldnames = ["modelReviztoId", "objectReviztoId", "nodeType", "categoryDisplayName", "displayName", "value"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved {len(rows)} rows to {path}")


if __name__ == "__main__":
    if not PROJECT_UUID:
        print("[Revizto] No active project set. Select a Revizto project in the sidebar first.")
        exit(1)

    print(f"[Revizto] Using project UUID: {PROJECT_UUID}")

    raw_scenes = get_scenes_and_models(PROJECT_UUID)
    if not raw_scenes:
        print("[Revizto] No scenes/models found for this project.")
        exit(1)

    models = flatten_scenes(raw_scenes)
    if not models:
        print("[Revizto] No models found in scenes.")
        exit(1)

    # Use the first model automatically — matches web app behaviour (no interactive prompt)
    selected = models[0]
    print(f"[Revizto] Using first model: {selected['modelName']}")
    print(f"          Scene ID: {selected['sceneReviztoId']} | Model ID: {selected['modelReviztoId']}")

    objects = get_object_tree(PROJECT_UUID, selected["sceneReviztoId"], selected["modelReviztoId"])

    if objects:
        rows = flatten_objects(objects, selected["modelReviztoId"])
        save_json(rows)
        save_csv(rows)
        print(f"\nTotal objects: {len(objects)} | Total property rows: {len(rows)}")
    else:
        print("[Revizto] No objects found.")