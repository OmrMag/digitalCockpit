import requests
import json
import csv
import os
from Revizto_Config import BASE_URL, EXPORTS_DIR, PROJECT_UUID, SCENE_ID, MODEL_ID
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


def _resolve_model(models: list[dict]) -> dict | None:
    """Pick the model to extract.

    Priority:
      1. SCENE_ID + MODEL_ID from config (set when the user picks a model in
         the cockpit's /extractions Fetch Models dropdown). Both must match
         a model in the freshly-fetched scenes list.
      2. Fall back to the first model in the project (preserves the
         standalone-from-VS-Code workflow).

    Returns the matching model dict or None if `models` is empty.
    """
    if not models:
        return None

    if SCENE_ID is not None and MODEL_ID is not None:
        match = next(
            (m for m in models
             if int(m["sceneReviztoId"]) == SCENE_ID
             and int(m["modelReviztoId"]) == MODEL_ID),
            None,
        )
        if match:
            print(f"[Revizto] Using selected model: {match['modelName']}")
            print(f"          Scene ID: {match['sceneReviztoId']} | Model ID: {match['modelReviztoId']}")
            return match
        # Selection set but didn't match — likely the user picked a model
        # from a stale dropdown that's no longer in the project. Be loud
        # about the fallback so the operator can re-select.
        print(f"[Revizto] WARNING: requested model (scene {SCENE_ID}, model {MODEL_ID}) "
              "not found in project; falling back to first model.")

    selected = models[0]
    print(f"[Revizto] Using first model: {selected['modelName']}")
    print(f"          Scene ID: {selected['sceneReviztoId']} | Model ID: {selected['modelReviztoId']}")
    return selected


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

    selected = _resolve_model(models)
    if selected is None:
        print("[Revizto] Could not resolve a model to extract.")
        exit(1)

    objects = get_object_tree(PROJECT_UUID, selected["sceneReviztoId"], selected["modelReviztoId"])

    if objects:
        rows = flatten_objects(objects, selected["modelReviztoId"])
        save_json(rows)
        save_csv(rows)
        print(f"\nTotal objects: {len(objects)} | Total property rows: {len(rows)}")
    else:
        print("[Revizto] No objects found.")