import requests
import json
import csv
import os
from Revizto_Config import BASE_URL, EXPORTS_DIR, PROJECT_UUID
from Revizto_MasterScript import get_headers


def get_scenes_and_models(project_uuid: str) -> list:
    """Get scenes and models with properties for a project."""
    url = f"{BASE_URL}/project/{project_uuid}/object-properties/get-scenes-and-models"
    params = {"angularUnit": "deg", "linearUnit": "mm"}

    response = requests.get(url, headers=get_headers(), params=params, timeout=30)
    print(f"[Revizto] Status: {response.status_code}")
    if response.status_code != 200:
        print(f"[Revizto] Error: {response.text}")
        return []
    return response.json().get("data", [])


def extract_model_name(properties: list[dict]) -> str:
    """Extract 'Original Name' value from Revizto category properties."""
    for prop in properties:
        if prop.get("categoryDisplayName") == "Revizto" and prop.get("displayName") == "Original Name":
            return prop.get("value", "")
    return ""


def flatten_scenes(scenes: list[dict]) -> list[dict]:
    """Flatten scenes and models into rows for CSV."""
    rows = []
    for scene in scenes:
        scene_id = scene.get("sceneReviztoId", "")
        for model in scene.get("models", []):
            rows.append({
                "modelName":      extract_model_name(model.get("properties", [])),
                "sceneReviztoId": scene_id,
                "modelReviztoId": model.get("modelReviztoId", ""),
                "lastSynced":     model.get("lastSynced", ""),
                "lastSyncedBy":   model.get("lastSyncedBy", ""),
            })
    rows.sort(key=lambda r: r["modelName"].lower())
    return rows


def save_json(data, filename: str = "revizto_scenes_models.json"):
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved to {path}")


def save_csv(rows: list[dict], filename: str = "revizto_scenes_models.csv"):
    if not rows:
        print("No data to save.")
        return
    path = os.path.join(EXPORTS_DIR, filename)
    fieldnames = ["modelName", "sceneReviztoId", "modelReviztoId", "lastSynced", "lastSyncedBy"]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved {len(rows)} rows to {path}")


if __name__ == "__main__":
    if not PROJECT_UUID:
        print("[Revizto] No active project set. Select a Revizto project in the sidebar first.")
        exit(1)

    print(f"[Revizto] Using project UUID: {PROJECT_UUID}")
    data = get_scenes_and_models(PROJECT_UUID)
    if data:
        save_json(data)
        rows = flatten_scenes(data)
        save_csv(rows)
        print(f"\nTotal scenes: {len(data)} | Total model rows: {len(rows)}")
    else:
        print("[Revizto] No scenes/models found.")