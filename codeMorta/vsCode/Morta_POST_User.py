import json
import os
import requests
from Morta_Config import API_KEY, HUB_ID, _ROOT_DIR

BASE_URL    = "https://api.morta.io"
HEADERS     = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
EXPORTS_DIR = os.path.join(_ROOT_DIR, "exports")
os.makedirs(EXPORTS_DIR, exist_ok=True)


def invite_hub_member(email: str, project_role: str = "member", tags: list = []) -> dict:
    payload = {
        "email":       email,
        "projectRole": project_role,
        "tags":        tags,
    }
    response = requests.post(
        f"{BASE_URL}/v1/hub/{HUB_ID}/invite",
        headers=HEADERS,
        json=payload,
        timeout=10,
    )
    print(f"[Morta] Status: {response.status_code}")
    return response.json() if response.content else {}


if __name__ == "__main__":
    if not HUB_ID:
        print("[Morta] No active hub set. Select a Morta hub in the sidebar first.")
        exit(1)

    email = input("Enter user email to invite: ").strip()
    if not email:
        print("No email entered.")
        exit(1)

    result = invite_hub_member(email=email)
    if result:
        path = os.path.join(EXPORTS_DIR, "morta_invite_response.json")
        with open(path, "w") as f:
            json.dump(result, f, indent=2)
        print(f"Saved {path}")
        print(f"\n{json.dumps(result, indent=2)}")