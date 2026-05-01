import json
import os
import requests
from Morta_Config import API_KEY, EXPORTS_DIR

BASE_URL = "https://api.morta.io"
HEADERS  = {"Authorization": f"Bearer {API_KEY}"}


def get_current_user() -> dict:
    response = requests.get(f"{BASE_URL}/v1/user/me", headers=HEADERS, timeout=10)
    print(f"[Morta] Status: {response.status_code}")
    return response.json().get("data", {}) if response.status_code == 200 else {}


if __name__ == "__main__":
    user = get_current_user()
    if user:
        path = os.path.join(EXPORTS_DIR, "morta_admin.json")
        with open(path, "w") as f:
            json.dump(user, f, indent=2)
        print(f"Saved {path}")
    else:
        print("[Morta] No admin user data returned.")