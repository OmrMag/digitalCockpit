import json
import os
import requests
from Revizto_Config import REGION, BASE_URL, ACCESS_CODE, TOKENS_FILE

# Re-export for backward compatibility
region = REGION


def _auth_with_access_code() -> dict:
    """Authenticate using the one-time access code."""
    print("[Revizto] Authenticating with access code...")
    response = requests.get(f"{BASE_URL}/oauth2?code={ACCESS_CODE}", timeout=10)
    if response.status_code != 200:
        print(f"[Revizto] Access code auth failed: {response.status_code}")
        print(response.text)
        print("[Revizto] You may need to generate a new access code at:")
        print("  https://ws.revizto.com/login?request=accessCode")
        return {}
    return response.json()


def _refresh_token(refresh_token: str) -> dict:
    """Refresh an existing token."""
    response = requests.post(
        f"{BASE_URL}/oauth2",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "refresh_token", "refresh_token": refresh_token},
        timeout=10
    )
    if response.status_code != 200:
        return {}
    data = response.json()
    if "access_token" not in data:
        return {}
    return data


def get_access_token() -> str:
    """Get a valid access token, refreshing if needed."""
    # Try refresh first
    if os.path.exists(TOKENS_FILE):
        with open(TOKENS_FILE, "r") as f:
            tokens = json.load(f)

        data = _refresh_token(tokens.get("refresh_token", ""))
        if data and "access_token" in data:
            with open(TOKENS_FILE, "w") as f:
                json.dump({
                    "access_token": data["access_token"],
                    "refresh_token": data["refresh_token"]
                }, f, indent=4)
            return data["access_token"]

        print("[Revizto] Token refresh failed, falling back to access code...")

    # Fall back to access code
    data = _auth_with_access_code()
    if not data or "access_token" not in data:
        print("[Revizto] Authentication failed completely.")
        return ""

    with open(TOKENS_FILE, "w") as f:
        json.dump({
            "access_token": data["access_token"],
            "refresh_token": data["refresh_token"]
        }, f, indent=4)

    return data["access_token"]


def get_headers() -> dict:
    """Get headers with a fresh access token."""
    token = get_access_token()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }