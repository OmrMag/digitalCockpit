import json
import os
import requests
from Revizto_Config import REGION, BASE_URL, ACCESS_CODE, TOKENS_FILE, _ROOT_DIR

# Re-export for backward compatibility
region = REGION

# ---------------------------------------------------------------------------
# Token persistence — blob when running under the web app, local disk otherwise
# ---------------------------------------------------------------------------

_COCKPIT_USER = os.environ.get("COCKPIT_USER", "")  # set by app.py before subprocess
_AZURE_CONN   = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
_BLOB_CONTAINER = "cockpit-data"


def _safe_email(email: str) -> str:
    return email.lower().replace("@", "_at_").replace(".", "_")


def _blob_key() -> str:
    """Per-admin blob key for this region's tokens."""
    return f"revizto_tokens_{REGION}_{_safe_email(_COCKPIT_USER)}.json"


def _get_blob_client():
    if not _AZURE_CONN or not _COCKPIT_USER:
        return None
    try:
        from azure.storage.blob import BlobServiceClient
        svc = BlobServiceClient.from_connection_string(_AZURE_CONN)
        return svc.get_blob_client(container=_BLOB_CONTAINER, blob=_blob_key())
    except Exception:
        return None


def _load_tokens() -> dict:
    """Load token dict from blob (web app) or local disk (VS Code)."""
    if _COCKPIT_USER and _AZURE_CONN:
        client = _get_blob_client()
        if client:
            try:
                data = client.download_blob().readall()
                return json.loads(data)
            except Exception:
                pass
    # Local disk fallback (VS Code runs, or blob unavailable)
    if os.path.exists(TOKENS_FILE):
        with open(TOKENS_FILE, "r") as f:
            return json.load(f)
    return {}


def _save_tokens(tokens: dict):
    """Persist token dict to blob (web app) or local disk (VS Code)."""
    if _COCKPIT_USER and _AZURE_CONN:
        client = _get_blob_client()
        if client:
            try:
                client.upload_blob(
                    json.dumps(tokens, indent=4).encode(),
                    overwrite=True
                )
                return
            except Exception as e:
                print(f"[Revizto] Blob write failed: {e} — falling back to disk")
    with open(TOKENS_FILE, "w") as f:
        json.dump(tokens, f, indent=4)


# ---------------------------------------------------------------------------

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
    """Get a valid access token, refreshing if needed.

    When running under the web app (COCKPIT_USER env var is set), tokens are
    stored per-admin in Azure Blob so they survive container restarts.
    When running from VS Code (no env var), tokens go to the local TOKENS_FILE.
    """
    tokens = _load_tokens()

    if tokens.get("refresh_token"):
        data = _refresh_token(tokens["refresh_token"])
        if data and "access_token" in data:
            _save_tokens({
                "access_token":  data["access_token"],
                "refresh_token": data["refresh_token"],
            })
            return data["access_token"]
        print("[Revizto] Token refresh failed, falling back to access code...")

    # Fall back to access code
    data = _auth_with_access_code()
    if not data or "access_token" not in data:
        print("[Revizto] Authentication failed completely.")
        return ""

    _save_tokens({
        "access_token":  data["access_token"],
        "refresh_token": data["refresh_token"],
    })
    return data["access_token"]


def get_headers() -> dict:
    """Get headers with a fresh access token."""
    token = get_access_token()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
        "Accept":        "application/json"
    }