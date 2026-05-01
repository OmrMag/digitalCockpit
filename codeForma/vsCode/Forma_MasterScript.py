import requests
import json
import os
import time
import webbrowser
from threading import Lock
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from Forma_Config import CLIENT_ID, CLIENT_SECRET, CLIENT_ID_3L, CLIENT_SECRET_3L, _ROOT_DIR

# ---------------------------------------------------------------------------
# 2-legged auth
# ---------------------------------------------------------------------------
SCOPES     = "account:read account:write data:read"
TOKEN_FILE = os.path.join(_ROOT_DIR, "forma_tokens.json")
TOKEN_URL  = "https://developer.api.autodesk.com/authentication/v2/token"

_token_lock = Lock()

# Blob config — shared token (Forma credentials are organisation-wide)
_AZURE_CONN     = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
_BLOB_CONTAINER = "cockpit-data"
_BLOB_KEY       = "forma_tokens.json"


def _get_blob_client():
    if not _AZURE_CONN:
        return None
    try:
        from azure.storage.blob import BlobServiceClient
        svc = BlobServiceClient.from_connection_string(_AZURE_CONN)
        return svc.get_blob_client(container=_BLOB_CONTAINER, blob=_BLOB_KEY)
    except Exception:
        return None


def _load_token_data() -> dict:
    if _AZURE_CONN:
        client = _get_blob_client()
        if client:
            try:
                return json.loads(client.download_blob().readall())
            except Exception:
                pass
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            return json.load(f)
    return {}


def _save_token_data(data: dict):
    if _AZURE_CONN:
        client = _get_blob_client()
        if client:
            try:
                client.upload_blob(json.dumps(data, indent=4).encode(), overwrite=True)
                return
            except Exception as e:
                print(f"[Forma] Blob write failed: {e} — falling back to disk")
    with open(TOKEN_FILE, "w") as f:
        json.dump(data, f, indent=4)


def fetch_new_token() -> dict:
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    body = {
        "grant_type":    "client_credentials",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope":         SCOPES,
    }
    response = requests.post(TOKEN_URL, headers=headers, data=body)
    if response.status_code == 200:
        data       = response.json()
        token_data = {
            "access_token": data["access_token"],
            "expires_at":   time.time() + data.get("expires_in", 3600) - 60,
        }
        _save_token_data(token_data)
        return token_data
    print(f"[Forma] Error fetching token: {response.status_code}")
    print(json.dumps(response.json(), indent=4))
    raise Exception("Failed to fetch 2-legged access token.")


def get_token() -> str:
    """Return a valid 2-legged access token.
    Token is cached in blob (Azure) or local disk (VS Code) and reused until expiry.
    """
    with _token_lock:
        tokens = _load_token_data()
        if tokens and time.time() < tokens.get("expires_at", 0):
            return tokens["access_token"]
        return fetch_new_token()["access_token"]


def get_headers() -> dict:
    """Get headers with a valid 2-legged token."""
    return {
        "Authorization": f"Bearer {get_token()}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }


# ---------------------------------------------------------------------------
# 3-legged auth
# ---------------------------------------------------------------------------
SCOPES_3L       = "data:read account:read"
TOKEN_FILE_3L   = os.path.join(_ROOT_DIR, "forma_tokens_3l.json")
CALLBACK_URL    = "http://localhost:8080/"
_auth_code      = None


class _CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global _auth_code
        params     = parse_qs(urlparse(self.path).query)
        _auth_code = params.get("code", [None])[0]
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Authorization successful! You can close this window.")

    def log_message(self, format, *args):
        pass


def fetch_new_3legged_token() -> dict:
    """Open browser for user login and exchange code for token.
    NOTE: This requires an interactive browser session — it will NOT work
    when called from a subprocess (e.g. the web app). In that context,
    get_3legged_token() will raise an exception which the caller should handle.
    """
    global _auth_code

    # Guard: refuse to run in a non-interactive subprocess context
    if not os.isatty(0):
        raise Exception(
            "3-legged auth requires a browser session and cannot run in a subprocess. "
            "Run Forma_MasterScript.py directly from your terminal first to cache the token."
        )

    _auth_code = None

    auth_url = (
        f"https://developer.api.autodesk.com/authentication/v2/authorize"
        f"?response_type=code"
        f"&client_id={CLIENT_ID_3L}"
        f"&redirect_uri={CALLBACK_URL}"
        f"&scope={SCOPES_3L.replace(' ', '%20')}"
    )
    print("Opening browser for Autodesk login...")
    webbrowser.open(auth_url)

    print("Waiting for authorization...")
    server = HTTPServer(("localhost", 8080), _CallbackHandler)
    server.handle_request()

    if not _auth_code:
        raise Exception("No auth code received.")

    response = requests.post(
        TOKEN_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type":    "authorization_code",
            "code":          _auth_code,
            "client_id":     CLIENT_ID_3L,
            "client_secret": CLIENT_SECRET_3L,
            "redirect_uri":  CALLBACK_URL,
        }
    )

    if response.status_code == 200:
        data       = response.json()
        token_data = {
            "access_token":  data["access_token"],
            "refresh_token": data.get("refresh_token"),
            "expires_at":    time.time() + data.get("expires_in", 3600) - 60,
        }
        with open(TOKEN_FILE_3L, "w") as f:
            json.dump(token_data, f, indent=4)
        print("[Forma] 3-legged token obtained and saved.")
        return token_data

    print(f"[Forma] Error fetching 3-legged token: {response.status_code}")
    print(json.dumps(response.json(), indent=4))
    raise Exception("Failed to fetch 3-legged access token.")


def refresh_3legged_token(refresh_token: str) -> dict:
    """Refresh an existing 3-legged token."""
    response = requests.post(
        TOKEN_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type":    "refresh_token",
            "refresh_token": refresh_token,
            "client_id":     CLIENT_ID_3L,
            "client_secret": CLIENT_SECRET_3L,
        }
    )
    if response.status_code == 200:
        data       = response.json()
        token_data = {
            "access_token":  data["access_token"],
            "refresh_token": data.get("refresh_token", refresh_token),
            "expires_at":    time.time() + data.get("expires_in", 3600) - 60,
        }
        with open(TOKEN_FILE_3L, "w") as f:
            json.dump(token_data, f, indent=4)
        print("[Forma] 3-legged token refreshed.")
        return token_data
    raise Exception("Failed to refresh 3-legged token.")


def get_3legged_token() -> str:
    """Return a valid 3-legged access token."""
    if os.path.exists(TOKEN_FILE_3L):
        with open(TOKEN_FILE_3L, "r") as f:
            tokens = json.load(f)

        if time.time() < tokens.get("expires_at", 0):
            return tokens["access_token"]

        refresh_token = tokens.get("refresh_token")
        if refresh_token:
            try:
                return refresh_3legged_token(refresh_token)["access_token"]
            except:
                pass

    return fetch_new_3legged_token()["access_token"]


if __name__ == "__main__":
    print("=== 2-legged token ===")
    token = get_token()
    print(f"{token[:30]}...\n")

    print("=== 3-legged token ===")
    token_3l = get_3legged_token()
    print(f"{token_3l[:30]}...")