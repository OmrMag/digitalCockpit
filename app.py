"""
Digital Construction Cockpit — FastAPI Application
"""

import json
import csv
import asyncio
import hashlib
import os
import requests as http_requests
import io
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, Request, Form, UploadFile, File, Depends, Cookie, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from itsdangerous import URLSafeSerializer
import subprocess, sys

app = FastAPI(title="Digital Construction Cockpit", version="0.2")

BASE_DIR    = Path(__file__).parent
EXPORTS_DIR = BASE_DIR / "exports"
EXPORTS_DIR.mkdir(exist_ok=True)
SCRIPT_DIRS = {
    "revizto": BASE_DIR / "codeRevizto" / "vsCode",
    "morta":   BASE_DIR / "codeMorta"   / "vsCode",
    "forma":   BASE_DIR / "codeForma"   / "vsCode",
}

app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

CONFIG_FILE       = BASE_DIR / "config.json"
LOG_FILE          = BASE_DIR / "invite_log.json"
AUTH_FILE         = BASE_DIR / "users.json"
MODELS_CACHE_FILE = BASE_DIR / "forma_models_cache.json"

SECRET_KEY  = "change-this-to-a-long-random-string"   # ← change this!
signer      = URLSafeSerializer(SECRET_KEY)

ALLOWED_DOMAINS = {"@alec.ae"}   # add more domains here if needed
OWNER_EMAIL     = "oawad@alec.ae"  # only this user can access the Project Registry

# ── Azure Blob Storage ─────────────────────────────────────────────────────
AZURE_STORAGE_CONNECTION_STRING = os.environ.get(
    "AZURE_STORAGE_CONNECTION_STRING", ""
)
BLOB_CONTAINER = "cockpit-data"
_blob_client = None

def get_blob_service():
    global _blob_client
    if _blob_client is None and AZURE_STORAGE_CONNECTION_STRING:
        try:
            from azure.storage.blob import BlobServiceClient
            _blob_client = BlobServiceClient.from_connection_string(
                AZURE_STORAGE_CONNECTION_STRING
            )
        except Exception as e:
            print(f"[Blob] Could not connect: {e}")
    return _blob_client

def blob_read(name: str) -> dict | list | None:
    """Read a JSON blob. Returns None if not found or blob storage unavailable."""
    svc = get_blob_service()
    if not svc:
        return None
    try:
        blob = svc.get_blob_client(container=BLOB_CONTAINER, blob=name)
        data = blob.download_blob().readall()
        return json.loads(data)
    except Exception:
        return None

def blob_write(name: str, data: dict | list):
    """Write a JSON blob. Silently falls back to disk if unavailable."""
    svc = get_blob_service()
    if not svc:
        return
    try:
        blob = svc.get_blob_client(container=BLOB_CONTAINER, blob=name)
        blob.upload_blob(
            json.dumps(data, indent=2).encode(),
            overwrite=True
        )
    except Exception as e:
        print(f"[Blob] Write failed for {name}: {e}")

def blob_delete(name: str):
    svc = get_blob_service()
    if not svc:
        return
    try:
        svc.get_blob_client(container=BLOB_CONTAINER, blob=name).delete_blob()
    except Exception:
        pass

USE_BLOB = bool(AZURE_STORAGE_CONNECTION_STRING)

# ── Project Registry helpers ───────────────────────────────────────────────
def load_registry() -> dict:
    if USE_BLOB:
        data = blob_read("projects_registry.json")
        if data is not None:
            return data
    p = BASE_DIR / "projects_registry.json"
    if p.exists():
        with open(p) as f:
            return json.load(f)
    return {}

def save_registry(data: dict):
    if USE_BLOB:
        blob_write("projects_registry.json", data)
    else:
        with open(BASE_DIR / "projects_registry.json", "w") as f:
            json.dump(data, f, indent=2)

def get_user_projects(email: str) -> list[str]:
    """Return list of project keys the user is admin of."""
    registry = load_registry()
    return [k for k, v in registry.items() if email in v.get("admins", [])]

def is_owner(user: str) -> bool:
    return user == OWNER_EMAIL

# ---------------------------------------------------------------------------
# Auth Helpers
# ---------------------------------------------------------------------------

def load_users():
    if USE_BLOB:
        data = blob_read("users.json")
        if data is not None:
            return data
    if AUTH_FILE.exists():
        with open(AUTH_FILE) as f:
            return json.load(f)
    return {}

def save_users(users):
    if USE_BLOB:
        blob_write("users.json", users)
    else:
        with open(AUTH_FILE, "w") as f:
            json.dump(users, f, indent=2)

def hash_password(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()

def make_session(email: str) -> str:
    return signer.dumps(email)

def get_current_user(session: str = Cookie(default=None)):
    if not session:
        return None
    try:
        return signer.loads(session)
    except Exception:
        return None

def require_user(user: str = Depends(get_current_user)):
    if not user:
        raise HTTPException(status_code=303, headers={"Location": "/login"})
    return user

def require_owner(user: str = Depends(require_user)):
    if user != OWNER_EMAIL:
        raise HTTPException(status_code=403, detail="Access restricted to system owner.")
    return user

# ---------------------------------------------------------------------------
# Config & Log
# ---------------------------------------------------------------------------

def get_default_config():
    return {
        "morta":   {"api_key": "", "hub_id": ""},
        "revizto_dubai":   {"license_uuid": "", "auth_method": "", "default_license_role": 2, "access_code": ""},
        "revizto_ireland": {"license_uuid": "", "auth_method": "", "default_license_role": 2, "access_code": ""},
        "forma":   {"client_id": "", "client_secret": "", "client_id_3l": "", "client_secret_3l": "", "account_id": "", "project_id": "", "admin_user_id": ""},
        "projects": {"morta": [], "revizto": [], "forma": []},
        "active_context": {
            "active_project_key":   "", "active_project_name":  "",
            "morta_hub_id": "", "morta_hub_name": "",
            "revizto_project_uuid": "", "revizto_project_name": "", "revizto_region": "",
            "forma_project_id": "", "forma_project_name": "", "forma_platform": ""
        },
    }

def get_user_config_file(email: str = None) -> Path:
    """Return per-user config file path for local fallback."""
    if email:
        safe = email.lower().replace("@", "_at_").replace(".", "_")
        return BASE_DIR / f"config_{safe}.json"
    return CONFIG_FILE

def _config_blob_name(email: str = None) -> str:
    if email:
        safe = email.lower().replace("@", "_at_").replace(".", "_")
        return f"config_{safe}.json"
    return "config.json"

def load_config(email: str = None):
    default = get_default_config()
    # Try blob first
    if USE_BLOB:
        saved = blob_read(_config_blob_name(email))
        if saved:
            for p in default:
                if p not in saved: saved[p] = default[p]
                else:
                    for k in default[p]:
                        if k not in saved[p]: saved[p][k] = default[p][k]
            return saved
    # Fall back to local file
    path = get_user_config_file(email)
    if path.exists():
        with open(path, "r") as f:
            saved = json.load(f)
        for p in default:
            if p not in saved: saved[p] = default[p]
            else:
                for k in default[p]:
                    if k not in saved[p]: saved[p][k] = default[p][k]
        return saved
    return default

def save_config(config, email: str = None):
    if USE_BLOB:
        blob_write(_config_blob_name(email), config)
    else:
        path = get_user_config_file(email)
        with open(path, "w") as f:
            json.dump(config, f, indent=2)

def load_invite_log():
    if USE_BLOB:
        data = blob_read("invite_log.json")
        if data is not None:
            return data
    if LOG_FILE.exists():
        with open(LOG_FILE, "r") as f:
            return json.load(f)
    return []

def save_invite_log(log):
    if USE_BLOB:
        blob_write("invite_log.json", log)
    else:
        with open(LOG_FILE, "w") as f:
            json.dump(log, f, indent=2)

def load_models_cache() -> dict:
    """Load per-project model cache."""
    if USE_BLOB:
        data = blob_read("forma_models_cache.json")
        if data is not None:
            return data
    if MODELS_CACHE_FILE.exists():
        with open(MODELS_CACHE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_models_cache(cache: dict):
    if USE_BLOB:
        blob_write("forma_models_cache.json", cache)
    else:
        with open(MODELS_CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)

def append_invite_log(entries):
    log = load_invite_log()
    log.extend(entries)
    save_invite_log(log)

# ---------------------------------------------------------------------------
# Platform Helpers — Names / Fetch
# ---------------------------------------------------------------------------

def get_morta_hub_name(config):
    api_key = config["morta"]["api_key"]
    hub_id  = config["morta"]["hub_id"]
    if not api_key or not hub_id:
        return "Not configured"
    response = http_requests.get(
        f"https://api.morta.io/v1/hub/{hub_id}",
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=10,
    )
    if response.status_code == 200:
        return response.json().get("data", {}).get("name", hub_id[:12] + "...")
    return hub_id[:12] + "..."

def fetch_morta_hubs(config):
    """Fetch projects from Morta user profile (favourites = active projects)."""
    api_key = config["morta"]["api_key"]
    if not api_key:
        return []
    response = http_requests.get(
        "https://api.morta.io/v1/user/me",
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=10,
    )
    if response.status_code != 200:
        return []
    data = response.json().get("data", {})
    favourites = data.get("favourites", [])
    return [
        {"id": f["publicId"], "name": f["name"]}
        for f in favourites
        if not f.get("isDeleted", False)
    ]

def revizto_get_token_for_region(config, region: str) -> str:
    """Get a valid Revizto access token for a specific region."""
    rc          = config.get(f"revizto_{region}", {})
    base_url    = f"https://api.{region}.revizto.com/v5"
    tokens_file = BASE_DIR / f"revizto_tokens_{region}.json"

    # Try refresh token first
    if tokens_file.exists():
        with open(tokens_file) as f:
            tokens = json.load(f)
        refresh_token = tokens.get("refresh_token", "")
        if refresh_token:
            r = http_requests.post(
                f"{base_url}/oauth2",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                timeout=10,
            )
            print(f"[Revizto/{region}] Token refresh → {r.status_code}")
            if r.status_code == 200 and "access_token" in r.json():
                data = r.json()
                with open(tokens_file, "w") as f:
                    json.dump({"access_token": data["access_token"], "refresh_token": data["refresh_token"]}, f, indent=4)
                return data["access_token"]
            else:
                print(f"[Revizto/{region}] Refresh failed: {r.text[:200]}")
        else:
            print(f"[Revizto/{region}] tokens.json exists but has no refresh_token — deleting and retrying with access code")
            tokens_file.unlink(missing_ok=True)

    # Fall back to access code
    access_code = rc.get("access_code", "")
    if not access_code:
        print(f"[Revizto/{region}] No access_code in config — cannot authenticate")
        return ""

    print(f"[Revizto/{region}] Trying access code auth...")
    r = http_requests.get(f"{base_url}/oauth2?code={access_code}", timeout=10)
    print(f"[Revizto/{region}] Access code auth → {r.status_code}")
    if r.status_code == 200 and "access_token" in r.json():
        data = r.json()
        with open(tokens_file, "w") as f:
            json.dump({"access_token": data["access_token"], "refresh_token": data["refresh_token"]}, f, indent=4)
        print(f"[Revizto/{region}] Token saved to {tokens_file}")
        return data["access_token"]
    else:
        print(f"[Revizto/{region}] Access code auth failed: {r.text[:300]}")
        print(f"[Revizto/{region}] Access codes are one-time use — generate a new one at https://ws.revizto.com/login?request=accessCode")

    return ""


def revizto_resolve_license(config, region: str) -> tuple[str, str]:
    """Resolve license_uuid and auth_method for a region dynamically from the API.
    Returns (license_uuid, auth_method) — both empty strings on failure.
    Results are cached back into config to avoid repeated API calls within a request."""
    rc = config.get(f"revizto_{region}", {})

    # Return cached values if already resolved this session
    if rc.get("license_uuid") and rc.get("auth_method"):
        return rc["license_uuid"], rc["auth_method"]

    token = revizto_get_token_for_region(config, region)
    if not token:
        return "", ""

    base_url = f"https://api.{region}.revizto.com/v5"
    headers  = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    # Fetch user licenses
    r = http_requests.get(f"{base_url}/user/licenses", headers=headers, timeout=15)
    if r.status_code != 200:
        print(f"[Revizto/{region}] Could not fetch licenses: {r.status_code}")
        return "", ""
    try:
        licenses = r.json()["data"]["entities"]
    except (KeyError, TypeError):
        return "", ""
    if not licenses:
        return "", ""

    # Use the first license (primary license for this region)
    lic         = licenses[0]
    license_uuid = lic.get("uuid", "")

    # Fetch auth methods for this license
    auth_method = ""
    r2 = http_requests.get(f"{base_url}/license/{license_uuid}/auth-methods", headers=headers, timeout=15)
    if r2.status_code == 200:
        try:
            methods = r2.json()["data"]["entities"]
            if methods:
                auth_method = methods[0].get("uuid", "")
        except (KeyError, TypeError):
            pass

    if not auth_method:
        print(f"[Revizto/{region}] Warning: could not resolve auth_method — invites may fail")

    # Cache back into config (in-memory only, not saved to disk)
    config.setdefault(f"revizto_{region}", {})["license_uuid"] = license_uuid
    config.setdefault(f"revizto_{region}", {})["auth_method"]  = auth_method

    print(f"[Revizto/{region}] Resolved license_uuid={license_uuid} auth_method={auth_method}")
    return license_uuid, auth_method


def fetch_revizto_projects(config):
    """Fetch all projects across all user licenses, for both configured regions."""
    all_projects = []
    seen_uuids = set()

    for region in ["dubai", "ireland"]:
        rc = config.get(f"revizto_{region}", {})
        if not rc.get("access_code") and not (BASE_DIR / f"revizto_tokens_{region}.json").exists():
            print(f"[Revizto/{region}] Skipping — no access_code and no saved token")
            continue
        token = revizto_get_token_for_region(config, region)
        if not token:
            print(f"[Revizto/{region}] Skipping — no valid token")
            continue
        base_url = f"https://api.{region}.revizto.com/v5"
        headers  = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        # Step 1: get all licenses available to this user
        r = http_requests.get(f"{base_url}/user/licenses", headers=headers, timeout=15)
        print(f"[Revizto/{region}] User licenses status={r.status_code}")
        if r.status_code != 200:
            print(f"[Revizto/{region}] Error fetching licenses: {r.text[:300]}")
            continue
        try:
            licenses = r.json()["data"]["entities"]
        except (KeyError, TypeError) as e:
            print(f"[Revizto/{region}] Unexpected license response: {e} | raw: {r.text[:300]}")
            continue
        print(f"[Revizto/{region}] Found {len(licenses)} license(s)")

        # Step 2: fetch projects for every license
        for lic in licenses:
            lic_uuid = lic.get("uuid", "")
            lic_name = lic.get("name", lic_uuid)
            if not lic_uuid:
                continue
            r2 = http_requests.get(
                f"{base_url}/project/list/{lic_uuid}/paged",
                headers=headers,
                timeout=15,
            )
            print(f"[Revizto/{region}] License '{lic_name}' projects status={r2.status_code}")
            if r2.status_code != 200:
                print(f"[Revizto/{region}] Error: {r2.text[:300]}")
                continue
            try:
                projects = r2.json()["data"]["data"]
            except (KeyError, TypeError) as e:
                print(f"[Revizto/{region}] Unexpected project response: {e} | raw: {r2.text[:300]}")
                continue
            added = 0
            for p in projects:
                uuid = p.get("uuid", "")
                if uuid and uuid not in seen_uuids:
                    seen_uuids.add(uuid)
                    all_projects.append({
                        "uuid":   uuid,
                        "title":  p.get("title", "Unnamed"),
                        "region": region,
                    })
                    added += 1
            print(f"[Revizto/{region}] License '{lic_name}' — {added} project(s) added")

    print(f"[Revizto] Total projects across all licenses: {len(all_projects)}")
    return all_projects

def get_revizto_project_names(config):
    """Legacy helper — returns saved projects or fetches fresh."""
    return config.get("projects", {}).get("revizto", [])

def get_forma_project_names(config):
    fc = config["forma"]
    if not fc["account_id"]:
        return []
    token = forma_get_token(config)
    if not token:
        return []
    headers     = {"Authorization": f"Bearer {token}"}
    all_projects = []
    for platform in ["acc", "bim360"]:
        response = http_requests.get(
            f"https://developer.api.autodesk.com/construction/admin/v1/accounts/{fc['account_id']}/projects",
            headers=headers,
            params={"limit": 200, "filter[status]": "active", "filter[platform]": platform},
            timeout=15,
        )
        if response.status_code == 200:
            for p in response.json().get("results", []):
                all_projects.append({"id": p["id"], "name": p["name"], "platform": p["platform"]})
    return all_projects


##DEBUG ONLY##############################################

@app.get("/api/debug/revizto")
async def debug_revizto(user: str = Depends(require_user)):
    config = load_config(user)
    results = {}
    for region in ["dubai", "ireland"]:
        rc    = config.get(f"revizto_{region}", {})
        token = revizto_get_token_for_region(config, region)
        results[region] = {
            "has_license_uuid": bool(rc.get("license_uuid")),
            "has_access_code":  bool(rc.get("access_code")),
            "token_obtained":   bool(token),
            "token_prefix":     token[:20] if token else None,
        }
    return JSONResponse(results)

@app.get("/api/debug/revizto/projects")
async def debug_revizto_projects(user: str = Depends(require_user)):
    config = load_config(user)
    token  = revizto_get_token_for_region(config, "dubai")
    rc     = config.get("revizto_dubai", {})
    r = http_requests.get(
        f"https://api.dubai.revizto.com/v5/project/list/{rc['license_uuid']}/paged",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        params={"page": 1, "pageSize": 100},
        timeout=15,
    )
    return JSONResponse({
        "status_code": r.status_code,
        "raw": r.json()
    })

##DEBUG END #############################################

@app.post("/api/forma/models")
async def api_forma_models(request: Request, user: str = Depends(require_user)):
    """Fetch BIM models from the active Forma project by walking top folders.
    Results are cached per project_id in forma_models_cache.json.
    Pass force_refresh=true to bypass cache."""
    body       = await request.json()
    project_id = body.get("project_id", "")
    force      = body.get("force_refresh", False)
    if not project_id:
        return JSONResponse({"models": [], "error": "No project selected."})

    # Return cached models if available and not forcing refresh
    if not force:
        cache = load_models_cache()
        if project_id in cache:
            print(f"[Forma] Returning cached models for project {project_id} ({len(cache[project_id])} models)")
            return JSONResponse({"models": cache[project_id], "cached": True})

    config     = load_config(user)
    token      = forma_get_token(config)
    if not token:
        return JSONResponse({"models": [], "error": "Forma auth failed."})

    fc         = config.get("forma", {})
    account_id = fc.get("account_id", "")
    base_url   = "https://developer.api.autodesk.com"
    headers    = {"Authorization": f"Bearer {token}"}

    # Step 1: get top-level folders
    top_res = http_requests.get(
        f"{base_url}/project/v1/hubs/b.{account_id}/projects/b.{project_id}/topFolders",
        headers=headers, timeout=15,
    )
    print(f"[Forma] Top folders status={top_res.status_code}")
    if top_res.status_code != 200:
        return JSONResponse({"models": [], "error": f"Top folders error {top_res.status_code}: {top_res.text[:200]}"})

    top_folders = top_res.json().get("data", [])
    if not top_folders:
        return JSONResponse({"models": [], "error": "No folders found in project."})

    print(f"[Forma] Walking {len(top_folders)} top folder(s)...")

    MODEL_EXTS = {".rvt", ".ifc", ".nwc", ".nwd"}

    def walk_all():
        found_models    = []
        visited_folders = set()

        def walk_folder(folder_id, folder_name, depth=0):
            if depth > 5 or folder_id in visited_folders:
                return
            visited_folders.add(folder_id)
            print(f"[Forma] {'  ' * depth}Folder: {folder_name}")
            page = 0
            while True:
                try:
                    r = http_requests.get(
                        f"{base_url}/data/v1/projects/b.{project_id}/folders/{folder_id}/contents",
                        headers=headers,
                        params={"page[number]": page, "page[limit]": 200},
                        timeout=20,
                    )
                except Exception as e:
                    print(f"[Forma] Timeout/error on folder {folder_name}: {e}")
                    break
                if r.status_code != 200:
                    print(f"[Forma] Folder {folder_name} returned {r.status_code}")
                    break
                data  = r.json()
                items = data.get("data", [])
                if not items:
                    break
                for item in items:
                    itype = item.get("type", "")
                    attrs = item.get("attributes", {})
                    name  = attrs.get("displayName") or attrs.get("name", "")
                    if itype == "folders":
                        walk_folder(item["id"], name, depth + 1)
                    elif itype == "items":
                        ext = "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""
                        if ext in MODEL_EXTS:
                            found_models.append({"id": item["id"], "name": name})
                            print(f"[Forma] Found model: {name}")
                if not data.get("links", {}).get("next"):
                    break
                page += 1

        for folder in top_folders:
            fname = folder.get("attributes", {}).get("displayName", folder["id"])
            walk_folder(folder["id"], fname)

        return found_models

    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        found_models = await loop.run_in_executor(pool, walk_all)

    # Save to per-project cache
    cache = load_models_cache()
    cache[project_id] = found_models
    save_models_cache(cache)

    print(f"[Forma] Found {len(found_models)} model(s) — cached for project {project_id}")
    return JSONResponse({"models": found_models, "cached": False})

 
@app.get("/api/exports/{filename}")
async def api_get_export(filename: str, user: str = Depends(require_user)):
    """Serve a file from the exports directory as JSON or plain text."""
    safe_name = Path(filename).name  # strip any path traversal
    path = EXPORTS_DIR / safe_name
    if not path.exists():
        return JSONResponse({"error": "File not found"}, status_code=404)
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    if safe_name.endswith(".json"):
        return JSONResponse(json.loads(content))
    return PlainTextResponse(content)


@app.post("/api/context/model")
async def api_set_model_context(request: Request, user: str = Depends(require_user)):
    """Save selected model for the active project into the models cache."""
    body       = await request.json()
    project_id = body.get("project_id", "")
    config     = load_config(user)
    # Also accept project_id from active_context if not provided
    if not project_id:
        project_id = config.get("active_context", {}).get("forma_project_id", "")
    if not project_id:
        return JSONResponse({"status": "error", "detail": "No project_id provided"})

    cache = load_models_cache()
    # Store selected model alongside the project's model list
    cache.setdefault(project_id, [])
    cache[f"{project_id}__selected"] = {
        "model_item_id": body.get("model_item_id", ""),
        "model_name":    body.get("model_name", ""),
    }
    save_models_cache(cache)

    # Also write to config so the Python scripts can pick it up via Forma_Config
    config.setdefault("forma", {})["model_item_id"] = body.get("model_item_id", "")
    config.setdefault("forma", {})["model_name"]    = body.get("model_name", "")
    save_config(config, user)

    return JSONResponse({"status": "ok"})


# ---------------------------------------------------------------------------
# Platform Helpers — Invite
# ---------------------------------------------------------------------------

def morta_invite(email, config):
    api_key = config["morta"]["api_key"]
    hub_id  = config.get("active_context", {}).get("morta_hub_id") or config["morta"].get("hub_id", "")
    if not api_key or not hub_id:
        return {"status": "error", "detail": "Morta not configured — set API key in Settings and select a hub in the sidebar"}
    response = http_requests.post(
        f"https://api.morta.io/v1/hub/{hub_id}/invite",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={"email": email, "projectRole": "member", "tags": []},
        timeout=15,
    )
    if response.status_code in (200, 201):
        return {"status": "success"}
    return {"status": "error", "detail": f"{response.status_code}: {response.text[:200]}"}

def revizto_get_token(config, region: str = None):
    # Resolve region: prefer explicit arg, then active_context, then first configured region
    if not region:
        region = config.get("active_context", {}).get("revizto_region", "")
    if not region:
        for r in ["dubai", "ireland"]:
            if config.get(f"revizto_{r}", {}).get("license_uuid"):
                region = r
                break
    if not region:
        return ""
    return revizto_get_token_for_region(config, region)

def revizto_invite(email, first_name, last_name, company, department, config, project_uuid=None):
    region = config.get("active_context", {}).get("revizto_region", "")
    if not region:
        for r in ["dubai", "ireland"]:
            if config.get(f"revizto_{r}", {}).get("access_code") or (BASE_DIR / f"revizto_tokens_{r}.json").exists():
                region = r
                break
    if not region:
        return {"status": "error", "detail": "Revizto region not set — select a project in the sidebar"}

    license_uuid, auth_method = revizto_resolve_license(config, region)
    if not license_uuid:
        return {"status": "error", "detail": f"Could not resolve Revizto license for region '{region}'"}

    rc       = config.get(f"revizto_{region}", {})
    token    = revizto_get_token_for_region(config, region)
    if not token:
        return {"status": "error", "detail": "Revizto auth failed"}
    base_url = f"https://api.{region}.revizto.com/v5"
    headers  = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "Accept": "application/json"}
    payload  = {
        "data": [{"email": email, "role": rc.get("default_license_role", 2), "firstName": first_name,
                  "lastName": last_name, "tags": [], "authMethod": auth_method,
                  "company": company, "department": department}],
        "preserveRoles": True, "makeGuests": False, "authMethod": auth_method, "deactivate": False,
    }
    response = http_requests.post(f"{base_url}/license/{license_uuid}/invite/bulk", headers=headers, json=payload, timeout=30)
    result   = {"status": "success"} if response.status_code == 200 else {"status": "error", "detail": f"{response.status_code}: {response.text[:200]}"}
    if project_uuid and result["status"] == "success":
        http_requests.post(f"{base_url}/project/{project_uuid}/role/invite", headers=headers,
                           json={"invitations": [email], "company": company, "department": department}, timeout=30)
    return result

def forma_get_token(config):
    fc = config["forma"]
    if not fc["client_id"] or not fc["client_secret"]: return ""
    response = http_requests.post("https://developer.api.autodesk.com/authentication/v2/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials", "client_id": fc["client_id"], "client_secret": fc["client_secret"],
              "scope": "account:read account:write data:read"}, timeout=10)
    if response.status_code == 200: return response.json().get("access_token", "")
    return ""

def forma_invite(email, config, project_id=None):
    fc  = config["forma"]
    pid = project_id or fc["project_id"]
    if not fc["account_id"] or not fc["admin_user_id"] or not pid:
        return {"status": "error", "detail": "Forma not configured"}
    token = forma_get_token(config)
    if not token: return {"status": "error", "detail": "Forma auth failed"}
    headers  = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "User-Id": fc["admin_user_id"]}
    payload  = {"users": [{"email": email, "products": [{"key": "docs", "access": "member"}]}], "suppressAdministrativeEmails": False}
    response = http_requests.post(f"https://developer.api.autodesk.com/construction/admin/v2/projects/{pid}/users:import",
        headers=headers, json=payload, timeout=30)
    if response.status_code in (200, 202): return {"status": "success"}
    return {"status": "error", "detail": f"{response.status_code}: {response.text[:200]}"}

# ---------------------------------------------------------------------------
# Platform Helpers — Search
# ---------------------------------------------------------------------------

def morta_search_user(email, config, allowed_hub_ids: list = None):
    api_key = config["morta"]["api_key"]
    if not api_key: return None

    # Determine which hubs to search
    hub_ids = allowed_hub_ids if allowed_hub_ids else []
    if not hub_ids:
        hub_id = config.get("active_context", {}).get("morta_hub_id") or config["morta"].get("hub_id", "")
        if hub_id: hub_ids = [hub_id]
    if not hub_ids: return None

    headers  = {"Authorization": f"Bearer {api_key}"}
    matches  = []

    for hub_id in hub_ids:
        hub_name = hub_id
        hub_resp = http_requests.get(f"https://api.morta.io/v1/hub/{hub_id}", headers=headers, timeout=10)
        if hub_resp.status_code == 200:
            hub_name = hub_resp.json().get("data", {}).get("name", hub_id)
        response = http_requests.get(f"https://api.morta.io/v1/hub/{hub_id}/members", headers=headers, timeout=15)
        if response.status_code != 200: continue
        for m in response.json().get("data", []):
            u = m.get("user", {})
            if u.get("email", "").lower() == email.lower():
                tags = [{"name": t.get("value", ""), "publicId": t.get("publicId", "")} for t in u.get("tags", []) if isinstance(t, dict)]
                matches.append({
                    "hub_id": hub_id, "hub_name": hub_name,
                    "name": u.get("name", ""), "email": u.get("email", ""),
                    "role": m.get("projectRole", ""), "role_method": m.get("roleAssignedMethod", ""),
                    "tags": tags, "joined": m.get("createdAt", ""), "public_id": u.get("publicId", ""),
                })
                break  # found in this hub, move to next hub

    if not matches: return None
    # Return primary result with all hub memberships listed
    primary = matches[0]
    return {
        "found":    True,
        "name":     primary["name"],
        "email":    primary["email"],
        "public_id": primary["public_id"],
        "hub_id":   primary["hub_id"],
        "hub_name": primary["hub_name"],
        "role":     primary["role"],
        "role_method": primary["role_method"],
        "tags":     primary["tags"],
        "joined":   primary["joined"],
        "hubs":     matches,   # all hub memberships
    }

def revizto_search_user(email, config, allowed_project_uuids: list = None):
    region = config.get("active_context", {}).get("revizto_region", "")
    if not region:
        for r in ["dubai", "ireland"]:
            if config.get(f"revizto_{r}", {}).get("access_code") or (BASE_DIR / f"revizto_tokens_{r}.json").exists():
                region = r; break
    if not region: return None

    license_uuid, _ = revizto_resolve_license(config, region)
    if not license_uuid: return None

    token = revizto_get_token_for_region(config, region)
    if not token: return None
    base_url = f"https://api.{region}.revizto.com/v5"
    headers  = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    response  = http_requests.get(f"{base_url}/license/{license_uuid}/member/list", headers=headers, timeout=15)
    if response.status_code != 200: return None
    data      = response.json().get("data", {})
    user_info = None
    if isinstance(data, dict):
        for mid, member in data.items():
            u = member.get("user", {})
            if u.get("email", "").lower() == email.lower():
                role_map  = {0: "Owner", 1: "License Admin", 2: "Collaborator", 3: "Content Creator", 4: "Guest"}
                user_info = {"found": True, "region": region, "license_uuid": license_uuid,
                    "name": u.get("fullname", ""), "email": u.get("email", ""),
                    "license_role": role_map.get(member.get("role"), str(member.get("role", ""))),
                    "status": "active" if member.get("activated") else "deactivated" if member.get("deactivated") else "pending",
                    "company": member.get("company", ""), "department": member.get("department", ""),
                    "invited_at": member.get("invitedAt", ""), "last_active": u.get("lastActive", ""),
                    "user_uuid": u.get("uuid", ""), "projects": []}
                break
    if not user_info: return None

    response = http_requests.get(f"{base_url}/project/list/{license_uuid}/paged", headers=headers, timeout=15)
    if response.status_code == 200:
        all_projects = response.json().get("data", {}).get("data", [])
        if allowed_project_uuids:
            projects = [p for p in all_projects if p.get("uuid") in allowed_project_uuids]
        else:
            projects = all_projects
        def check_project(project):
            pr = http_requests.get(f"{base_url}/project/{project['uuid']}/role/member/list", headers=headers, timeout=10)
            if pr.status_code == 200:
                pm_data = pr.json().get("data", {})
                if isinstance(pm_data, dict):
                    for pid, pm in pm_data.items():
                        if pm.get("user", {}).get("email", "").lower() == email.lower():
                            return {"name": project.get("title", ""), "uuid": project.get("uuid", ""),
                                "role": pm.get("role", {}).get("name", "") if isinstance(pm.get("role"), dict) else str(pm.get("role", ""))}
            return None
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(check_project, projects))
        user_info["projects"] = [r for r in results if r is not None]
    return user_info

def forma_search_user(email, config, allowed_project_ids: list = None):
    fc = config["forma"]
    if not fc["account_id"]: return None
    token = forma_get_token(config)
    if not token: return None
    headers  = {"Authorization": f"Bearer {token}"}
    hub_info = None
    offset   = 0
    while True:
        response = http_requests.get(f"https://developer.api.autodesk.com/hq/v1/accounts/{fc['account_id']}/users",
            headers=headers, params={"limit": 100, "offset": offset}, timeout=15)
        if response.status_code != 200: break
        batch = response.json()
        if not batch: break
        for u in batch:
            if u.get("email", "").lower() == email.lower():
                hub_info = {"found": True, "name": u.get("name", ""), "email": u.get("email", ""),
                    "hub_role": u.get("role", ""), "hub_status": u.get("status", ""),
                    "company": u.get("company_name") or u.get("company", ""),
                    "job_title": u.get("job_title", ""), "last_sign_in": u.get("last_sign_in", ""),
                    "user_id": u.get("id", ""), "autodesk_id": u.get("uid", ""), "projects": []}
                break
        if hub_info: break
        offset += len(batch)
        if len(batch) < 100: break
    if not hub_info: return None

    all_projects = get_forma_project_names(config)
    # Filter to allowed projects if specified
    if allowed_project_ids:
        projects = [p for p in all_projects if p.get("id") in allowed_project_ids]
    else:
        projects = all_projects

    def check_forma_project(project):
        po = 0
        while True:
            response = http_requests.get(
                f"https://developer.api.autodesk.com/construction/admin/v1/projects/{project['id']}/users",
                headers=headers, params={"limit": 200, "offset": po}, timeout=10)
            if response.status_code != 200: break
            batch = response.json().get("results", [])
            if not batch: break
            for u in batch:
                if u.get("email", "").lower() == email.lower():
                    access = [k for k, v in u.get("accessLevels", {}).items() if v] if isinstance(u.get("accessLevels"), dict) else []
                    return {"name": project["name"], "platform": project["platform"],
                        "status": u.get("status", ""), "access_levels": ", ".join(access) if access else "member",
                        "added_on": u.get("addedOn", "")}
            po += len(batch)
            if len(batch) < 200: break
        return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(check_forma_project, projects))
    hub_info["projects"] = [r for r in results if r is not None]
    return hub_info
# ---------------------------------------------------------------------------
# Auth Routes
# ---------------------------------------------------------------------------

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, user: str = Depends(get_current_user)):
    if user:
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse(request=request, name="login.html", context={})

@app.post("/api/auth/check-email")
async def check_email(request: Request):
    body  = await request.json()
    email = body.get("email", "").strip().lower()
    if not email:
        return JSONResponse({"detail": "Email is required."}, status_code=400)
    users = load_users()
    return JSONResponse({"stage": "password" if email in users else "register"})

@app.post("/api/auth/login")
async def auth_login(request: Request):
    body     = await request.json()
    email    = body.get("email", "").strip().lower()
    password = body.get("password", "")
    users    = load_users()
    if email not in users or users[email]["password"] != hash_password(password):
        return JSONResponse({"detail": "Incorrect email or password."}, status_code=401)
    resp = JSONResponse({"ok": True})
    resp.set_cookie("session", make_session(email), httponly=True, samesite="lax")
    return resp

@app.post("/api/auth/register")
async def auth_register(request: Request):
    body    = await request.json()
    email   = body.get("email", "").strip().lower()
    p1      = body.get("password", "")
    p2      = body.get("confirm", "")
    if not any(email.endswith(d) for d in ALLOWED_DOMAINS):
        return JSONResponse({"detail": "Registration is restricted to ALEC email addresses."}, status_code=403)
    if p1 != p2:
        return JSONResponse({"detail": "Passwords do not match."}, status_code=400)
    if len(p1) < 6:
        return JSONResponse({"detail": "Password must be at least 6 characters."}, status_code=400)
    users = load_users()
    if email in users:
        return JSONResponse({"detail": "This email is already registered."}, status_code=400)
    users[email] = {"password": hash_password(p1), "created": datetime.now().isoformat()}
    save_users(users)
    resp = JSONResponse({"ok": True})
    resp.set_cookie("session", make_session(email), httponly=True, samesite="lax")
    return resp

@app.get("/logout")
async def logout():
    resp = RedirectResponse("/login", status_code=302)
    resp.delete_cookie("session")
    return resp

# ---------------------------------------------------------------------------
# Page Routes  (all protected — require login)
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, user: str = Depends(require_user)):
    config         = load_config(user)
    log            = load_invite_log()
    ctx            = config.get("active_context", {})
    recent_invites = log[-10:][::-1] if log else []
    return templates.TemplateResponse(request=request, name="home.html", context={
        "active_page": "home", "config": config,
        "morta_count": None, "revizto_count": None, "forma_count": None,
        "active_project": ctx.get("active_project_name", ""),
        "morta_hub":      config["morta"]["hub_id"][:12]      + "..." if config["morta"]["hub_id"]      else None,
        "revizto_region": config["revizto_dubai"]["region"].title() if config["revizto_dubai"].get("region") else None,
        "forma_account":  config["forma"]["account_id"][:12]  + "..." if config["forma"]["account_id"] else None,
        "recent_invites": recent_invites, "insights": [],
        "current_user": user,
    })


# ── Portal — public, no login required ───────────────────────────────────

@app.get("/portal/platformAccess", response_class=HTMLResponse)
async def portal_page(request: Request):
    return templates.TemplateResponse(request=request, name="portal.html", context={})


@app.post("/portal/platformAccess/submit")
async def portal_submit(
    request: Request,
    submitter_name:  str = Form(""),
    submitter_email: str = Form(""),
    project_id:      str = Form(""),   # this is the registry key (e.g. "rak-central")
    project_name:    str = Form(""),
    file: UploadFile = File(...),
):
    user = None  # public endpoint
    # Look up platform IDs from registry using the project key
    registry    = load_registry()
    reg_project = registry.get(project_id, {})
    platforms   = reg_project.get("platforms", {})
    reg_name    = reg_project.get("name", project_name)
    import pandas as pd

    contents = await file.read()
    try:
        if file.filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(contents))
        else:
            df = pd.read_excel(io.BytesIO(contents), header=1)
        df.columns = df.columns.str.strip()
    except Exception as e:
        return JSONResponse({"status": "error", "detail": str(e)}, status_code=400)

    users = []
    for _, row in df.iterrows():
        email = str(row.get("Email", "")).strip()
        if not email or email.lower() == "nan":
            continue
        def clean(val):
            s = str(val).strip()
            return "" if s == "nan" else s
        users.append({
            "email":               email,
            "first_name":          clean(row.get("First Name", "")),
            "last_name":           clean(row.get("Last  Name", row.get("Last Name", ""))),
            "company":             clean(row.get("Company", "")),
            "department":          clean(row.get("Department", "")),
            "morta":               clean(row.get("Morta Access",   "")).upper() == "YES",
            "revizto":             clean(row.get("Revizto Access", "")).upper() == "YES",
            "forma":               clean(row.get("ACC Access",     "")).upper() == "YES",
            "project_id":          project_id,
            "project_name":        reg_name,
            "submitted_by_name":   submitter_name,
            "submitted_by_email":  submitter_email,
            "submitted_at":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status":              "pending",
        })

    if not users:
        return JSONResponse({"status": "error", "detail": "No valid users found in file."}, status_code=400)

    pending_file = BASE_DIR / "portal_requests.json"
    existing = {}
    if pending_file.exists():
        with open(pending_file) as f:
            loaded = json.load(f)
        # Migrate old flat list format to new dict format
        existing = loaded if isinstance(loaded, dict) else {}
    # Group by project_id
    key = project_id or "unassigned"
    if key not in existing:
        existing[key] = {
            "project_id":   project_id,
            "project_name": project_name,
            "submissions":  []
        }
    existing[key]["submissions"].append({
        "submitted_by_name":  submitter_name,
        "submitted_by_email": submitter_email,
        "submitted_at":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "users":              users,
    })
    with open(pending_file, "w") as f:
        json.dump(existing, f, indent=2)

    return JSONResponse({"status": "success", "total": len(users)})


@app.get("/api/portal/link")
async def api_portal_link(request: Request, user: str = Depends(require_user)):
    """Generate a portal link using the registry project key."""
    config   = load_config(user)
    ctx      = config.get("active_context", {})
    key      = ctx.get("active_project_key", "")
    name     = ctx.get("active_project_name", "")
    base_url = str(request.base_url).rstrip("/")
    if not key:
        return JSONResponse({"status": "error", "detail": "No active project selected."})
    link = f"{base_url}/portal/platformAccess?pid={key}"
    return JSONResponse({"status": "ok", "link": link, "project_key": key, "project_name": name})


# ── Pending submissions API ───────────────────────────────────────────────

def _load_portal_requests() -> dict:
    if USE_BLOB:
        data = blob_read("portal_requests.json")
        if data is not None:
            return data if isinstance(data, dict) else {}
    p = BASE_DIR / "portal_requests.json"
    if p.exists():
        with open(p) as f:
            loaded = json.load(f)
        return loaded if isinstance(loaded, dict) else {}
    return {}

def _save_portal_requests(data: dict):
    if USE_BLOB:
        blob_write("portal_requests.json", data)
    else:
        with open(BASE_DIR / "portal_requests.json", "w") as f:
            json.dump(data, f, indent=2)


@app.get("/api/pending")
async def api_pending_list(user: str = Depends(require_user)):
    """Return submissions only for projects the user is admin of."""
    data          = _load_portal_requests()
    user_projects = get_user_projects(user)
    # Owner sees all
    if is_owner(user):
        user_projects = list(data.keys())

    submissions = []
    for key, project in data.items():
        if key not in user_projects:
            continue
        for idx, sub in enumerate(project.get("submissions", [])):
            users = sub.get("users", [])
            submissions.append({
                "_key":            key,
                "_idx":            idx,
                "project_id":      project.get("project_id", key),
                "project_name":    project.get("project_name", ""),
                "submitter_name":  sub.get("submitted_by_name", ""),
                "submitter_email": sub.get("submitted_by_email", ""),
                "submitted_at":    sub.get("submitted_at", ""),
                "total":           len(users),
                "status":          "pending" if any(u.get("status","pending") == "pending" for u in users) else "processed",
            })
    submissions.sort(key=lambda s: s.get("submitted_at",""), reverse=True)
    return JSONResponse({"submissions": submissions})


@app.get("/api/pending/{project_key}/{sub_idx}")
async def api_pending_get(project_key: str, sub_idx: int, user: str = Depends(require_user)):
    """Get a specific submission for review."""
    data = _load_portal_requests()
    project = data.get(project_key)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    subs = project.get("submissions", [])
    if sub_idx >= len(subs):
        raise HTTPException(status_code=404, detail="Submission not found")
    sub = subs[sub_idx]
    users = sub.get("users", [])
    # Add approved flag for frontend (default True for pending users)
    for u in users:
        if "approved" not in u:
            u["approved"] = u.get("status", "pending") == "pending"
    return JSONResponse({
        "project_id":      project.get("project_id", project_key),
        "project_name":    project.get("project_name", ""),
        "submitter_name":  sub.get("submitted_by_name", ""),
        "submitter_email": sub.get("submitted_by_email", ""),
        "submitted_at":    sub.get("submitted_at", ""),
        "total":           len(users),
        "users":           users,
    })


@app.post("/api/pending/{project_key}/{sub_idx}/process")
async def api_pending_process(project_key: str, sub_idx: int, request: Request, user: str = Depends(require_user)):
    """Process approved users from a submission — invite them to all requested platforms."""
    body = await request.json()
    users = body.get("users", [])

    success = 0; errors = 0; rejected = 0
    config   = load_config(user)
    registry = load_registry()

    for u in users:
        if not u.get("approved", True):
            rejected += 1
            u["status"] = "rejected"
            continue
        try:
            # Build invite payload matching existing /api/invite/single format
            payload = {
                "email":      u.get("email",""),
                "first_name": u.get("first_name",""),
                "last_name":  u.get("last_name",""),
                "company":    u.get("company",""),
                "department": u.get("department",""),
                "morta":      u.get("morta", False),
                "revizto":    u.get("revizto", False),
                "forma":      u.get("forma", False),
                # Look up platform IDs from registry using project key
                "revizto_project_uuid": registry.get(project_key, {}).get("platforms", {}).get("revizto", {}).get("project_uuid", ""),
                "forma_project_id":     registry.get(project_key, {}).get("platforms", {}).get("forma", {}).get("project_id", ""),
            }
            import httpx
            async with httpx.AsyncClient() as client:
                res = await client.post(
                    str(request.base_url).rstrip("/") + "/api/invite/single",
                    json=payload,
                    cookies={"session": request.cookies.get("session","")},
                    timeout=60,
                )
            if res.status_code == 200:
                u["status"] = "processed"; success += 1
            else:
                u["status"] = "error"; errors += 1
        except Exception as e:
            u["status"] = "error"; errors += 1

    # Save updated statuses back to portal_requests.json
    data = _load_portal_requests()
    if project_key in data and sub_idx < len(data[project_key].get("submissions",[])):
        data[project_key]["submissions"][sub_idx]["users"] = users
    _save_portal_requests(data)

    return JSONResponse({"success": success, "errors": errors, "rejected": rejected})


@app.delete("/api/pending/{project_key}/{sub_idx}")
async def api_pending_delete(project_key: str, sub_idx: int, user: str = Depends(require_user)):
    """Delete a submission."""
    data = _load_portal_requests()
    if project_key in data:
        subs = data[project_key].get("submissions", [])
        if sub_idx < len(subs):
            subs.pop(sub_idx)
        if not subs:
            del data[project_key]
        else:
            data[project_key]["submissions"] = subs
    _save_portal_requests(data)
    return JSONResponse({"status": "ok"})


@app.get("/registry", response_class=HTMLResponse)
async def registry_page(request: Request, user: str = Depends(require_owner)):
    registry = load_registry()
    return templates.TemplateResponse(request=request, name="registry.html", context={
        "active_page": "registry", "current_user": user, "registry": registry,
    })

@app.get("/api/registry")
async def api_registry_get(user: str = Depends(require_user)):
    if not is_owner(user):
        raise HTTPException(status_code=403)
    return JSONResponse(load_registry())

@app.post("/api/registry")
async def api_registry_save(request: Request, user: str = Depends(require_user)):
    if not is_owner(user):
        raise HTTPException(status_code=403)
    body = await request.json()
    registry = load_registry()
    key = body.get("key", "").strip().lower().replace(" ", "-")
    if not key:
        return JSONResponse({"status": "error", "detail": "Project key required."}, status_code=400)
    registry[key] = {
        "name":      body.get("name", "").strip(),
        "platforms": {
            "morta":   {"hub_id": body.get("morta_hub_id", "").strip()},
            "revizto": {"project_uuid": body.get("revizto_uuid", "").strip(),
                        "region": body.get("revizto_region", "dubai").strip()},
            "forma":   {"project_id": body.get("forma_project_id", "").strip()},
        },
        "admins": [e.strip().lower() for e in body.get("admins", "").split(",") if e.strip()],
    }
    save_registry(registry)
    return JSONResponse({"status": "ok", "key": key})

@app.delete("/api/registry/{key}")
async def api_registry_delete(key: str, user: str = Depends(require_user)):
    if not is_owner(user):
        raise HTTPException(status_code=403)
    registry = load_registry()
    registry.pop(key, None)
    save_registry(registry)
    return JSONResponse({"status": "ok"})


@app.get("/api/registry")
async def api_registry_get(user: str = Depends(require_owner)):
    return JSONResponse(load_registry())

@app.post("/api/registry/project")
async def api_registry_create(request: Request, user: str = Depends(require_owner)):
    body = await request.json()
    registry = load_registry()
    key = body.get("key", "").strip().lower().replace(" ", "-")
    if not key:
        return JSONResponse({"status": "error", "detail": "Project key is required."}, status_code=400)
    registry[key] = {
        "name":      body.get("name", key),
        "platforms": {
            "morta":   {"hub_id":       body.get("morta_hub_id", "")},
            "revizto": {"project_uuid": body.get("revizto_uuid", ""),
                        "region":       body.get("revizto_region", "dubai")},
            "forma":   {"project_id":   body.get("forma_project_id", "")},
        },
        "admins": [a.strip().lower() for a in body.get("admins", "").split(",") if a.strip()],
    }
    save_registry(registry)
    return JSONResponse({"status": "ok", "key": key})

@app.put("/api/registry/project/{key}")
async def api_registry_update(key: str, request: Request, user: str = Depends(require_owner)):
    body = await request.json()
    registry = load_registry()
    if key not in registry:
        raise HTTPException(status_code=404, detail="Project not found.")
    registry[key].update({
        "name":      body.get("name", registry[key]["name"]),
        "platforms": {
            "morta":   {"hub_id":       body.get("morta_hub_id", "")},
            "revizto": {"project_uuid": body.get("revizto_uuid", ""),
                        "region":       body.get("revizto_region", "dubai")},
            "forma":   {"project_id":   body.get("forma_project_id", "")},
        },
        "admins": [a.strip().lower() for a in body.get("admins", "").split(",") if a.strip()],
    })
    save_registry(registry)
    return JSONResponse({"status": "ok"})

@app.delete("/api/registry/project/{key}")
async def api_registry_delete(key: str, user: str = Depends(require_owner)):
    registry = load_registry()
    if key in registry:
        del registry[key]
        save_registry(registry)
    return JSONResponse({"status": "ok"})


@app.get("/info", response_class=HTMLResponse)
async def info_page(request: Request, user: str = Depends(require_user)):
    return templates.TemplateResponse(request=request, name="info.html", context={
        "active_page": "info", "current_user": user,
    })


@app.get("/invite", response_class=HTMLResponse)
async def invite_page(request: Request, user: str = Depends(require_user)):
    return templates.TemplateResponse(request=request, name="invite.html", context={
        "active_page": "invite", "current_user": user,
    })

@app.get("/manage", response_class=HTMLResponse)
async def manage_page(request: Request, user: str = Depends(require_user)):
    log           = load_invite_log()
    user_projects = get_user_projects(user)
    # Filter log to only entries for projects the user is admin of
    # Owner sees all. If no registry set up yet, show all.
    if not is_owner(user) and user_projects:
        log = [e for e in log if e.get("project_key") in user_projects
               or e.get("project_id") in user_projects]  # backwards compat
    recent_invites = log[-20:][::-1] if log else []
    return templates.TemplateResponse(request=request, name="manage.html", context={
        "active_page": "manage", "recent_invites": recent_invites, "current_user": user,
    })

@app.get("/scheduler", response_class=HTMLResponse)
async def scheduler_page(request: Request, user: str = Depends(require_user)):
    return templates.TemplateResponse(request=request, name="scheduler.html", context={
        "active_page": "scheduler", "current_user": user,
    })

@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request, user: str = Depends(require_user)):
    config = load_config(user)
    return templates.TemplateResponse(request=request, name="settings.html", context={
        "active_page": "settings", "config": config,
        "message": None, "message_type": None, "current_user": user,
    })

@app.post("/settings", response_class=HTMLResponse)
async def settings_save(request: Request, user: str = Depends(require_user),
    morta_api_key: str = Form(""),
    revizto_dubai_default_role: int = Form(2), revizto_dubai_access_code: str = Form(""),
    revizto_ireland_default_role: int = Form(2), revizto_ireland_access_code: str = Form(""),
    forma_client_id: str = Form(""), forma_client_secret: str = Form(""),
    forma_client_id_3l: str = Form(""), forma_client_secret_3l: str = Form(""),
    forma_account_id: str = Form(""), forma_project_id: str = Form(""), forma_admin_user_id: str = Form("")):
    existing = load_config(user)
    config = {
        "morta": {"api_key": morta_api_key, "hub_id": existing["morta"].get("hub_id", "")},
        "revizto_dubai": {
            # license_uuid and auth_method are resolved dynamically from the API — not stored here
            "default_license_role": revizto_dubai_default_role,
            "access_code": revizto_dubai_access_code,
        },
        "revizto_ireland": {
            "default_license_role": revizto_ireland_default_role,
            "access_code": revizto_ireland_access_code,
        },
        "forma": {"client_id": forma_client_id, "client_secret": forma_client_secret,
                  "client_id_3l": forma_client_id_3l, "client_secret_3l": forma_client_secret_3l,
                  "account_id": forma_account_id, "project_id": forma_project_id, "admin_user_id": forma_admin_user_id},
        # Preserve cached project lists and active sidebar selection
        "projects":       existing.get("projects", {"morta": [], "revizto": [], "forma": []}),
        "active_context": existing.get("active_context", get_default_config()["active_context"]),
    }
    save_config(config, user)
    return templates.TemplateResponse(request=request, name="settings.html", context={
        "active_page": "settings", "config": config,
        "message": "Settings saved successfully.", "message_type": "success", "current_user": user,
    })

@app.get("/extractions", response_class=HTMLResponse)
async def extractions_page(request: Request, user: str = Depends(require_user)):
    return templates.TemplateResponse(request=request, name="extractions.html", context={
        "active_page": "extractions", "current_user": user,
    })

@app.post("/api/extract")
async def api_extract(request: Request, user: str = Depends(require_user)):
    body         = await request.json()
    script_name  = body.get("script", "")
    project_uuid = body.get("project_uuid")

    if not script_name:
        return JSONResponse({"status": "error", "message": "No script specified."}, status_code=400)

    PLATFORM_MAP = {"Revizto": "revizto", "Morta": "morta", "Forma": "forma", "Proccesed": "forma"}
    platform_key = next((v for k, v in PLATFORM_MAP.items() if script_name.startswith(k)), None)
    if not platform_key:
        return JSONResponse({"status": "error", "message": f"Unknown platform for: {script_name}"}, status_code=400)

    script_path = SCRIPT_DIRS[platform_key] / f"{script_name}.py"
    if not script_path.exists():
        return JSONResponse({"status": "error", "message": f"Script not found: {script_name}.py"}, status_code=404)

    if project_uuid:
        config = load_config(user)
        if platform_key == "revizto":
            config.setdefault("revizto", {})["project_uuid"] = project_uuid
        elif platform_key == "forma":
            config.setdefault("forma", {})["project_id"] = project_uuid
        save_config(config, user)

    try:
        loop = asyncio.get_event_loop()
        def run_script():
            return subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True, text=True, timeout=300,
                cwd=str(SCRIPT_DIRS[platform_key]),
            )
        result = await loop.run_in_executor(None, run_script)
        lines  = [l for l in (result.stdout + result.stderr).strip().splitlines() if l.strip()]
        # Check saved files in output — more reliable than exit code alone
        saved  = any("saved" in l.lower() or "export" in l.lower() for l in lines)
        ok     = result.returncode == 0 or saved
        return JSONResponse({
            "status":  "success" if ok else "error",
            "message": "Extraction complete. Output saved to exports/." if ok else f"Script exited with code {result.returncode}.",
            "output":  lines[:10],
        })
    except subprocess.TimeoutExpired:
        return JSONResponse({"status": "error", "message": "Timed out after 5 minutes.", "output": []})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e), "output": []})

#TEMP DEBUG ROUTE TO CHECK PATHS IN DEPLOYMENT ENVIRONMENT    
@app.get("/api/debug/paths")
async def debug_paths():
    return JSONResponse({
        "BASE_DIR": str(BASE_DIR),
        "revizto": str(BASE_DIR.parent / "codeRevizto" / "vsCode"),
        "morta":   str(BASE_DIR.parent / "codeMorta"   / "vsCode"),
        "forma":   str(BASE_DIR.parent / "codeForma"   / "vsCode"),
        "revizto_exists": (BASE_DIR.parent / "codeRevizto" / "vsCode").exists(),
        "morta_exists":   (BASE_DIR.parent / "codeMorta"   / "vsCode").exists(),
        "forma_exists":   (BASE_DIR.parent / "codeForma"   / "vsCode").exists(),
    })    
# ---------------------------------------------------------------------------
# API — Projects List (saved, for dropdowns)
# ---------------------------------------------------------------------------

@app.get("/api/projects")
async def api_projects(user: str = Depends(require_user)):
    """Return saved projects from config (no live API calls)."""
    config = load_config(user)
    projects = config.get("projects", {"morta": [], "revizto": [], "forma": []})
    context  = config.get("active_context", {})
    return JSONResponse({"projects": projects, "active_context": context})

# ---------------------------------------------------------------------------
# API — Fetch & Save Projects (live API calls, triggered from Settings)
# ---------------------------------------------------------------------------

@app.post("/api/projects/fetch")
async def api_fetch_projects(request: Request, user: str = Depends(require_user)):
    """Fetch projects from all platforms and save to config.json."""
    body     = await request.json()
    platform = body.get("platform")
    config   = load_config(user)
    results  = {}

    if platform in ("morta", "all"):
        try:
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                hubs = await loop.run_in_executor(pool, fetch_morta_hubs, config)
            config.setdefault("projects", {})["morta"] = hubs
            results["morta"] = {"count": len(hubs), "items": hubs}
        except Exception as e:
            results["morta"] = {"count": 0, "items": [], "error": str(e)}

    if platform in ("revizto", "all"):
        try:
            dubai_ok   = bool(config.get("revizto_dubai",   {}).get("access_code")) or (BASE_DIR / "revizto_tokens_dubai.json").exists()
            ireland_ok = bool(config.get("revizto_ireland", {}).get("access_code")) or (BASE_DIR / "revizto_tokens_ireland.json").exists()
            if not dubai_ok and not ireland_ok:
                results["revizto"] = {"count": 0, "items": [], "error": "No Revizto access code configured — check Settings"}
            else:
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor() as pool:
                    projects = await loop.run_in_executor(pool, fetch_revizto_projects, config)
                config.setdefault("projects", {})["revizto"] = projects
                if not projects:
                    results["revizto"] = {
                        "count": 0, "items": [],
                        "error": "Auth failed or no projects returned — check server logs. Access code may be expired or already used."
                    }
                else:
                    results["revizto"] = {"count": len(projects), "items": projects}
        except Exception as e:
            results["revizto"] = {"count": 0, "items": [], "error": str(e)}

    if platform in ("forma", "all"):
        try:
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                projects = await loop.run_in_executor(pool, get_forma_project_names, config)
            config.setdefault("projects", {})["forma"] = projects
            results["forma"] = {"count": len(projects), "items": projects}
        except Exception as e:
            results["forma"] = {"count": 0, "items": [], "error": str(e)}

    save_config(config, user)
    return JSONResponse({"status": "ok", "results": results})

# ---------------------------------------------------------------------------
# API — Active Context (sidebar project selection)
# ---------------------------------------------------------------------------

@app.get("/api/context")
async def api_get_context(user: str = Depends(require_user)):
    config = load_config(user)
    return JSONResponse(config.get("active_context", {}))

@app.post("/api/context")
async def api_set_context(request: Request, user: str = Depends(require_user)):
    body   = await request.json()
    config = load_config(user)
    config["active_context"] = {
        "active_project_key":     body.get("active_project_key", ""),
        "active_project_name":    body.get("active_project_name", ""),
        "morta_hub_id":           body.get("morta_hub_id", ""),
        "morta_hub_name":         body.get("morta_hub_name", ""),
        "revizto_project_uuid":   body.get("revizto_project_uuid", ""),
        "revizto_project_name":   body.get("revizto_project_name", ""),
        "revizto_region":         body.get("revizto_region", "dubai"),
        "forma_project_id":       body.get("forma_project_id", ""),
        "forma_project_name":     body.get("forma_project_name", ""),
        "forma_platform":         body.get("forma_platform", "acc"),
    }

    # Write platform IDs back so scripts pick them up
    if body.get("morta_hub_id"):
        config.setdefault("morta", {})["hub_id"] = body["morta_hub_id"]
    if body.get("revizto_project_uuid"):
        config.setdefault("revizto", {})["project_uuid"] = body["revizto_project_uuid"]
        config.setdefault("revizto", {})["region"]       = body.get("revizto_region", "")
    if body.get("forma_project_id"):
        config.setdefault("forma", {})["project_id"] = body["forma_project_id"]

    save_config(config, user)
    return JSONResponse({"status": "ok", "active_context": config["active_context"]})
# ---------------------------------------------------------------------------
# API — Exports (widget builder data source)
# ---------------------------------------------------------------------------

@app.get("/api/exports")
async def api_exports_list(user: str = Depends(require_user)):
    """Return a list of available CSV files in the exports folder."""
    files = sorted(EXPORTS_DIR.glob("*.csv"))
    return JSONResponse({
        "files": [
            {
                "name": f.stem,
                "filename": f.name,
                "size_kb": round(f.stat().st_size / 1024, 1),
                "modified": datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
            }
            for f in files
        ]
    })

@app.get("/api/exports/{filename}")
async def api_exports_read(filename: str, user: str = Depends(require_user)):
    """Read and return the contents of a specific CSV from the exports folder."""
    # Prevent path traversal
    safe_name = Path(filename).name
    if not safe_name.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")
    file_path = EXPORTS_DIR / safe_name
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"{safe_name} not found in exports.")
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows   = list(reader)
        headers = reader.fieldnames or []
    return JSONResponse({
        "filename": safe_name,
        "headers": headers,
        "rows": rows,
        "total": len(rows),
    })

# ---------------------------------------------------------------------------
# API — Invite
# ---------------------------------------------------------------------------

@app.post("/api/invite/single")
async def api_invite_single(request: Request, user: str = Depends(require_user)):
    data   = await request.json()
    config = load_config(user)
    email  = data.get("email", "").strip()
    if not email: return JSONResponse({"status": "error", "detail": "Email is required"}, status_code=400)
    result = {
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "mode": "single", "email": email,
        "first_name": data.get("first_name", ""), "last_name": data.get("last_name", ""),
        "company": data.get("company", ""), "department": data.get("department", ""),
        "morta_status": "skipped", "revizto_status": "skipped", "forma_status": "skipped",
        "revizto_project": data.get("revizto_project_name", ""),
        "forma_project":   data.get("forma_project_name", ""),
        "project_key":     config.get("active_context", {}).get("active_project_key", ""),
    }
    if data.get("morta"):
        r = morta_invite(email, config)
        result["morta_status"] = r["status"]
    if data.get("revizto"):
        r = revizto_invite(email, data.get("first_name", ""), data.get("last_name", ""),
                          data.get("company", ""), data.get("department", ""), config,
                          project_uuid=data.get("revizto_project_uuid"))
        result["revizto_status"] = r["status"]
    if data.get("forma"):
        r = forma_invite(email, config, project_id=data.get("forma_project_id"))
        result["forma_status"] = r["status"]
    append_invite_log([result])
    has_error = any(result[k].startswith("error") for k in ["morta_status", "revizto_status", "forma_status"])
    return JSONResponse({"status": "success" if not has_error else "partial", "result": result})

@app.post("/api/invite/preview")
async def api_invite_preview(file: UploadFile = File(...), user: str = Depends(require_user)):
    import pandas as pd
    contents = await file.read()
    if file.filename.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(contents))
    else:
        df = pd.read_excel(io.BytesIO(contents), header=1)
    df.columns = df.columns.str.strip()
    users = []
    for _, row in df.iterrows():
        email = str(row.get("Email", "")).strip()
        if not email or email == "nan": continue
        fn = str(row.get("First Name", "")).strip()
        ln = str(row.get("Last  Name", row.get("Last Name", ""))).strip()
        co = str(row.get("Company", "")).strip()
        dp = str(row.get("Department", "")).strip()
        users.append({
            "email":      email,
            "first_name": "" if fn == "nan" else fn,
            "last_name":  "" if ln == "nan" else ln,
            "company":    "" if co == "nan" else co,
            "department": "" if dp == "nan" else dp,
            "morta":    str(row.get("Morta Access",   "")).strip().upper() == "YES",
            "revizto":  str(row.get("Revizto Access", "")).strip().upper() == "YES",
            "forma":    str(row.get("ACC Access",     "")).strip().upper() == "YES",
        })
    return JSONResponse({"status": "success", "users": users, "total": len(users)})

@app.post("/api/invite/bulk")
async def api_invite_bulk(request: Request, user: str = Depends(require_user)):
    data   = await request.json()
    config = load_config(user)
    users  = data.get("users", [])
    ctx    = config.get("active_context", {})

    # Use active context from sidebar (no longer passed from dropdown)
    revizto_project_uuid = ctx.get("revizto_project_uuid", "") or data.get("revizto_project_uuid")
    revizto_project_name = ctx.get("revizto_project_name", "") or data.get("revizto_project_name", "")
    forma_project_id     = ctx.get("forma_project_id", "")     or data.get("forma_project_id")
    forma_project_name   = ctx.get("forma_project_name", "")   or data.get("forma_project_name", "")
    project_key          = ctx.get("active_project_key", "")

    results = []
    for u in users:
        result = {
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "mode": "bulk", "email": u["email"],
            "first_name": u.get("first_name", ""), "last_name": u.get("last_name", ""),
            "company": u.get("company", ""), "department": u.get("department", ""),
            "morta_status": "skipped", "revizto_status": "skipped", "forma_status": "skipped",
            "revizto_project": revizto_project_name, "forma_project": forma_project_name,
            "project_key": project_key,
        }
        if u.get("morta"):
            r = morta_invite(u["email"], config)
            result["morta_status"] = r["status"]
        if u.get("revizto"):
            r = revizto_invite(u["email"], u.get("first_name", ""), u.get("last_name", ""),
                              u.get("company", ""), u.get("department", ""), config,
                              project_uuid=revizto_project_uuid)
            result["revizto_status"] = r["status"]
        if u.get("forma"):
            r = forma_invite(u["email"], config, project_id=forma_project_id)
            result["forma_status"] = r["status"]
        results.append(result)
    if results: append_invite_log(results)
    success = sum(1 for r in results if not any(r[k].startswith("error") for k in ["morta_status", "revizto_status", "forma_status"] if r[k] != "skipped"))
    return JSONResponse({"status": "success", "total": len(results), "success": success, "errors": len(results) - success})

# ---------------------------------------------------------------------------
# API — User Search
# ---------------------------------------------------------------------------

@app.get("/api/user/search")
async def api_user_search(email: str, user: str = Depends(require_user)):
    config   = load_config(user)
    email    = email.strip().lower()
    if not email: return JSONResponse({"found": False})

    # Get projects this admin has access to from registry
    registry      = load_registry()
    admin_projects = {k: v for k, v in registry.items() if user in v.get("admins", [])}

    # If owner, they see everything (use all registry projects)
    if user == OWNER_EMAIL:
        admin_projects = registry

    # Extract per-platform allowed IDs
    allowed_morta_hubs    = [v["platforms"]["morta"]["hub_id"]         for v in admin_projects.values() if v.get("platforms", {}).get("morta", {}).get("hub_id")]
    allowed_revizto_uuids = [v["platforms"]["revizto"]["project_uuid"] for v in admin_projects.values() if v.get("platforms", {}).get("revizto", {}).get("project_uuid")]
    allowed_forma_ids     = [v["platforms"]["forma"]["project_id"]      for v in admin_projects.values() if v.get("platforms", {}).get("forma", {}).get("project_id")]

    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        morta_fut   = loop.run_in_executor(pool, morta_search_user,   email, config, allowed_morta_hubs)
        revizto_fut = loop.run_in_executor(pool, revizto_search_user, email, config, allowed_revizto_uuids)
        forma_fut   = loop.run_in_executor(pool, forma_search_user,   email, config, allowed_forma_ids)
        morta_result, revizto_result, forma_result = await asyncio.gather(morta_fut, revizto_fut, forma_fut)

    found = any(r is not None for r in [morta_result, revizto_result, forma_result])
    name  = ""
    for r in [morta_result, forma_result, revizto_result]:
        if r and r.get("name"): name = r["name"]; break
    return JSONResponse({"found": found, "name": name, "email": email,
                         "morta": morta_result, "revizto": revizto_result, "forma": forma_result})

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)