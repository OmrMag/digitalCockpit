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
import urllib.parse
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, Request, Form, UploadFile, File, Depends, Cookie, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from itsdangerous import URLSafeSerializer
import subprocess, sys
import uuid

# Lightweight readiness probe for /profile (Morta API + Revizto access).
# Lives in a separate module so it doesn't touch the import-time-cached
# Morta_Config / Revizto_Config and can probe both Revizto regions cleanly.
from Profile_Status_Check import check_admin_status

app = FastAPI(title="Digital Construction Cockpit", version="0.2")


# ── Request timing middleware (pure ASGI implementation) ───────────────
# Every response gets a `Server-Timing` header that browsers parse and
# display in DevTools → Network tab → Timing column. Values are in
# milliseconds. The header looks like:
#
#     Server-Timing: total;dur=42.7
#
# IMPORTANT: this is implemented as a pure ASGI middleware rather than
# FastAPI's @app.middleware("http") decorator. The decorator uses
# BaseHTTPMiddleware which is widely reported (Starlette docs, FastAPI
# discussions, multiple benchmark articles) to add ~1+ second of
# overhead per request on single-worker setups under concurrent load.
# It wraps the request body in an in-memory channel, creates a task
# group, and runs the handler in a separate coroutine — all of which
# is overhead the request pays even when the middleware does nothing.
#
# Pure ASGI middleware is a class with __call__ that wraps `send`. It
# adds essentially zero overhead because there's no extra coroutine
# layer or request cloning.
class ServerTimingMiddleware:
    """Adds a Server-Timing response header with the wall-clock time
    spent in the application from start of request to first response
    chunk. Slow requests (>500ms) are also logged to stderr."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        import time as _time
        start = _time.perf_counter()

        async def send_with_timing(message):
            # Inject the Server-Timing header into the response start
            if message["type"] == "http.response.start":
                elapsed_ms = (_time.perf_counter() - start) * 1000
                headers = list(message.get("headers", []))
                headers.append(
                    (b"server-timing", f"total;dur={elapsed_ms:.1f}".encode())
                )
                message["headers"] = headers
                # Log slow paths so they surface in Azure log stream
                if elapsed_ms > 500:
                    method = scope.get("method", "?")
                    path   = scope.get("path", "?")
                    print(f"[slow] {method} {path} → {elapsed_ms:.0f}ms")
            await send(message)

        await self.app(scope, receive, send_with_timing)

app.add_middleware(ServerTimingMiddleware)

BASE_DIR    = Path(__file__).parent
EXPORTS_DIR = BASE_DIR / "exports"
EXPORTS_DIR.mkdir(exist_ok=True)
TEMP_DIR    = BASE_DIR / "temp"
TEMP_DIR.mkdir(exist_ok=True)
SCRIPT_DIRS = {
    "revizto": BASE_DIR / "codeRevizto" / "vsCode",
    "morta":   BASE_DIR / "codeMorta"   / "vsCode",
    "forma":   BASE_DIR / "codeForma"   / "vsCode",
}

app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")


# ── Favicon route alias ────────────────────────────────────────────────
# Browsers automatically fetch /favicon.ico from the domain root for
# every page on the site. Without this route, those requests 404.
# Templates that include <link rel="icon" href="/static/favicon.ico">
# will still work fine — this is a safety net so pages without the
# explicit link (or older/cached pages) still get the icon.
@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    from fastapi.responses import FileResponse
    fav = BASE_DIR / "static" / "favicon.ico"
    if fav.exists():
        return FileResponse(fav, media_type="image/x-icon")
    raise HTTPException(status_code=404)

CONFIG_FILE       = BASE_DIR / "config.json"
LOG_FILE          = BASE_DIR / "invite_log.json"
AUTH_FILE         = BASE_DIR / "admins.json"      # renamed from users.json — see migrate_users_to_admins()
LEGACY_USERS_FILE = BASE_DIR / "users.json"       # only read during migration; never written
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

# Eagerly import the Azure SDK at module load. The import itself takes
# ~200-300ms the first time (it's a chunky library). Doing it here means
# the cost is paid ONCE when the worker starts, not on the first user
# request that needs blob storage.
if AZURE_STORAGE_CONNECTION_STRING:
    try:
        from azure.storage.blob import BlobServiceClient as _BlobServiceClient
        _blob_client = _BlobServiceClient.from_connection_string(
            AZURE_STORAGE_CONNECTION_STRING
        )
        print(f"[Blob] Connected at module import (cold-start tax paid here, not on first request).")
    except Exception as e:
        print(f"[Blob] Could not connect at module import: {e}")

def get_blob_service():
    """Returns the cached BlobServiceClient. The import + connect happens
    once at module load (above), so this is a near-free dictionary
    lookup on every call."""
    global _blob_client
    if _blob_client is None and AZURE_STORAGE_CONNECTION_STRING:
        # Fallback in case the eager init above failed but credentials
        # later become valid. Should be very rare.
        try:
            from azure.storage.blob import BlobServiceClient
            _blob_client = BlobServiceClient.from_connection_string(
                AZURE_STORAGE_CONNECTION_STRING
            )
        except Exception as e:
            print(f"[Blob] Could not connect (lazy retry): {e}")
    return _blob_client

def blob_read(name: str) -> dict | list | None:
    """Read a JSON blob. Returns None if not found or blob storage unavailable.

    NOTE: This is the SYNC version. Safe to call from sync code or in
    cases where event-loop blocking isn't a concern (e.g. one-off
    startup migration). Async route handlers should prefer the cached
    loaders (load_users / load_registry / load_config / load_invite_log)
    which avoid the network call most of the time.
    """
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

# ── Email (Azure Communication Services — REST) ─────────────────────────────
ACS_CONNECTION_STRING = os.environ.get("AZURE_COMMUNICATION_CONNECTION_STRING", "")
EMAIL_FROM            = os.environ.get("EMAIL_FROM", "DoNotReply@azurecomm.net")
EMAIL_FROM_NAME       = "Digital Construction Cockpit"

def _parse_acs_connection_string(conn_str: str):
    """Parse ACS connection string into (endpoint, access_key)."""
    parts = dict(p.split("=", 1) for p in conn_str.split(";") if "=" in p)
    endpoint   = parts.get("endpoint", "")
    access_key = parts.get("accesskey", "") or parts.get("accessKey", "")
    return endpoint.rstrip("/"), access_key

def send_email(to_emails: list, subject: str, html_body: str) -> bool:
    """Send email via ACS REST API. No SDK required."""
    print(f"[Email] Attempting → {to_emails} | from: {EMAIL_FROM}")
    if not ACS_CONNECTION_STRING or not to_emails:
        print("[Email] Not configured — skipping.")
        return False
    try:
        import hmac, hashlib, base64, uuid
        from datetime import datetime, timezone

        endpoint, access_key = _parse_acs_connection_string(ACS_CONNECTION_STRING)
        if not endpoint or not access_key:
            print("[Email] Could not parse ACS connection string.")
            return False

        url     = f"{endpoint}/emails:send?api-version=2023-03-31"
        payload = {
            "senderAddress": EMAIL_FROM,
            "recipients":    {"to": [{"address": e} for e in to_emails]},
            "content":       {"subject": subject, "html": html_body},
        }
        body_bytes   = json.dumps(payload).encode("utf-8")
        content_hash = base64.b64encode(hashlib.sha256(body_bytes).digest()).decode()
        request_id   = str(uuid.uuid4())
        date_str     = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

        # Build the HMAC-SHA256 signature
        host         = endpoint.replace("https://", "").replace("http://", "")
        string_to_sign = f"POST\n/emails:send?api-version=2023-03-31\n{date_str};{host};{content_hash}"
        decoded_key    = base64.b64decode(access_key)
        signature      = base64.b64encode(
            hmac.new(decoded_key, string_to_sign.encode("utf-8"), hashlib.sha256).digest()
        ).decode()

        headers = {
            "Content-Type":          "application/json",
            "x-ms-date":             date_str,
            "x-ms-content-sha256":   content_hash,
            "Authorization":         f"HMAC-SHA256 SignedHeaders=x-ms-date;host;x-ms-content-sha256&Signature={signature}",
            "Operation-Id":          request_id,
            "x-ms-client-request-id": request_id,
        }

        resp = http_requests.post(url, headers=headers, data=body_bytes, timeout=15)
        print(f"[Email] Response: {resp.status_code} — {resp.text[:200]}")
        return resp.status_code in (200, 202)
    except Exception as e:
        print(f"[Email] Send failed: {type(e).__name__}: {e}")
        return False




# ── Project Registry helpers ───────────────────────────────────────────────
def load_registry() -> dict:
    cached = _cache_get("projects_registry.json")
    if cached is not None:
        return cached

    if USE_BLOB:
        data = blob_read("projects_registry.json")
        if data is not None:
            _cache_set("projects_registry.json", data)
            return data
    p = BASE_DIR / "projects_registry.json"
    if p.exists():
        with open(p) as f:
            data = json.load(f)
            _cache_set("projects_registry.json", data)
            return data
    _cache_set("projects_registry.json", {})
    return {}

def save_registry(data: dict):
    if USE_BLOB:
        blob_write("projects_registry.json", data)
    else:
        with open(BASE_DIR / "projects_registry.json", "w") as f:
            json.dump(data, f, indent=2)
    _cache_invalidate("projects_registry.json")

def get_user_projects(email: str) -> list[str]:
    """Return list of project keys the user is admin of."""
    registry = load_registry()
    return [k for k, v in registry.items() if email in v.get("admins", [])]

def is_owner(user: str) -> bool:
    return user == OWNER_EMAIL


# ── Tiny TTL cache for blob/disk reads ──────────────────────────────
# Without this, every page load makes 3-5 blob round-trips because the
# loaders (load_users, load_registry, load_config, load_invite_log) are
# called from multiple spots in the same request. A 5-second TTL means
# back-to-back reads coalesce to one fetch while still picking up changes
# from blob storage within ~5s of a write.
#
# Save functions explicitly invalidate the matching cache key so writes
# are reflected immediately on the next read.
import time as _time_for_cache

_blob_cache: dict = {}                    # key -> (expires_at, data)
# TTL was 5 seconds initially; bumped to 5 minutes after profiling showed
# the cache was being thrown away long before any user request arrived.
# All save_* functions invalidate their key explicitly via _cache_invalidate,
# so writes are still immediately visible on the next read. The TTL only
# bounds how long stale data can hang around if the file is modified
# OUT-OF-BAND (e.g. you edit admins.json directly in Azure portal). With a
# 5-minute window, edits become visible within 5 minutes max.
_BLOB_CACHE_TTL_SEC = 300

def _cache_get(key: str):
    """Return cached value or None if missing/expired."""
    entry = _blob_cache.get(key)
    if not entry:
        return None
    if entry[0] < _time_for_cache.time():
        _blob_cache.pop(key, None)
        return None
    return entry[1]

def _cache_set(key: str, value):
    """Store value with TTL."""
    _blob_cache[key] = (_time_for_cache.time() + _BLOB_CACHE_TTL_SEC, value)

def _cache_invalidate(key: str):
    """Remove a key so the next read forces a blob fetch."""
    _blob_cache.pop(key, None)


# ---------------------------------------------------------------------------
# Auth Helpers
# ---------------------------------------------------------------------------

def load_users():
    """Load admin records. Reads admins.json from blob (USE_BLOB=true)
    or local disk. Cached in-memory for 5s to coalesce repeated calls
    within the same request. The function name is preserved for backward
    compat with all the existing call sites."""
    cached = _cache_get("admins.json")
    if cached is not None:
        return cached

    if USE_BLOB:
        data = blob_read("admins.json")
        if data is not None:
            _cache_set("admins.json", data)
            return data
    if AUTH_FILE.exists():
        with open(AUTH_FILE) as f:
            data = json.load(f)
            _cache_set("admins.json", data)
            return data
    _cache_set("admins.json", {})
    return {}

def save_users(users):
    """Persist admin records to admins.json (blob or local disk)."""
    if USE_BLOB:
        blob_write("admins.json", users)
    else:
        with open(AUTH_FILE, "w") as f:
            json.dump(users, f, indent=2)
    _cache_invalidate("admins.json")


# ── One-time migration: users.json → admins.json ─────────────────────
# Runs on app startup. Idempotent — if admins.json already exists this
# is a no-op. Backs up users.json before the migration so a corrupted
# rename can be rolled back manually.
def migrate_users_to_admins():
    """Migrate the legacy users.json to admins.json.

    Steps:
      1. If admins.json already exists (blob or disk), skip — already migrated.
      2. Read users.json (blob first, then legacy local disk).
      3. For each record, ensure `first_name` is present (default "").
      4. Write to admins.json.
      5. On disk: rename users.json → users.json.backup-{timestamp}.
         On blob: leave the legacy users.json alone — admins.json is the
         new source of truth, and we don't blob_delete in case anything
         else references it. You can delete it from the Azure portal
         after verifying the migration worked.

    Safe to run on every startup. After the first successful run it's
    a fast no-op (one blob_read or one Path.exists() check).
    """
    # ── 1. Already migrated? ─────────────────────────────────────────
    if USE_BLOB:
        if blob_read("admins.json") is not None:
            return
    elif AUTH_FILE.exists():
        return

    # ── 2. Read legacy users.json ────────────────────────────────────
    legacy = None
    if USE_BLOB:
        legacy = blob_read("users.json")
    if legacy is None and LEGACY_USERS_FILE.exists():
        try:
            with open(LEGACY_USERS_FILE) as f:
                legacy = json.load(f)
        except (OSError, ValueError):
            legacy = None

    if not legacy:
        # Nothing to migrate — fresh install. Leave admins.json absent;
        # next register call will create it.
        print("[migrate] No legacy users.json found — fresh install, skipping migration.")
        return

    # ── 3. Add first_name field if missing ───────────────────────────
    migrated = {}
    for email, rec in legacy.items():
        if not isinstance(rec, dict):
            continue
        new_rec = dict(rec)
        new_rec.setdefault("first_name", "")
        # Ensure cockpit_id exists (was added in an earlier round)
        if not new_rec.get("cockpit_id"):
            import uuid as _uuid
            new_rec["cockpit_id"] = str(_uuid.uuid4())
        migrated[email] = new_rec

    # ── 4. Write admins.json ─────────────────────────────────────────
    if USE_BLOB:
        blob_write("admins.json", migrated)
    else:
        with open(AUTH_FILE, "w") as f:
            json.dump(migrated, f, indent=2)

    # ── 5. Backup the legacy file (local disk only) ──────────────────
    if not USE_BLOB and LEGACY_USERS_FILE.exists():
        from datetime import datetime as _dt
        ts = _dt.now().strftime("%Y%m%d-%H%M%S")
        backup = BASE_DIR / f"users.json.backup-{ts}"
        try:
            LEGACY_USERS_FILE.rename(backup)
            print(f"[migrate] Migrated {len(migrated)} record(s) to admins.json. Legacy backed up → {backup.name}")
        except OSError as e:
            print(f"[migrate] Migrated to admins.json but couldn't rename legacy: {e}")
    else:
        print(f"[migrate] Migrated {len(migrated)} record(s) to admins.json. Legacy users.json blob still exists — delete from Azure portal once verified.")


# Run the migration once at import time so it's done before any route
# handler tries to read admin records. Failure prints but doesn't crash
# the app — better to serve broken than to refuse to boot.
try:
    migrate_users_to_admins()
except Exception as _migrate_err:
    print(f"[migrate] Migration failed: {type(_migrate_err).__name__}: {_migrate_err}")


# ── Pre-warm the cache at module load ────────────────────────────────
# The actual call is at the END of this module (after all loaders are
# defined). Keep the function definition here for proximity to the
# cache machinery.
def _prewarm_cache():
    """Pre-load the common shared blobs into _blob_cache.

    Per-user files (config_*.json) can't be pre-warmed because we don't
    know which user will hit the worker first. They'll be cached on the
    first hit per user. The shared files below are the same for everyone.
    """
    try:
        load_users()
        load_registry()
        load_invite_log()
        _load_portal_requests()
        print(f"[prewarm] Cache pre-warmed with {len(_blob_cache)} entries.")
    except Exception as e:
        print(f"[prewarm] Failed (non-fatal): {type(e).__name__}: {e}")

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

@app.get("/api/test-email")
async def test_email(user: str = Depends(require_user)):
    """Owner-only: fire a test email to verify ACS is working."""
    if not is_owner(user):
        raise HTTPException(status_code=403)
    success = send_email(
        [user],
        "Cockpit Email Test",
        "<p>If you receive this, Azure Communication Services email is working correctly.</p>"
    )
    return JSONResponse({
        "status":         "sent" if success else "failed",
        "to":             user,
        "acs_configured": bool(ACS_CONNECTION_STRING),
        "from":           EMAIL_FROM,
    })

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
    cache_key = f"config:{email or 'default'}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

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
            _cache_set(cache_key, saved)
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
        _cache_set(cache_key, saved)
        return saved
    _cache_set(cache_key, default)
    return default

def save_config(config, email: str = None):
    if USE_BLOB:
        blob_write(_config_blob_name(email), config)
    else:
        path = get_user_config_file(email)
        with open(path, "w") as f:
            json.dump(config, f, indent=2)
    _cache_invalidate(f"config:{email or 'default'}")

def load_invite_log():
    cached = _cache_get("invite_log.json")
    if cached is not None:
        return cached

    if USE_BLOB:
        data = blob_read("invite_log.json")
        if data is not None:
            _cache_set("invite_log.json", data)
            return data
    if LOG_FILE.exists():
        with open(LOG_FILE, "r") as f:
            data = json.load(f)
            _cache_set("invite_log.json", data)
            return data
    _cache_set("invite_log.json", [])
    return []

def save_invite_log(log):
    if USE_BLOB:
        blob_write("invite_log.json", log)
    else:
        with open(LOG_FILE, "w") as f:
            json.dump(log, f, indent=2)
    _cache_invalidate("invite_log.json")

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


def append_activity(event_type: str,
                    project_key: str = None,
                    actor: str = None,
                    **fields):
    """Append a non-invite event to the shared activity log.

    The log file is still invite_log.json (kept for backward-compat) but
    entries now carry a `type` field so the feed can render different
    event kinds. Existing invite entries have no `type` field — readers
    treat that as type='invite'.

    event_type: one of 'extraction', 'registry_edit', 'metadata_sync',
                'package_bake', 'portal_decision'. (Invite events
                continue to be written by the existing invite-log code
                path with no `type` field, so they're implicitly typed.)
    project_key: registry key the event is scoped to (None = global)
    actor:       email of the admin who triggered the event
    **fields:    event-specific extra data (status, counts, detail, etc.)
    """
    entry = {
        "type":        event_type,
        "date":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "project_key": project_key,
        "actor":       actor,
    }
    entry.update(fields)
    append_invite_log([entry])

# ---------------------------------------------------------------------------
# Per-user temp directory  (extraction outputs, session-scoped, 24h TTL)
# ---------------------------------------------------------------------------

def get_user_temp_dir(email: str) -> Path:
    """Return the per-user temp directory, creating it if needed."""
    d = TEMP_DIR / _safe_email(email)
    d.mkdir(parents=True, exist_ok=True)
    return d

def cleanup_stale_temp_dirs(max_age_hours: int = 24):
    """Delete per-user temp folders older than max_age_hours. Called on each extraction."""
    import time as _time
    cutoff = _time.time() - (max_age_hours * 3600)
    for child in TEMP_DIR.iterdir():
        if child.is_dir():
            try:
                # Use the most-recently-modified file in the folder as the age marker
                mtime = max((f.stat().st_mtime for f in child.rglob("*") if f.is_file()), default=child.stat().st_mtime)
                if mtime < cutoff:
                    import shutil
                    shutil.rmtree(child, ignore_errors=True)
                    print(f"[Temp] Cleaned up stale dir: {child.name}")
            except Exception:
                pass

# ---------------------------------------------------------------------------
# Token Storage Helpers  (blob-backed, local-disk fallback)
# ---------------------------------------------------------------------------

def _safe_email(email: str) -> str:
    """Convert an email to a safe filename segment."""
    return email.lower().replace("@", "_at_").replace(".", "_")

def _revizto_token_blob_key(region: str, email: str) -> str:
    """Blob key for a per-admin, per-region Revizto token file."""
    return f"revizto_tokens_{region}_{_safe_email(email)}.json"

def _revizto_token_local_path(region: str, email: str = None) -> Path:
    """Local fallback path — per-admin when email given, legacy shared otherwise."""
    if email:
        return BASE_DIR / f"revizto_tokens_{region}_{_safe_email(email)}.json"
    return BASE_DIR / f"revizto_tokens_{region}.json"

def load_revizto_tokens(region: str, email: str) -> dict:
    """Load Revizto token dict for a specific admin + region."""
    if USE_BLOB:
        data = blob_read(_revizto_token_blob_key(region, email))
        if data:
            return data
    path = _revizto_token_local_path(region, email)
    if path.exists():
        with open(path) as f:
            return json.load(f)
    # Legacy fallback — shared file used before per-user tokens
    legacy = BASE_DIR / f"revizto_tokens_{region}.json"
    if legacy.exists():
        with open(legacy) as f:
            return json.load(f)
    return {}

def save_revizto_tokens(region: str, email: str, data: dict):
    """Persist Revizto token dict for a specific admin + region."""
    if USE_BLOB:
        blob_write(_revizto_token_blob_key(region, email), data)
    else:
        path = _revizto_token_local_path(region, email)
        with open(path, "w") as f:
            json.dump(data, f, indent=4)

# Forma 2-legged token — shared credentials, one token for all
_FORMA_TOKEN_BLOB_KEY   = "forma_tokens.json"
_FORMA_TOKEN_LOCAL_PATH = BASE_DIR / "forma_tokens.json"

def load_forma_tokens() -> dict:
    if USE_BLOB:
        data = blob_read(_FORMA_TOKEN_BLOB_KEY)
        if data:
            return data
    if _FORMA_TOKEN_LOCAL_PATH.exists():
        with open(_FORMA_TOKEN_LOCAL_PATH) as f:
            return json.load(f)
    return {}

def save_forma_tokens(data: dict):
    if USE_BLOB:
        blob_write(_FORMA_TOKEN_BLOB_KEY, data)
    else:
        with open(_FORMA_TOKEN_LOCAL_PATH, "w") as f:
            json.dump(data, f, indent=4)

_forma_token_lock_obj = None

def _get_forma_lock():
    """Lazy-init a threading lock for Forma token refresh."""
    global _forma_token_lock_obj
    if _forma_token_lock_obj is None:
        import threading
        _forma_token_lock_obj = threading.Lock()
    return _forma_token_lock_obj

def forma_get_token(config) -> str:
    """Return a valid Forma 2-legged access token.
    Uses shared Forma credentials (client_id / client_secret).
    Falls back to owner config if admin config has no Forma creds.
    Token is cached in blob (or local disk) and reused until expiry.
    """
    import time
    fc = config.get("forma", {})
    client_id     = fc.get("client_id", "")
    client_secret = fc.get("client_secret", "")

    # Fall back to owner config if this admin has no Forma credentials
    if not client_id or not client_secret:
        owner_cfg = load_config(OWNER_EMAIL)
        fc2       = owner_cfg.get("forma", {})
        client_id     = fc2.get("client_id", "")
        client_secret = fc2.get("client_secret", "")

    if not client_id or not client_secret:
        print("[Forma] No client credentials available — configure Forma in Settings")
        return ""

    with _get_forma_lock():
        tokens = load_forma_tokens()
        if tokens and time.time() < tokens.get("expires_at", 0):
            return tokens["access_token"]

        # Fetch a new 2-legged token
        TOKEN_URL = "https://developer.api.autodesk.com/authentication/v2/token"
        try:
            r = http_requests.post(
                TOKEN_URL,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={
                    "grant_type":    "client_credentials",
                    "client_id":     client_id,
                    "client_secret": client_secret,
                    "scope":         "account:read account:write data:read",
                },
                timeout=15,
            )
        except Exception as e:
            print(f"[Forma] Token request failed: {e}")
            return ""

        if r.status_code != 200:
            print(f"[Forma] Token fetch failed: {r.status_code} {r.text[:200]}")
            return ""

        data = r.json()
        token_data = {
            "access_token": data["access_token"],
            "expires_at":   time.time() + data.get("expires_in", 3600) - 60,
        }
        save_forma_tokens(token_data)
        print("[Forma] New 2-legged token obtained and cached")
        return token_data["access_token"]

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

def revizto_get_token_for_region(config, region: str, email: str = None) -> str:
    """Get a valid Revizto access token for a specific admin + region.

    Tokens are stored per-admin in blob storage (key: revizto_tokens_{region}_{email}.json)
    so they survive Azure container restarts and are never shared between admins.

    email: the logged-in admin's email. When called from a web route, always pass
           the user from Depends(require_user). Omit only for VS Code / CLI runs.
    """
    rc       = config.get(f"revizto_{region}", {})
    base_url = f"https://api.{region}.revizto.com/v5"

    # --- Try refresh token ---
    tokens = load_revizto_tokens(region, email) if email else {}
    if not tokens:
        # Legacy local-disk fallback for VS Code runs with no email
        legacy = BASE_DIR / f"revizto_tokens_{region}.json"
        if legacy.exists():
            with open(legacy) as f:
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
            new_tokens = {"access_token": data["access_token"], "refresh_token": data["refresh_token"]}
            if email:
                save_revizto_tokens(region, email, new_tokens)
            return data["access_token"]
        print(f"[Revizto/{region}] Refresh failed: {r.text[:200]}")

    # --- Fall back to access code ---
    access_code = rc.get("access_code", "")
    if not access_code:
        print(f"[Revizto/{region}] No access_code in config — cannot authenticate")
        return ""

    print(f"[Revizto/{region}] Trying access code auth...")
    r = http_requests.get(f"{base_url}/oauth2?code={access_code}", timeout=10)
    print(f"[Revizto/{region}] Access code auth → {r.status_code}")
    if r.status_code == 200 and "access_token" in r.json():
        data = r.json()
        new_tokens = {"access_token": data["access_token"], "refresh_token": data["refresh_token"]}
        if email:
            save_revizto_tokens(region, email, new_tokens)
        else:
            # VS Code run — write to legacy local file
            with open(BASE_DIR / f"revizto_tokens_{region}.json", "w") as f:
                json.dump(new_tokens, f, indent=4)
        print(f"[Revizto/{region}] Token obtained and saved (user={email or 'local'})")
        return data["access_token"]

    print(f"[Revizto/{region}] Access code auth failed: {r.text[:300]}")
    print(f"[Revizto/{region}] Access codes are one-time use — generate a new one at https://ws.revizto.com/login?request=accessCode")
    return ""


def revizto_resolve_license(config, region: str, email: str = None) -> tuple[str, str]:
    """Resolve license_uuid and auth_method for a region dynamically from the API.
    Returns (license_uuid, auth_method) — both empty strings on failure.
    Results are cached back into config to avoid repeated API calls within a request."""
    rc = config.get(f"revizto_{region}", {})

    # Return cached values if already resolved this session
    if rc.get("license_uuid") and rc.get("auth_method"):
        return rc["license_uuid"], rc["auth_method"]

    token = revizto_get_token_for_region(config, region, email=email)
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


def fetch_revizto_projects(config, email: str = None):
    """Fetch all projects across all user licenses, for both configured regions."""
    all_projects = []
    seen_uuids = set()

    for region in ["dubai", "ireland"]:
        rc = config.get(f"revizto_{region}", {})
        # Check blob tokens if we have an email, otherwise fall back to local disk check
        has_tokens = bool(load_revizto_tokens(region, email)) if email else (BASE_DIR / f"revizto_tokens_{region}.json").exists()
        if not rc.get("access_code") and not has_tokens:
            print(f"[Revizto/{region}] Skipping — no access_code and no saved token")
            continue
        token = revizto_get_token_for_region(config, region, email=email)
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
        token = revizto_get_token_for_region(config, region, email=user)
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
    token  = revizto_get_token_for_region(config, "dubai", email=user)
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

    MODEL_EXTS = {".rvt", ".ifc", ".nwc", ".nwd"}

    def _name(folder):
        return (folder.get("attributes", {}).get("displayName", "") or "")

    def _has_shared_in_name(folder):
        return "shared" in _name(folder).lower()

    # ── Resolve the SHARED entry point ────────────────────────────────
    # ACC's API returns whatever the project's "topFolders" are, which can
    # be either the SHARED folders directly OR a wrapper folder ("Project
    # Files") with the SHARED folders nested inside as children. Try the
    # top level first; if nothing matches, descend one level into each
    # wrapper and look at its children for SHARED-named folders.
    shared_folders = [f for f in top_folders if _has_shared_in_name(f)]

    if not shared_folders:
        # Fallback: probe each top folder for SHARED-named immediate
        # children. This is the path your Wynn project takes — Forma
        # returns "Project Files" only at the top level.
        print(f"[Forma] No SHARED at top level; descending into "
              f"{[_name(f) for f in top_folders]} to look for SHARED children…")
        for wrapper in top_folders:
            wid    = wrapper.get("id")
            wname  = _name(wrapper)
            try:
                r = http_requests.get(
                    f"{base_url}/data/v1/projects/b.{project_id}/folders/{wid}/contents",
                    headers=headers,
                    params={"page[number]": 0, "page[limit]": 200},
                    timeout=20,
                )
            except Exception as e:
                print(f"[Forma] Timeout/error probing wrapper {wname}: {e}")
                continue
            if r.status_code != 200:
                print(f"[Forma] Wrapper {wname} returned {r.status_code}")
                continue
            for child in (r.json().get("data") or []):
                if child.get("type") != "folders":
                    continue
                if _has_shared_in_name(child):
                    print(f"[Forma]   → matched child '{_name(child)}' under '{wname}'")
                    shared_folders.append(child)

    if not shared_folders:
        return JSONResponse({
            "models": [],
            "error": "No SHARED folder found in this project (checked top level + one level down).",
        })

    print(f"[Forma] SHARED entry points: {[_name(f) for f in shared_folders]}")

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

        for folder in shared_folders:
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
    """Serve a file from the user's temp directory as JSON or plain text.
    Used by the extractions page to load scene/model data after a script run.
    """
    safe_name = Path(filename).name
    path = get_user_temp_dir(user) / safe_name
    if not path.exists():
        return JSONResponse({"error": "File not found — run the extraction first."}, status_code=404)
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

def morta_invite(email, config, hub_id: str = None, tag_reference_id: str = None):
    api_key    = config["morta"]["api_key"]
    hub_id     = hub_id or config.get("active_context", {}).get("morta_hub_id") or config["morta"].get("hub_id", "")
    if not api_key or not hub_id:
        return {"status": "error", "detail": "Morta not configured — set API key in Settings and select a hub in the sidebar"}
    headers  = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    response = http_requests.post(
        f"https://api.morta.io/v1/hub/{hub_id}/invite",
        headers=headers,
        json={"email": email, "projectRole": "member", "tags": []},
        timeout=15,
    )
    if response.status_code not in (200, 201):
        return {"status": "error", "detail": f"{response.status_code}: {response.text[:200]}"}

    # Chain tag assignment if tag_reference_id provided
    if tag_reference_id:
        # Look up the user's public_id from the hub members
        try:
            members_resp = http_requests.get(
                f"https://api.morta.io/v1/hub/{hub_id}/members",
                headers={"Authorization": f"Bearer {api_key}"}, timeout=10
            )
            if members_resp.status_code == 200:
                for m in members_resp.json().get("data", []):
                    u = m.get("user", {})
                    if u.get("email", "").lower() == email.lower():
                        user_id = u.get("publicId", "")
                        if user_id:
                            http_requests.post(
                                f"https://api.morta.io/v1/user/{user_id}/tags",
                                headers={**headers, "Accept": "application/json"},
                                json={"tagReferenceId": tag_reference_id},
                                timeout=10,
                            )
                        break
        except Exception:
            pass  # Tag assignment failure shouldn't fail the whole invite

    return {"status": "success"}


def revizto_fetch_project_team_suggestions(project_uuid: str, region: str, token: str,
                                           timeout: int = 15) -> dict:
    """Fetch the project's team and aggregate companies / departments /
    locations seen across team members for use as autocomplete suggestions
    in the invite form.

    Endpoint: GET /v5/project/{projectUuid}/team

    Per Revizto API docs, response shape is:
        {
          "result": 0,
          "data": {
            "count": N,
            "entities": [
              {"company": "...", "department": "...", "location": "...", ... },
              ...
            ]
          }
        }

    All three fields (company, department, location) are top-level on each
    entity — exactly the data we want for the invite form's hybrid
    dropdown/text inputs. Project-scoped, so each project's suggestions
    reflect ITS team — more contextually useful than a license-wide list
    would be.

    Returns: {"companies": [...], "departments": [...], "locations": [...],
              "_member_count": N, "_error": "..." (if failure)}
    """
    suggestions = {"companies": [], "departments": [], "locations": []}
    if not project_uuid:
        suggestions["_error"] = "no project_uuid"
        return suggestions
    try:
        url = f"https://api.{region}.revizto.com/v5/project/{project_uuid}/team"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        r = http_requests.get(url, headers=headers, timeout=timeout)
        if r.status_code != 200:
            short = r.text[:80]
            try:
                short = (r.json().get("message") or short)[:80]
            except Exception:
                pass
            suggestions["_error"] = f"HTTP {r.status_code}: {short}"
            return suggestions

        body = r.json()
        if body.get("result", 0) < 0:
            suggestions["_error"] = f"result={body.get('result')}: {(body.get('message') or '')[:80]}"
            return suggestions

        data = body.get("data") or {}
        if not isinstance(data, dict):
            suggestions["_error"] = f"unexpected data type: {type(data).__name__}"
            return suggestions

        # Per docs: data.entities is an ARRAY of project member objects
        entities = data.get("entities") or []
        if not isinstance(entities, list):
            suggestions["_error"] = f"unexpected entities type: {type(entities).__name__}"
            return suggestions

        seen_co = set()
        seen_dept = set()
        seen_loc = set()
        member_count = 0
        for ent in entities:
            if not isinstance(ent, dict):
                continue
            member_count += 1
            co   = (ent.get("company")    or "").strip()
            dept = (ent.get("department") or "").strip()
            loc  = (ent.get("location")   or "").strip()
            # Case-insensitive dedupe, preserve original casing of first seen
            if co and co.lower() not in seen_co:
                seen_co.add(co.lower())
                suggestions["companies"].append(co)
            if dept and dept.lower() not in seen_dept:
                seen_dept.add(dept.lower())
                suggestions["departments"].append(dept)
            if loc and loc.lower() not in seen_loc:
                seen_loc.add(loc.lower())
                suggestions["locations"].append(loc)

        suggestions["companies"].sort(key=str.lower)
        suggestions["departments"].sort(key=str.lower)
        suggestions["locations"].sort(key=str.lower)
        suggestions["_member_count"] = member_count
    except Exception as e:
        suggestions["_error"] = f"exception: {type(e).__name__}: {str(e)[:120]}"
    return suggestions


def revizto_invite(email, first_name, last_name, company, department, config,
                   project_uuid=None, license_role_id: int = None,
                   project_role_id: int = None,
                   invite_to_license: bool = True,
                   invite_to_project: bool = False,
                   location: str = "",
                   admin_email: str = None):
    region = config.get("active_context", {}).get("revizto_region", "")
    if not region:
        for r in ["dubai", "ireland"]:
            has_tokens = bool(load_revizto_tokens(r, admin_email)) if admin_email else (BASE_DIR / f"revizto_tokens_{r}.json").exists()
            if config.get(f"revizto_{r}", {}).get("access_code") or has_tokens:
                region = r; break
    if not region:
        return {"status": "error", "detail": "Revizto region not set — select a project in the sidebar"}

    license_uuid, auth_method = revizto_resolve_license(config, region, email=admin_email)
    if not license_uuid:
        return {"status": "error", "detail": f"Could not resolve Revizto license for region '{region}'"}

    rc       = config.get(f"revizto_{region}", {})
    token    = revizto_get_token_for_region(config, region, email=admin_email)
    if not token: return {"status": "error", "detail": "Revizto auth failed"}
    base_url = f"https://api.{region}.revizto.com/v5"
    headers  = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "Accept": "application/json"}

    result = {"status": "success"}

    # ── License invite ────────────────────────────────────────────────────
    if invite_to_license:
        role = int(license_role_id or rc.get("default_license_role", 2))
        # Per Revizto API docs the license invite supports company, department,
        # and location as first-class fields. We pass whatever the caller gave
        # us; empty strings are accepted by the API and surface as empty fields
        # in Revizto rather than errors.
        payload = {
            "data": [{"email": email, "role": role, "firstName": first_name,
                      "lastName": last_name, "tags": [], "authMethod": auth_method,
                      "company": company, "department": department, "location": location}],
            "preserveRoles": True, "makeGuests": False, "authMethod": auth_method, "deactivate": False,
        }
        resp = http_requests.post(f"{base_url}/license/{license_uuid}/invite/bulk",
                                   headers=headers, json=payload, timeout=30)
        if resp.status_code != 200:
            result = {"status": "error", "detail": f"License invite: {resp.status_code}: {resp.text[:200]}"}

    # ── Project invite ────────────────────────────────────────────────────
    if invite_to_project and project_uuid and result["status"] == "success":
        proj_payload = {
            "invitations": [email],
            "company":     company,
            "department":  department,
        }
        if project_role_id:
            proj_payload["roleId"] = project_role_id
        resp = http_requests.post(f"{base_url}/project/{project_uuid}/role/invite",
                                   headers=headers, json=proj_payload, timeout=30)
        if resp.status_code != 200:
            result = {"status": "error", "detail": f"Project invite: {resp.status_code}: {resp.text[:200]}"}

    return result


def forma_invite(email, config, project_id=None, company_id: str = None,
                 role_ids: list = None, products: list = None):
    fc  = config["forma"]
    pid = project_id or fc.get("project_id", "")
    if not fc["account_id"] or not fc["admin_user_id"] or not pid:
        return {"status": "error", "detail": "Forma not configured"}
    token = forma_get_token(config)
    if not token: return {"status": "error", "detail": "Forma auth failed"}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "User-Id": fc["admin_user_id"]}

    # Build products array — default to docs/member if not specified
    if products is None:
        products = [{"key": "docs", "access": "member"}]

    user_entry = {"email": email, "products": products}
    if company_id:
        user_entry["companyId"] = company_id
    if role_ids:
        user_entry["roleIds"] = role_ids

    payload  = {"users": [user_entry], "suppressAdministrativeEmails": False}
    response = http_requests.post(
        f"https://developer.api.autodesk.com/construction/admin/v2/projects/{pid}/users:import",
        headers=headers, json=payload, timeout=30
    )
    if response.status_code in (200, 202): return {"status": "success"}
    return {"status": "error", "detail": f"{response.status_code}: {response.text[:200]}"}


# ---------------------------------------------------------------------------
# Platform Helpers — Search
# ---------------------------------------------------------------------------

def morta_search_user(email, config, allowed_hub_ids: list = None):
    """Search for a user across the allowed Morta hubs.

    Uses the API key from the provided config (admin's own key).
    allowed_hub_ids comes from the registry — only hubs the searching admin
    is assigned to are searched. If no hub IDs or no API key, returns None.
    """
    api_key = config["morta"]["api_key"]
    if not api_key:
        return None  # Morta key is personal — no fallback to owner

    # Strictly registry-scoped — if no allowed hubs, return nothing
    hub_ids = allowed_hub_ids if allowed_hub_ids else []
    if not hub_ids:
        return None

    headers = {"Authorization": f"Bearer {api_key}"}
    matches = []

    for hub_id in hub_ids:
        # Get hub name
        hub_name = hub_id
        hub_resp = http_requests.get(
            f"https://api.morta.io/v1/hub/{hub_id}",
            headers=headers, timeout=10
        )
        if hub_resp.status_code == 200:
            hub_name = hub_resp.json().get("data", {}).get("name", hub_id)

        # Search members of this hub
        response = http_requests.get(
            f"https://api.morta.io/v1/hub/{hub_id}/members",
            headers=headers, timeout=15
        )
        if response.status_code != 200:
            print(f"[Morta] Members fetch failed for hub {hub_id}: {response.status_code}")
            continue

        for m in response.json().get("data", []):
            u = m.get("user", {})
            if u.get("email", "").lower() == email.lower():
                tags = [
                    {"name": t.get("value", ""), "publicId": t.get("publicId", "")}
                    for t in u.get("tags", []) if isinstance(t, dict)
                ]
                matches.append({
                    "hub_id":      hub_id,
                    "hub_name":    hub_name,
                    "name":        u.get("name", ""),
                    "email":       u.get("email", ""),
                    "role":        m.get("projectRole", ""),
                    "role_method": m.get("roleAssignedMethod", ""),
                    "tags":        tags,
                    "joined":      m.get("createdAt", ""),
                    "public_id":   u.get("publicId", ""),
                })
                break  # found in this hub, move to next

    if not matches:
        return None

    primary = matches[0]
    return {
        "found":       True,
        "name":        primary["name"],
        "email":       primary["email"],
        "public_id":   primary["public_id"],
        "hub_id":      primary["hub_id"],
        "hub_name":    primary["hub_name"],
        "role":        primary["role"],
        "role_method": primary["role_method"],
        "tags":        primary["tags"],
        "joined":      primary["joined"],
        "hubs":        matches,
    }

def revizto_search_user(email, config, allowed_project_uuids: list = None, admin_email: str = None, registry_projects: dict = None):
    """Search for a user in Revizto, scoped to the allowed project UUIDs from the registry.

    Checks license-level membership using the admin's token, then checks
    project-level membership only for registry-allowed projects.
    If no allowed_project_uuids are provided, returns None (no scope = no results).
    registry_projects: dict of {project_key: registry_entry} for the admin's allowed projects.
    """
    # Strictly registry-scoped
    if not allowed_project_uuids:
        return None

    # Resolve region from registry projects if possible, else fall back to config/token detection
    region = config.get("active_context", {}).get("revizto_region", "")
    if not region and registry_projects:
        for rp in registry_projects.values():
            r = rp.get("platforms", {}).get("revizto", {}).get("region", "")
            if r:
                region = r
                break
    if not region:
        for r in ["dubai", "ireland"]:
            has_tokens = bool(load_revizto_tokens(r, admin_email)) if admin_email else (BASE_DIR / f"revizto_tokens_{r}.json").exists()
            if config.get(f"revizto_{r}", {}).get("access_code") or has_tokens:
                region = r; break
    if not region:
        return None

    token = revizto_get_token_for_region(config, region, email=admin_email)
    if not token:
        return None

    license_uuid, _ = revizto_resolve_license(config, region, email=admin_email)
    if not license_uuid:
        return None

    base_url = f"https://api.{region}.revizto.com/v5"
    headers  = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    # Check license-level membership
    response = http_requests.get(
        f"{base_url}/license/{license_uuid}/member/list",
        headers=headers, timeout=15
    )
    if response.status_code != 200:
        print(f"[Revizto] License member list failed: {response.status_code}")
        return None

    data      = response.json().get("data", {})
    user_info = None
    if isinstance(data, dict):
        for mid, member in data.items():
            u = member.get("user", {})
            if u.get("email", "").lower() == email.lower():
                role_map = {0: "Owner", 1: "License Admin", 2: "Collaborator", 3: "Content Creator", 4: "Guest"}
                user_info = {
                    "found":        True,
                    "region":       region,
                    "license_uuid": license_uuid,
                    "name":         u.get("fullname", ""),
                    "email":        u.get("email", ""),
                    "license_role": role_map.get(member.get("role"), str(member.get("role", ""))),
                    "status":       "active" if member.get("activated") else "deactivated" if member.get("deactivated") else "pending",
                    "company":      member.get("company", ""),
                    "department":   member.get("department", ""),
                    "invited_at":   member.get("invitedAt", ""),
                    "last_active":  u.get("lastActive", ""),
                    "user_uuid":    u.get("uuid", ""),
                    "projects":     [],
                }
                break

    if not user_info:
        return None

    # Check project membership — only within registry-allowed projects
    def check_project(project_uuid):
        pr = http_requests.get(
            f"{base_url}/project/{project_uuid}/role/member/list",
            headers=headers, timeout=10
        )
        if pr.status_code == 200:
            pm_data = pr.json().get("data", {})
            if isinstance(pm_data, dict):
                for pid, pm in pm_data.items():
                    if pm.get("user", {}).get("email", "").lower() == email.lower():
                        role = pm.get("role", {})
                        return {
                            "uuid": project_uuid,
                            "role": role.get("name", "") if isinstance(role, dict) else str(role),
                        }
        return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(check_project, allowed_project_uuids))
    user_info["projects"] = [r for r in results if r is not None]
    return user_info

def forma_search_user(email, config, allowed_project_ids: list = None):
    """Search for a user in Forma/ACC, scoped to registry-allowed project IDs.

    If no allowed_project_ids are provided, returns None (no scope = no results).
    Uses shared Forma credentials (falls back to owner config).
    """
    if not allowed_project_ids:
        return None

    fc = config.get("forma", {})
    if not fc.get("account_id"):
        owner_cfg = load_config(OWNER_EMAIL)
        fc        = owner_cfg.get("forma", {})
    if not fc.get("account_id"):
        return None

    token = forma_get_token(config)
    if not token:
        return None

    headers  = {"Authorization": f"Bearer {token}"}
    hub_info = None
    offset   = 0

    # Find the user in the hub-level user list
    while True:
        response = http_requests.get(
            f"https://developer.api.autodesk.com/hq/v1/accounts/{fc['account_id']}/users",
            headers=headers, params={"limit": 100, "offset": offset}, timeout=15
        )
        if response.status_code != 200:
            print(f"[Forma] Hub users fetch failed: {response.status_code}")
            break
        batch = response.json()
        if not batch: break
        for u in batch:
            if u.get("email", "").lower() == email.lower():
                hub_info = {
                    "found":       True,
                    "name":        u.get("name", ""),
                    "email":       u.get("email", ""),
                    "hub_role":    u.get("role", ""),
                    "hub_status":  u.get("status", ""),
                    "company":     u.get("company_name") or u.get("company", ""),
                    "job_title":   u.get("job_title", ""),
                    "last_sign_in": u.get("last_sign_in", ""),
                    "user_id":     u.get("id", ""),
                    "autodesk_id": u.get("uid", ""),
                    "projects":    [],
                }
                break
        if hub_info: break
        offset += len(batch)
        if len(batch) < 100: break

    if not hub_info:
        return None

    # Check project membership — only within registry-allowed projects
    def check_forma_project(project_id):
        po = 0
        while True:
            response = http_requests.get(
                f"https://developer.api.autodesk.com/construction/admin/v1/projects/{project_id}/users",
                headers=headers, params={"limit": 200, "offset": po}, timeout=10
            )
            if response.status_code != 200: break
            batch = response.json().get("results", [])
            if not batch: break
            for u in batch:
                if u.get("email", "").lower() == email.lower():
                    access = [k for k, v in u.get("accessLevels", {}).items() if v] if isinstance(u.get("accessLevels"), dict) else []
                    return {
                        "project_id":    project_id,
                        "status":        u.get("status", ""),
                        "access_levels": ", ".join(access) if access else "member",
                        "added_on":      u.get("addedOn", ""),
                    }
            po += len(batch)
            if len(batch) < 200: break
        return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(check_forma_project, allowed_project_ids))
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
    users[email] = {
        "password":    hash_password(p1),
        "created":     datetime.now().isoformat(),
        "cockpit_id":  str(uuid.uuid4()),    # globally unique admin ID, surfaced on /profile and home
        "first_name":  "",                   # populated later by editing admins.json (Round B)
    }
    save_users(users)
    resp = JSONResponse({"ok": True})
    resp.set_cookie("session", make_session(email), httponly=True, samesite="lax")
    return resp


def get_user_record(email: str) -> dict:
    """Return the user record for `email`, lazily backfilling missing
    fields (cockpit_id, first_name) so old accounts gain them on first
    access. Idempotent — fields are only set if missing, and only one
    save is performed per call regardless of how many fields backfilled.
    """
    if not email:
        return {}
    users = load_users()
    rec = users.get(email)
    if not rec:
        return {}

    changed = False
    if not rec.get("cockpit_id"):
        rec["cockpit_id"] = str(uuid.uuid4())
        changed = True
    if "first_name" not in rec:
        # Empty string, not a derived guess — first_name is owner-controlled.
        # Leaving it "" makes get_user_first_name() fall through to the
        # email-derived default, so the welcome line still works.
        rec["first_name"] = ""
        changed = True

    if changed:
        users[email] = rec
        save_users(users)
    return rec


def get_user_first_name(email: str, rec: dict | None = None) -> str:
    """Resolve the admin's first name for the welcome line.

    Order of preference:
      1. user record's `first_name` field (set manually in admins.json)
      2. parse from email local-part — split on '.' and take the first chunk
         (e.g. omar.awad@alec.ae → 'Omar', oawad@alec.ae → 'Oawad')

    `rec` is an optional pre-fetched user record. Passing it avoids a
    redundant load_users() call when the caller already has the record
    in hand (e.g. home/profile routes that call get_user_record first).
    """
    if not email:
        return ""
    if rec is None:
        rec = get_user_record(email)
    if rec.get("first_name"):
        return rec["first_name"].strip().title()
    local = email.split("@")[0]
    first = local.split(".")[0] if "." in local else local
    return first.capitalize()

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

    # ── Resolve admin metadata for the redesigned home header ─────────────
    user_rec   = get_user_record(user)
    first_name = get_user_first_name(user, rec=user_rec)
    cockpit_id = user_rec.get("cockpit_id", "")

    # ── Scope strip counts (registry-driven) ─────────────────────────────
    registry = load_registry()
    if is_owner(user):
        scoped_registry = registry
    else:
        scoped_registry = {k: v for k, v in registry.items() if user in v.get("admins", [])}

    morta_hubs = {
        p.get("platforms", {}).get("morta", {}).get("hub_id")
        for p in scoped_registry.values()
        if p.get("platforms", {}).get("morta", {}).get("hub_id")
    }

    # ── Platform-members counts for the active project (sidebar selection) ─
    # The JS in home.html calls /api/project/{key}/members on page load
    # and on project change. That endpoint returns cached counts instantly
    # and refreshes in the background. So we don't need to do any server-side
    # member-count work here — pass empty values and the JS fills them in.
    plat_members = {"morta": None, "revizto": None, "forma": None}

    return templates.TemplateResponse(request=request, name="home.html", context={
        "active_page": "home", "config": config,
        "morta_count": None, "revizto_count": None, "forma_count": None,
        "active_project":      ctx.get("active_project_name", ""),
        "active_project_key":  ctx.get("active_project_key", ""),    # used by JS to fetch member counts on page load
        "morta_hub":      config["morta"]["hub_id"][:12]      + "..." if config["morta"]["hub_id"]      else None,
        "revizto_region": config["revizto_dubai"]["region"].title() if config["revizto_dubai"].get("region") else None,
        "forma_account":  config["forma"]["account_id"][:12]  + "..." if config["forma"]["account_id"] else None,
        "recent_invites": recent_invites, "insights": [],
        "current_user":   user,
        "first_name":     first_name,
        "cockpit_id":     cockpit_id,
        "scope":          {"hubs": len(morta_hubs), "projects": len(scoped_registry)},
        "plat_members":   plat_members,
    })


# ── Profile page — admin info + allocated projects ────────────────────────
@app.get("/profile", response_class=HTMLResponse)
async def profile(request: Request, user: str = Depends(require_user)):
    config = load_config(user)

    # Status: Morta API ready + at least one Revizto region ready.
    # Returns {kind: 'ok'|'action_needed'|'error', detail, morta, revizto}.
    # Probe takes 1–3s; never raises (errors wrap into kind='error').
    # Pass `email` so it reads per-admin Revizto tokens, matching the
    # naming scheme used by load_revizto_tokens()/save_revizto_tokens().
    status = check_admin_status(config, email=user)

    # Resolve admin metadata from admins.json (cockpit_id, created date, etc.)
    user_rec    = get_user_record(user)
    first_name  = get_user_first_name(user, rec=user_rec)
    cockpit_id  = user_rec.get("cockpit_id", "")
    member_since = user_rec.get("created", "")
    if member_since:
        # Format as "Aug 12, 2024" if it's an ISO timestamp
        try:
            dt = datetime.fromisoformat(member_since)
            member_since = dt.strftime("%b %d, %Y")
        except (ValueError, TypeError):
            pass

    # Allocated projects: registry is keyed by project id, each entry has
    # {name, platforms: {morta,revizto,forma}, admins: [emails]}.
    # Owner sees all; admins see only registry entries where they're listed.
    registry = load_registry()
    if is_owner(user):
        user_registry = registry
    else:
        user_registry = {k: v for k, v in registry.items() if user in v.get("admins", [])}

    projects = []
    for proj_id, proj in user_registry.items():
        if not isinstance(proj, dict):
            continue
        # Inject the registry key as `id` so the template can show it.
        projects.append({"id": proj_id, **proj})

    # Activity counts — derived from invite_log scoped to this admin's
    # registry projects. invite_log doesn't track "who sent the invite",
    # so we can't count "invites I personally sent" — instead we count
    # all invite events (admin-driven + portal-submitted) on projects
    # this user admins. For the owner, that's all log entries.
    log = load_invite_log() or []
    if is_owner(user):
        scoped_log = log
    else:
        scoped_log = [e for e in log if e.get("project_key") in user_registry]
    invite_events_alltime = len(scoped_log)

    # Hubs admin count = unique Morta hubs across the user's registry projects.
    morta_hubs = {
        p.get("platforms", {}).get("morta", {}).get("hub_id")
        for p in user_registry.values()
        if p.get("platforms", {}).get("morta", {}).get("hub_id")
    }
    hubs_admin = len(morta_hubs)

    return templates.TemplateResponse(request=request, name="profile.html", context={
        "active_page":  "profile",
        "current_user": user,
        "profile": {
            "name":         first_name,
            "role":         "Owner" if is_owner(user) else "Project Admin",
            "cockpit_id":   cockpit_id,
            "member_since": member_since or "—",
            "last_seen":    datetime.now().strftime("%Y-%m-%d %H:%M"),
            "timezone":     "Asia/Dubai (UTC+4)",
            "mfa":          "Not configured",
            "scope":        {"hubs": hubs_admin},
            "activity": {
                "events":      invite_events_alltime,
                "extractions": 0,
            },
            "status": status,
        },
        "projects": projects,
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

    # Save submission
    existing = _load_portal_requests()
    key = project_id or "unassigned"
    if key not in existing:
        existing[key] = {
            "project_id":   project_id,
            "project_name": reg_name,
            "submissions":  []
        }
    existing[key]["submissions"].append({
        "submitted_by_name":  submitter_name,
        "submitted_by_email": submitter_email,
        "submitted_at":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "users":              users,
    })
    _save_portal_requests(existing)

    # ── Email notifications to project admins ─────────────────────────────
    registry     = load_registry()
    proj_entry   = registry.get(key, {})
    admin_emails = proj_entry.get("admins", [])
    proj_name    = reg_name or project_id

    if admin_emails and ACS_CONNECTION_STRING:
        base_url      = str(request.base_url).rstrip("/")
        manage_url    = f"{base_url}/manage"
        morta_count   = sum(1 for u in users if u.get("morta"))
        revizto_count = sum(1 for u in users if u.get("revizto"))
        forma_count   = sum(1 for u in users if u.get("forma"))

        html = f"""
        <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
            <div style="background:#0f1b2d;padding:20px 24px;border-radius:8px 8px 0 0;">
                <h2 style="color:#e8edf2;margin:0;font-size:18px;">New Platform Access Request</h2>
                <p style="color:#8099b3;margin:4px 0 0;font-size:13px;">{proj_name}</p>
            </div>
            <div style="background:#fff;border:1px solid #e0e6ed;border-top:none;padding:24px;border-radius:0 0 8px 8px;">
                <p style="color:#2d3748;font-size:14px;margin-top:0;">
                    A new access request has been submitted and requires your review.
                </p>
                <table style="width:100%;border-collapse:collapse;margin-bottom:20px;">
                    <tr style="background:#f5f7fa;">
                        <td style="padding:10px 14px;font-size:12px;color:#6b7280;text-transform:uppercase;letter-spacing:0.5px;font-weight:600;">Submitted by</td>
                        <td style="padding:10px 14px;font-size:14px;color:#0f1b2d;font-weight:500;">{submitter_name} &lt;{submitter_email}&gt;</td>
                    </tr>
                    <tr>
                        <td style="padding:10px 14px;font-size:12px;color:#6b7280;text-transform:uppercase;letter-spacing:0.5px;font-weight:600;">Project</td>
                        <td style="padding:10px 14px;font-size:14px;color:#0f1b2d;font-weight:500;">{proj_name}</td>
                    </tr>
                    <tr style="background:#f5f7fa;">
                        <td style="padding:10px 14px;font-size:12px;color:#6b7280;text-transform:uppercase;letter-spacing:0.5px;font-weight:600;">Users requested</td>
                        <td style="padding:10px 14px;font-size:14px;color:#0f1b2d;font-weight:500;">{len(users)} user(s)</td>
                    </tr>
                    <tr>
                        <td style="padding:10px 14px;font-size:12px;color:#6b7280;text-transform:uppercase;letter-spacing:0.5px;font-weight:600;">Platforms</td>
                        <td style="padding:10px 14px;font-size:14px;color:#0f1b2d;">
                            {"<span style='background:#e8f0fe;color:#1a56db;padding:2px 8px;border-radius:3px;font-size:12px;margin-right:6px;'>Morta: " + str(morta_count) + "</span>" if morta_count else ""}
                            {"<span style='background:#e8f5e9;color:#1b7a34;padding:2px 8px;border-radius:3px;font-size:12px;margin-right:6px;'>Revizto: " + str(revizto_count) + "</span>" if revizto_count else ""}
                            {"<span style='background:#fff3e0;color:#b45309;padding:2px 8px;border-radius:3px;font-size:12px;'>Forma: " + str(forma_count) + "</span>" if forma_count else ""}
                        </td>
                    </tr>
                </table>
                <a href="{manage_url}" style="display:inline-block;background:#0f1b2d;color:#e8edf2;padding:12px 24px;border-radius:6px;text-decoration:none;font-size:14px;font-weight:600;">
                    Review in Cockpit →
                </a>
                <p style="color:#9ca3af;font-size:12px;margin-top:20px;margin-bottom:0;">
                    Digital Construction Cockpit · ALEC Holdings
                </p>
            </div>
        </div>
        """

        send_email(admin_emails, f"[Cockpit] New access request — {proj_name} ({len(users)} users)", html)

    return JSONResponse({"status": "success", "total": len(users)})


@app.post("/portal/platformAccess/submit/single")
async def portal_submit_single(request: Request):
    """Public endpoint — single user access request from the portal.
    Saves into portal_requests.json as a pending submission (same structure as bulk)
    so admins can review and process it through the same Manage Users flow.
    """
    body = await request.json()

    submitter_name  = body.get("submitter_name", "").strip()
    submitter_email = body.get("submitter_email", "").strip()
    project_id      = body.get("project_id", "").strip()
    email           = body.get("email", "").strip()

    if not email:
        return JSONResponse({"status": "error", "detail": "Email is required."}, status_code=400)
    if not project_id:
        return JSONResponse({"status": "error", "detail": "No project reference in link."}, status_code=400)

    registry    = load_registry()
    reg_project = registry.get(project_id, {})
    reg_name    = reg_project.get("name", project_id)

    user_entry = {
        "email":               email,
        "first_name":          body.get("first_name", "").strip(),
        "last_name":           body.get("last_name", "").strip(),
        "company":             body.get("company", "").strip(),
        "department":          body.get("department", "").strip(),
        "morta":               bool(body.get("morta", True)),
        "revizto":             bool(body.get("revizto", True)),
        "forma":               bool(body.get("forma", True)),
        "project_id":          project_id,
        "project_name":        reg_name,
        "submitted_by_name":   submitter_name,
        "submitted_by_email":  submitter_email,
        "submitted_at":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status":              "pending",
    }

    existing = _load_portal_requests()
    if project_id not in existing:
        existing[project_id] = {
            "project_id":   project_id,
            "project_name": reg_name,
            "submissions":  []
        }
    existing[project_id]["submissions"].append({
        "submitted_by_name":  submitter_name,
        "submitted_by_email": submitter_email,
        "submitted_at":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "users":              [user_entry],
    })
    _save_portal_requests(existing)

    # Email notification to project admins
    admin_emails = reg_project.get("admins", [])
    if admin_emails and ACS_CONNECTION_STRING:
        base_url   = str(request.base_url).rstrip("/")
        manage_url = f"{base_url}/manage"
        platforms_html = "".join([
            "<span style='background:#e8f0fe;color:#1a56db;padding:2px 8px;border-radius:3px;font-size:12px;margin-right:6px;'>Morta</span>" if user_entry["morta"] else "",
            "<span style='background:#e8f5e9;color:#1b7a34;padding:2px 8px;border-radius:3px;font-size:12px;margin-right:6px;'>Revizto</span>" if user_entry["revizto"] else "",
            "<span style='background:#fff3e0;color:#b45309;padding:2px 8px;border-radius:3px;font-size:12px;'>Forma</span>" if user_entry["forma"] else "",
        ])
        html = f"""
        <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
            <div style="background:#0f1b2d;padding:20px 24px;border-radius:8px 8px 0 0;">
                <h2 style="color:#e8edf2;margin:0;font-size:18px;">New Platform Access Request</h2>
                <p style="color:#8099b3;margin:4px 0 0;font-size:13px;">{reg_name}</p>
            </div>
            <div style="background:#fff;border:1px solid #e0e6ed;border-top:none;padding:24px;border-radius:0 0 8px 8px;">
                <p style="color:#2d3748;font-size:14px;margin-top:0;">
                    A new single-user access request has been submitted and requires your review.
                </p>
                <table style="width:100%;border-collapse:collapse;margin-bottom:20px;">
                    <tr style="background:#f5f7fa;">
                        <td style="padding:10px 14px;font-size:12px;color:#6b7280;text-transform:uppercase;letter-spacing:0.5px;font-weight:600;">Submitted by</td>
                        <td style="padding:10px 14px;font-size:14px;color:#0f1b2d;font-weight:500;">{submitter_name} &lt;{submitter_email}&gt;</td>
                    </tr>
                    <tr>
                        <td style="padding:10px 14px;font-size:12px;color:#6b7280;text-transform:uppercase;letter-spacing:0.5px;font-weight:600;">Project</td>
                        <td style="padding:10px 14px;font-size:14px;color:#0f1b2d;font-weight:500;">{reg_name}</td>
                    </tr>
                    <tr style="background:#f5f7fa;">
                        <td style="padding:10px 14px;font-size:12px;color:#6b7280;text-transform:uppercase;letter-spacing:0.5px;font-weight:600;">User</td>
                        <td style="padding:10px 14px;font-size:14px;color:#0f1b2d;font-weight:500;">{email}{f" ({user_entry['first_name']} {user_entry['last_name']})" if user_entry.get('first_name') else ""}</td>
                    </tr>
                    <tr>
                        <td style="padding:10px 14px;font-size:12px;color:#6b7280;text-transform:uppercase;letter-spacing:0.5px;font-weight:600;">Platforms</td>
                        <td style="padding:10px 14px;font-size:14px;color:#0f1b2d;">{platforms_html}</td>
                    </tr>
                </table>
                <a href="{manage_url}" style="display:inline-block;background:#0f1b2d;color:#e8edf2;padding:12px 24px;border-radius:6px;text-decoration:none;font-size:14px;font-weight:600;">
                    Review in Cockpit →
                </a>
                <p style="color:#9ca3af;font-size:12px;margin-top:20px;margin-bottom:0;">
                    Digital Construction Cockpit · ALEC Holdings
                </p>
            </div>
        </div>
        """
        send_email(
            admin_emails,
            f"[Cockpit] New access request — {reg_name} (1 user)",
            html,
        )

    return JSONResponse({"status": "success"})
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
    cached = _cache_get("portal_requests.json")
    if cached is not None:
        return cached

    if USE_BLOB:
        data = blob_read("portal_requests.json")
        if data is not None:
            result = data if isinstance(data, dict) else {}
            _cache_set("portal_requests.json", result)
            return result
    p = BASE_DIR / "portal_requests.json"
    if p.exists():
        with open(p) as f:
            loaded = json.load(f)
        result = loaded if isinstance(loaded, dict) else {}
        _cache_set("portal_requests.json", result)
        return result
    _cache_set("portal_requests.json", {})
    return {}

def _save_portal_requests(data: dict):
    if USE_BLOB:
        blob_write("portal_requests.json", data)
    else:
        with open(BASE_DIR / "portal_requests.json", "w") as f:
            json.dump(data, f, indent=2)
    _cache_invalidate("portal_requests.json")


@app.get("/api/pending")
async def api_pending_list(user: str = Depends(require_user)):
    """Return submissions only for projects the user is admin of."""
    data          = _load_portal_requests()
    owner_view    = is_owner(user)
    user_projects = get_user_projects(user) if not owner_view else None

    submissions = []
    for key, project in data.items():
        # Owner sees everything, others see only their registry projects
        if not owner_view and key not in (user_projects or []):
            continue
        for idx, sub in enumerate(project.get("submissions", [])):
            users = sub.get("users", [])
            statuses = [u.get("status", "pending") for u in users]
            if any(s == "pending" for s in statuses):
                status = "pending"
            elif all(s == "rejected" for s in statuses):
                status = "rejected"
            elif any(s == "error" for s in statuses):
                status = "partial"
            else:
                status = "processed"
            submissions.append({
                "_key":            key,
                "_idx":            idx,
                "project_id":      project.get("project_id", key),
                "project_name":    project.get("project_name", ""),
                "submitter_name":  sub.get("submitted_by_name", ""),
                "submitter_email": sub.get("submitted_by_email", ""),
                "submitted_at":    sub.get("submitted_at", ""),
                "total":           len(users),
                "status":          status,
            })
    submissions.sort(key=lambda s: s.get("submitted_at",""), reverse=True)
    return JSONResponse({"submissions": submissions})


@app.get("/api/pending/{project_key}/{sub_idx}")
async def api_pending_get(project_key: str, sub_idx: int, user: str = Depends(require_user)):
    """Get a specific submission for review."""
    registry = load_registry()
    if not is_owner(user) and user not in registry.get(project_key, {}).get("admins", []):
        raise HTTPException(status_code=403, detail="You are not an admin of this project.")
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
    """Process approved users with configured access parameters."""
    # Access check — owner sees all, admins only their own projects
    registry = load_registry()
    if not is_owner(user) and user not in registry.get(project_key, {}).get("admins", []):
        raise HTTPException(status_code=403, detail="You are not an admin of this project.")

    body     = await request.json()
    users    = body.get("users", [])
    access   = body.get("access", {})

    success = 0; errors = 0; rejected = 0
    config   = load_config(user)
    registry = load_registry()
    proj     = registry.get(project_key, {})
    platforms = proj.get("platforms", {})

    morta_hub_id     = platforms.get("morta", {}).get("hub_id", "")
    revizto_uuid     = platforms.get("revizto", {}).get("project_uuid", "")
    forma_project_id = platforms.get("forma", {}).get("project_id", "")

    morta_tag_id         = access.get("morta_tag_id", "")
    revizto_license_role = access.get("revizto_license_role_id")
    revizto_project_role = access.get("revizto_project_role_id")
    revizto_to_license   = access.get("revizto_invite_license", True)
    revizto_to_project   = access.get("revizto_invite_project", False)
    forma_company_id     = access.get("forma_company_id", "")
    forma_role_ids       = access.get("forma_role_ids", [])
    forma_products       = access.get("forma_products", [{"key": "docs", "access": "member"}])

    for u in users:
        if not u.get("approved", True):
            rejected += 1; u["status"] = "rejected"; continue

        u_access         = u.get("access_override", {})
        u_morta_tag      = u_access.get("morta_tag_id", morta_tag_id)
        u_rev_lic_role   = u_access.get("revizto_license_role_id", revizto_license_role)
        u_rev_proj_role  = u_access.get("revizto_project_role_id", revizto_project_role)
        u_rev_to_license = u_access.get("revizto_invite_license", revizto_to_license)
        u_rev_to_project = u_access.get("revizto_invite_project", revizto_to_project)
        u_forma_company  = u_access.get("forma_company_id", forma_company_id)
        u_forma_roles    = u_access.get("forma_role_ids", forma_role_ids)
        u_forma_products = u_access.get("forma_products", forma_products)

        email = u.get("email",""); first_name = u.get("first_name","")
        last_name = u.get("last_name",""); company = u.get("company",""); department = u.get("department","")
        err_parts = []

        try:
            if u.get("morta"):
                r = morta_invite(email, config, hub_id=morta_hub_id, tag_reference_id=u_morta_tag)
                if r["status"] != "success": err_parts.append(f"Morta: {r.get('detail','failed')}")

            if u.get("revizto"):
                r = revizto_invite(email, first_name, last_name, company, department, config,
                                   project_uuid=revizto_uuid, license_role_id=u_rev_lic_role,
                                   project_role_id=u_rev_proj_role, invite_to_license=u_rev_to_license,
                                   invite_to_project=u_rev_to_project, admin_email=user)
                if r["status"] != "success": err_parts.append(f"Revizto: {r.get('detail','failed')}")

            if u.get("forma"):
                r = forma_invite(email, config, project_id=forma_project_id,
                                 company_id=u_forma_company,
                                 role_ids=u_forma_roles if u_forma_roles else None,
                                 products=u_forma_products)
                if r["status"] != "success": err_parts.append(f"Forma: {r.get('detail','failed')}")

            if err_parts:
                u["status"] = "error"; u["error"] = " | ".join(err_parts); errors += 1
            else:
                u["status"] = "processed"; success += 1
        except Exception as e:
            u["status"] = "error"; u["error"] = str(e); errors += 1

    data = _load_portal_requests()
    if project_key in data and sub_idx < len(data[project_key].get("submissions", [])):
        data[project_key]["submissions"][sub_idx]["users"] = users
    _save_portal_requests(data)

    append_activity("portal_decision", project_key=project_key, actor=user,
                    sub_idx=sub_idx, success=success, errors=errors, rejected=rejected,
                    user_count=len(users))

    return JSONResponse({"success": success, "errors": errors, "rejected": rejected})


@app.delete("/api/pending/{project_key}/{sub_idx}")
async def api_pending_delete(project_key: str, sub_idx: int, user: str = Depends(require_user)):
    """Delete a submission."""
    registry = load_registry()
    if not is_owner(user) and user not in registry.get(project_key, {}).get("admins", []):
        raise HTTPException(status_code=403, detail="You are not an admin of this project.")
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
    """Return registry projects scoped to the logged-in user.
    Owner sees all projects. Admins only see projects they are listed in.
    """
    registry = load_registry()
    if is_owner(user):
        return JSONResponse(registry)
    # Non-owners only get their own projects
    user_registry = {k: v for k, v in registry.items() if user in v.get("admins", [])}
    return JSONResponse(user_registry)

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
    append_activity("registry_edit", project_key=key, actor=user,
                    action="create", project_name=registry[key]["name"])
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
    append_activity("registry_edit", project_key=key, actor=user,
                    action="update", project_name=registry[key]["name"])
    return JSONResponse({"status": "ok"})

@app.delete("/api/registry/project/{key}")
async def api_registry_delete(key: str, user: str = Depends(require_owner)):
    registry = load_registry()
    if key in registry:
        deleted_name = registry[key].get("name", key)
        del registry[key]
        save_registry(registry)
        append_activity("registry_edit", project_key=key, actor=user,
                        action="delete", project_name=deleted_name)
    return JSONResponse({"status": "ok"})


@app.post("/api/registry/project/{key}/sync")
async def api_registry_sync(key: str, request: Request, user: str = Depends(require_user)):
    """Fetch and store tags/roles/companies. Owner can sync any project; admins only their own."""
    registry = load_registry()
    if key not in registry:
        raise HTTPException(status_code=404, detail="Project not found.")
    # Check access: owner or project admin
    if not is_owner(user) and user not in registry[key].get("admins", []):
        raise HTTPException(status_code=403, detail="Not an admin of this project.")

    results = _sync_project_metadata(user, key, registry)
    save_registry(registry)
    append_activity("metadata_sync", project_key=key, actor=user,
                    scope="single", project_name=registry[key].get("name", key),
                    synced=results)
    return JSONResponse({"status": "ok", "synced": results})


@app.post("/api/registry/sync-mine")
async def api_registry_sync_mine(request: Request, user: str = Depends(require_user)):
    """Sync platform metadata across every project the caller has access to.

    Owner: all projects. Admin: only projects where they're listed in `admins`.

    Each project is synced sequentially (predictable rate against the platforms,
    simple error handling). Within a project, the sync helper runs Morta /
    Revizto / Forma fetches in series too — they share the same registry write
    target and there's no benefit to parallelising across platforms here.

    Revizto roles are cached per region during one sync run: the role list is
    license-scoped, so two projects in the same region share the same roles.
    Without this cache we'd fetch identical data N times for N projects.

    Returns a per-project results map so the UI can show what synced and what
    didn't. A failure on one project does not abort the others.
    """
    registry = load_registry()
    if is_owner(user):
        keys = list(registry.keys())
    else:
        keys = [k for k, v in registry.items() if user in v.get("admins", [])]

    revizto_roles_cache: dict = {}    # region -> list[dict] | "error message"
    revizto_member_cache: dict = {}   # region -> {companies, departments, locations}

    per_project = {}
    for key in keys:
        try:
            per_project[key] = {
                "name":   registry[key].get("name", key),
                "synced": _sync_project_metadata(
                    user, key, registry,
                    revizto_roles_cache, revizto_member_cache
                ),
            }
        except Exception as e:
            # Keep going — one project's failure shouldn't block the others
            per_project[key] = {
                "name":  registry[key].get("name", key),
                "error": str(e),
            }

    save_registry(registry)
    append_activity("metadata_sync", project_key=None, actor=user,
                    scope="batch", project_count=len(keys))
    return JSONResponse({
        "status":      "ok",
        "project_count": len(keys),
        "results":     per_project,
    })


def _forma_derive_roles_from_users(account_id: str, clean_pid: str, headers: dict) -> dict:
    """Aggregate Forma project roles from existing project members.

    Used as a fallback when the canonical `industry_roles` endpoints all
    fail. The /construction/admin/v1/projects/{pid}/users endpoint is known
    to work for 2L tokens (the cockpit uses it for searching users today).

    Returns:
        {"roles": [{id, name}, ...], "note": str, "shape_seen": str}

    "note" is non-empty when the result is empty — explains why so the caller
    can surface a useful error. "shape_seen" describes which field carried
    role data (varies by API version).

    Pagination cap: 5 pages × 200 users = 1000 users scanned. Beyond that we
    stop and return whatever was found. Real projects max out around 300
    users, so this is generous.
    """
    role_index = {}    # id -> {id, name}  (dedup by id)
    shape_seen = set()
    pages_scanned = 0
    users_scanned = 0
    last_status = None

    for offset in range(0, 5 * 200, 200):
        r = http_requests.get(
            f"https://developer.api.autodesk.com/construction/admin/v1/projects/{clean_pid}/users",
            headers=headers,
            params={"limit": 200, "offset": offset},
            timeout=15,
        )
        last_status = r.status_code
        if r.status_code != 200:
            break

        try:
            batch = r.json().get("results", [])
        except Exception:
            break
        if not batch:
            break

        pages_scanned += 1
        users_scanned += len(batch)

        for u in batch:
            # Try multiple shapes. ACC variants:
            # - "roles": [{"id": "...", "name": "..."}]   (full pairs — best)
            # - "roleIds": ["..."]                        (ids only — useless without names)
            # - "industryRoles": [{"id": ..., "name": ..}] (alt shape)
            for field in ("roles", "industryRoles"):
                items = u.get(field)
                if isinstance(items, list) and items:
                    for ro in items:
                        if not isinstance(ro, dict):
                            continue
                        rid  = ro.get("id") or ro.get("roleId") or ""
                        name = ro.get("name") or ""
                        if rid and name and rid not in role_index:
                            role_index[rid] = {"id": rid, "name": name}
                            shape_seen.add(field)

        if len(batch) < 200:
            break

    roles = list(role_index.values())
    note = ""
    if not roles:
        if last_status is None:
            note = "users endpoint never reached"
        elif last_status != 200:
            note = f"users endpoint returned {last_status}"
        elif users_scanned == 0:
            note = "no users in project"
        else:
            note = f"{users_scanned} users scanned, no role names found in any of the expected fields"
    return {
        "roles":      roles,
        "note":       note,
        "shape_seen": ",".join(sorted(shape_seen)) or "none",
        "pages":      pages_scanned,
        "users":      users_scanned,
    }


def _sync_project_metadata(user: str, key: str, registry: dict,
                           revizto_roles_cache: dict = None,
                           revizto_member_cache: dict = None) -> dict:
    """Fetch fresh metadata from Morta/Revizto/Forma for one project and write
    it into the registry entry in place.

    Mutates `registry[key]['platforms']`. Caller is responsible for
    `save_registry(registry)` once all desired keys are synced — batching
    the save is faster than writing once per project.

    `revizto_roles_cache` (optional) memoises Revizto license-roles fetches
    across projects in a single batch sync. Keys are region names; values are
    either a list of role dicts (success) or an error string. Pass None for
    one-off syncs of a single project.

    `revizto_member_cache` (optional) memoises Revizto project-team
    suggestion fetches across projects in a single batch sync. Keys are
    project UUIDs (not regions — each project has its own team). Values
    are dicts of {companies, departments, locations} suggestion lists
    sourced from /v5/project/{uuid}/team. Pass None for one-off syncs.

    Returns a flat results dict reporting counts on success and error strings
    on failure, so the UI can show a per-platform status. Partial success is
    expected — if the admin doesn't have Forma access for this account, the
    Morta and Revizto fields still get refreshed.

    Auth strategy:
    - Morta: caller's API key, fall back to owner's if caller has none
    - Revizto: caller's per-region access token (refreshed if needed)
    - Forma: caller's 2-legged token, fall back to owner's account_id if missing
    """
    proj      = registry[key]
    platforms = proj.get("platforms", {})
    config    = load_config(user)
    results   = {}

    # ── Morta tags ────────────────────────────────────────────────────────
    hub_id  = platforms.get("morta", {}).get("hub_id", "")
    api_key = config["morta"]["api_key"]
    if not api_key:
        api_key = load_config(OWNER_EMAIL).get("morta", {}).get("api_key", "")
    if hub_id and api_key:
        try:
            headers  = {"Authorization": f"Bearer {api_key}"}
            response = http_requests.get(
                f"https://api.morta.io/v1/hub/{hub_id}/tags",
                headers=headers, timeout=10
            )
            if response.status_code == 200:
                raw_tags = response.json().get("data", [])
                tags = []
                for tag_group in raw_tags:
                    for cell in tag_group.get("cells", []):
                        if "value" in cell and "id" in cell:
                            tags.append({"name": cell["value"], "tag_reference_id": cell["id"]})
                platforms["morta"]["tags"] = tags
                results["morta_tags"] = len(tags)
            else:
                results["morta_tags_error"] = f"{response.status_code}: {response.text[:120]}"
        except Exception as e:
            results["morta_error"] = str(e)

    # ── Revizto roles + license roles ─────────────────────────────────────
    # Revizto's role concept is license-scoped — the same role list applies
    # across every project under that license/region. The endpoint is
    # /license/{license_uuid}/role/list, NOT /project/{uuid}/role/list
    # (which doesn't exist and returns 404). We resolve license UUID via
    # the existing helper, fetch once per region, and reuse across projects
    # via revizto_roles_cache.
    rev_uuid   = platforms.get("revizto", {}).get("project_uuid", "")
    rev_region = platforms.get("revizto", {}).get("region", "dubai")
    # Always store the safe license roles (no License Admin)
    platforms.setdefault("revizto", {})["license_roles"] = [
        {"id": 1, "name": "Guest"},
        {"id": 2, "name": "Collaborator"},
        {"id": 3, "name": "Content Creator"},
    ]
    if rev_uuid:
        try:
            cache = revizto_roles_cache if revizto_roles_cache is not None else {}
            cached = cache.get(rev_region)
            if isinstance(cached, list):
                # Reuse the role list fetched earlier in this batch
                platforms["revizto"]["roles"] = cached
                results["revizto_roles"] = len(cached)
            elif isinstance(cached, str):
                # Earlier project in same region failed; surface the same error
                results["revizto_roles_error"] = cached
            else:
                token = revizto_get_token_for_region(config, rev_region, email=user)
                if not token:
                    err = f"No valid Revizto token for {rev_region} — enter your access code in Settings first"
                    cache[rev_region] = err
                    results["revizto_error"] = err
                else:
                    base_url = f"https://api.{rev_region}.revizto.com/v5"
                    headers  = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

                    # Fetch ALL licenses for this user/region — not just the first.
                    # Different licenses on the same account can have different
                    # role-list permissions. Revizto returns 200 OK with
                    # `result: -20` ("resource server rejected the request")
                    # when the token doesn't admin a particular license, even
                    # though the auth itself is valid. The fix is to iterate
                    # licenses until one returns roles successfully.
                    lr = http_requests.get(f"{base_url}/user/licenses", headers=headers, timeout=10)
                    licenses = []
                    if lr.status_code == 200:
                        try:
                            licenses = lr.json().get("data", {}).get("entities", []) or []
                        except Exception:
                            licenses = []

                    if not licenses:
                        err = f"Could not list user licenses for {rev_region}"
                        cache[rev_region] = err
                        results["revizto_roles_error"] = err
                    else:
                        roles = []
                        used_license = ""
                        attempts = []
                        for lic in licenses:
                            lic_uuid = lic.get("uuid", "")
                            if not lic_uuid:
                                continue
                            r = http_requests.get(
                                f"{base_url}/license/{lic_uuid}/role/list",
                                headers=headers, timeout=10
                            )
                            if r.status_code != 200:
                                attempts.append(f"{lic_uuid[:8]}=HTTP{r.status_code}")
                                continue
                            try:
                                body = r.json()
                            except Exception:
                                attempts.append(f"{lic_uuid[:8]}=non-JSON")
                                continue

                            # Revizto error envelope: result < 0 means application
                            # error despite the 200 OK. Skip this license.
                            if body.get("result", 0) < 0 or body.get("data") is None:
                                msg = body.get("message", "no data")
                                attempts.append(f"{lic_uuid[:8]}=result_{body.get('result')}({msg[:40]})")
                                continue

                            raw = body.get("data", {})
                            if isinstance(raw, dict):
                                parsed = [{"id": v.get("id"), "uuid": v.get("uuid", ""), "name": v.get("name", "")} for v in raw.values()]
                            elif isinstance(raw, list):
                                parsed = [{"id": v.get("id"), "uuid": v.get("uuid", ""), "name": v.get("name", "")} for v in raw]
                            else:
                                attempts.append(f"{lic_uuid[:8]}=unexpected_data_type({type(raw).__name__})")
                                continue

                            if parsed:
                                roles = parsed
                                used_license = lic_uuid
                                break
                            else:
                                attempts.append(f"{lic_uuid[:8]}=empty")

                        if roles:
                            platforms["revizto"]["roles"] = roles
                            results["revizto_roles"] = len(roles)
                            cache[rev_region] = roles
                            # Cache the working license back so future calls skip
                            # the loop. Stored in in-memory config; not persisted.
                            config.setdefault(f"revizto_{rev_region}", {})["license_uuid"] = used_license
                        else:
                            err = f"No roles in any of {len(licenses)} license(s): " + " | ".join(attempts)
                            cache[rev_region] = err
                            results["revizto_roles_error"] = err
        except Exception as e:
            results["revizto_error"] = str(e)

        # ── Project team suggestions (companies / depts / locations) ──
        # SEPARATE try-block from roles so a member-suggestion fetch
        # failure can't mask role success. Per-project endpoint
        # (/v5/project/{uuid}/team) returns each member's company,
        # department, location — exactly the autocomplete data the
        # invite form needs. Project-scoped: each project has its
        # own team, so cached per project_uuid (not per region).
        try:
            mcache = revizto_member_cache if revizto_member_cache is not None else {}
            if rev_uuid in mcache:
                # Already fetched for this project earlier in the batch
                cached_sug = mcache[rev_uuid]
                if isinstance(cached_sug, dict):
                    platforms["revizto"]["member_suggestions"] = cached_sug
                    results["revizto_suggestions"] = (
                        len(cached_sug.get("companies",   []))
                        + len(cached_sug.get("departments", []))
                        + len(cached_sug.get("locations",  []))
                    )
            else:
                token = revizto_get_token_for_region(config, rev_region, email=user)
                if token:
                    sug = revizto_fetch_project_team_suggestions(rev_uuid, rev_region, token)
                    sug_clean = {
                        "companies":   sug.get("companies",   []),
                        "departments": sug.get("departments", []),
                        "locations":   sug.get("locations",   []),
                    }
                    platforms["revizto"]["member_suggestions"] = sug_clean
                    mcache[rev_uuid] = sug_clean
                    results["revizto_suggestions"] = (
                        len(sug_clean["companies"])
                        + len(sug_clean["departments"])
                        + len(sug_clean["locations"])
                    )
                    if sug.get("_error"):
                        results["revizto_suggestions_error"] = sug["_error"]
                    if "_member_count" in sug:
                        results["revizto_member_count"] = sug["_member_count"]
        except Exception as e:
            results["revizto_suggestions_error"] = f"exception: {type(e).__name__}: {str(e)[:120]}"

    # ── Forma companies + roles ───────────────────────────────────────────
    forma_pid  = platforms.get("forma", {}).get("project_id", "")
    account_id = config["forma"].get("account_id", "")
    if not account_id:
        owner_forma = load_config(OWNER_EMAIL).get("forma", {})
        account_id  = owner_forma.get("account_id", "")
    if forma_pid and account_id:
        try:
            token   = forma_get_token(config)
            headers = {"Authorization": f"Bearer {token}"}
            clean_pid = forma_pid.lstrip("b.") if forma_pid.startswith("b.") else forma_pid
            # Companies — already known to work
            r = http_requests.get(
                f"https://developer.api.autodesk.com/hq/v1/accounts/{account_id}/projects/{clean_pid}/companies",
                headers=headers, params={"limit": 100}, timeout=15
            )
            if r.status_code == 200:
                companies = [{"id": c["id"], "name": c["name"]} for c in r.json()]
                platforms["forma"]["companies"] = companies
                results["forma_companies"] = len(companies)
            else:
                results["forma_companies_error"] = f"{r.status_code}: {r.text[:100]}"

            # Roles — try multiple URL shapes. The /construction/admin paths
            # 404 for 2L tokens (those are user-scoped endpoints). The HQ
            # namespace works for 2L because it's account-level. The BIM 360
            # heritage of ACC means roles live at `industry_roles` in HQ.
            roles_found = False
            roles_attempts = []
            for label, roles_url in [
                ("hq_v2",   f"https://developer.api.autodesk.com/hq/v2/accounts/{account_id}/projects/{clean_pid}/industry_roles"),
                ("hq_v1",   f"https://developer.api.autodesk.com/hq/v1/accounts/{account_id}/projects/{clean_pid}/industry_roles"),
                ("adm_v2",  f"https://developer.api.autodesk.com/construction/admin/v2/projects/{clean_pid}/roles"),
                ("adm_v1",  f"https://developer.api.autodesk.com/construction/admin/v1/projects/{clean_pid}/roles"),
            ]:
                r = http_requests.get(roles_url, headers=headers, timeout=15)
                if r.status_code == 200:
                    raw = r.json()
                    # HQ industry_roles returns a list. Construction admin
                    # might return {"results": [...]} or a list.
                    role_list = raw.get("results", raw) if isinstance(raw, dict) else raw
                    if not isinstance(role_list, list):
                        roles_attempts.append(f"{label}=200 unexpected_shape({type(role_list).__name__})")
                        continue
                    roles = [{"id": ro.get("id", ""), "name": ro.get("name", "")} for ro in role_list if ro.get("name")]
                    if roles:
                        platforms["forma"]["roles"] = roles
                        results["forma_roles"] = len(roles)
                        roles_found = True
                        break
                    roles_attempts.append(f"{label}=200 empty")
                else:
                    roles_attempts.append(f"{label}={r.status_code}")

            # Fallback: derive role list by aggregating roles from existing
            # project members. Triggered only if all canonical endpoints
            # above failed. The construction/admin/v1/projects/{pid}/users
            # endpoint is known to work for 2L tokens (the cockpit uses it
            # for searching users by email today).
            #
            # Limitation: this only finds roles assigned to at least one
            # user. A role defined but not yet used will be missed. For
            # a populated project this is rarely an issue.
            if not roles_found:
                try:
                    derived = _forma_derive_roles_from_users(
                        account_id, clean_pid, headers
                    )
                    if derived["roles"]:
                        platforms["forma"]["roles"] = derived["roles"]
                        results["forma_roles"] = len(derived["roles"])
                        results["forma_roles_source"] = "derived_from_users"
                        roles_found = True
                    else:
                        roles_attempts.append(f"derive_from_users={derived['note']}")
                except Exception as e:
                    roles_attempts.append(f"derive_from_users=exception({e})")

            if not roles_found:
                results["forma_roles_error"] = " | ".join(roles_attempts)
        except Exception as e:
            results["forma_error"] = str(e)

    proj["platforms"] = platforms
    registry[key]     = proj
    return results


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
    config = load_config(user)
    return templates.TemplateResponse(request=request, name="manage.html", context={
        "active_page":    "manage",
        "current_user":   user,
        "active_context": config.get("active_context", {}),
    })

@app.get("/scheduler", response_class=HTMLResponse)
async def scheduler_page(request: Request, user: str = Depends(require_user)):
    return templates.TemplateResponse(request=request, name="scheduler.html", context={
        "active_page": "scheduler", "current_user": user,
    })

@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request,
                        forma_3l_connected: str = "",
                        forma_3l_error: str = "",
                        user: str = Depends(require_user)):
    config = load_config(user)
    # If we just landed back from the Forma OAuth flow, surface a banner.
    # Errors are mapped to readable text — Autodesk's error codes alone
    # aren't very helpful for end users.
    message = None
    message_type = None
    if forma_3l_connected:
        message = "Forma account connected. 3-legged extractions can now run with your identity."
        message_type = "success"
    elif forma_3l_error:
        ERR_MAP = {
            "no_client_id":       "Forma 3-legged client ID is not configured. Ask the system owner to add it.",
            "no_credentials":     "Forma 3-legged credentials are missing or incomplete.",
            "missing_code_or_state": "Authorization callback was malformed.",
            "invalid_state":      "Authorization session expired or could not be verified. Try connecting again.",
            "exchange_failed":    "Could not reach Autodesk to exchange the authorization code.",
            "exchange_bad_json":  "Autodesk returned an unexpected response. Try connecting again.",
            "no_access_token":    "Autodesk responded but no access token was provided.",
            "access_denied":      "You denied the authorization request.",
        }
        message = ERR_MAP.get(forma_3l_error)
        if not message:
            # Pass through token-exchange status codes (e.g. "exchange_400") raw
            if forma_3l_error.startswith("exchange_"):
                code = forma_3l_error.replace("exchange_", "")
                message = f"Autodesk rejected the token exchange (HTTP {code}). Check your client ID/secret and registered callback URL."
            else:
                message = f"Forma authorization failed: {forma_3l_error}"
        message_type = "error"
    return templates.TemplateResponse(request=request, name="settings.html", context={
        "active_page": "settings", "config": config,
        "message": message, "message_type": message_type,
        "current_user": user, "is_owner": is_owner(user),
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
    # Preserve the per-admin 3-legged token block if it exists. Settings save
    # rebuilds the forma{} dict from form fields, so without this anyone who
    # saved Settings after connecting Forma would lose their tokens.
    existing_forma   = existing.get("forma", {}) or {}
    existing_session = existing_forma.get("3l_session") or existing_forma.get("forma_3l_session")
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
    if existing_session:
        config["forma"]["3l_session"] = existing_session
    save_config(config, user)
    return templates.TemplateResponse(request=request, name="settings.html", context={
        "active_page": "settings", "config": config,
        "message": "Settings saved successfully.", "message_type": "success",
        "current_user": user, "is_owner": is_owner(user),
    })

@app.get("/extractions", response_class=HTMLResponse)
async def extractions_page(request: Request, user: str = Depends(require_user)):
    return templates.TemplateResponse(request=request, name="extractions.html", context={
        "active_page": "extractions", "current_user": user,
    })


# ═══════════════════════════════════════════════════════════════════════════
# Forma 3-legged OAuth flow
# ═══════════════════════════════════════════════════════════════════════════
#
# Standard OAuth 2.0 authorization code flow with PKCE. Each admin can
# connect their own Autodesk identity to the cockpit, granting Forma
# scripts access to their resources without manual token caching.
#
# Flow:
#   1. /auth/forma/start  → server redirects browser to Autodesk's consent page
#   2. Admin authenticates with Autodesk, approves scopes
#   3. Autodesk redirects back to /auth/forma/callback?code=...
#   4. Server exchanges code for {access_token, refresh_token, expires_at}
#   5. Server stores tokens in this admin's config under forma.3l_session
#
# Token refresh happens lazily — when a Forma script needs the token, the
# cockpit checks expiry and refreshes via /auth/forma/refresh (internal
# helper) before launching the subprocess. (That part lands in Round 2.)
#
# Per-admin: each admin has their own forma.3l_session block. Owner's tokens
# don't grant other admins access — Forma's permissions are user-scoped.
#
# Required APS app config: the redirect URI registered with Autodesk MUST
# match what we send. We compute it from request.base_url so it works in
# both prod and dev — but the URL must be explicitly added to the APS app's
# Callback URL list at https://aps.autodesk.com/myapps.

FORMA_OAUTH_SCOPES = "data:read data:write"
FORMA_OAUTH_AUTHORIZE_URL = "https://developer.api.autodesk.com/authentication/v2/authorize"
FORMA_OAUTH_TOKEN_URL     = "https://developer.api.autodesk.com/authentication/v2/token"

# In-memory state map for OAuth: {state_token: {user, code_verifier, created_at}}.
# Used to defend against CSRF and to look up the PKCE verifier on callback.
# Entries auto-expire after 10 minutes — the consent flow shouldn't take
# longer than that. Worker-restart wipes the map; an admin mid-flow during
# a restart would just re-click Connect.
_FORMA_OAUTH_STATES: dict = {}
_FORMA_OAUTH_STATE_TTL = 600  # seconds

def _forma_redirect_uri(request: Request) -> str:
    """Build the absolute callback URI that Autodesk will redirect to.
    Honors X-Forwarded-Proto (Azure App Service uses HTTPS termination)."""
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host   = request.headers.get("host", request.url.hostname or "localhost")
    return f"{scheme}://{host}/auth/forma/callback"

def _forma_3l_creds(user: str) -> tuple[str, str]:
    """Return (client_id_3l, client_secret_3l) for this admin, falling back to owner's."""
    cfg = load_config(user).get("forma", {}) or {}
    cid = cfg.get("client_id_3l") or ""
    sec = cfg.get("client_secret_3l") or ""
    if not cid or not sec:
        owner_cfg = load_config(OWNER_EMAIL).get("forma", {}) or {}
        cid = cid or owner_cfg.get("client_id_3l") or ""
        sec = sec or owner_cfg.get("client_secret_3l") or ""
    return cid, sec

def _forma_save_session(user: str, token_data: dict):
    """Persist the OAuth token block onto the admin's config.

    token_data shape (from Autodesk's /token response):
      access_token, refresh_token, expires_in, token_type, ...
    """
    import time as _time
    cfg = load_config(user)
    expires_in = int(token_data.get("expires_in") or 3600)
    cfg.setdefault("forma", {})["3l_session"] = {
        "access_token":  token_data.get("access_token", ""),
        "refresh_token": token_data.get("refresh_token", ""),
        "expires_at":    int(_time.time()) + expires_in - 30,  # 30s safety margin
        "scope":         token_data.get("scope", ""),
        "obtained_at":   int(_time.time()),
    }
    save_config(cfg, user)

def _forma_clear_session(user: str):
    cfg = load_config(user)
    if "3l_session" in cfg.get("forma", {}):
        cfg["forma"].pop("3l_session", None)
        save_config(cfg, user)


@app.get("/auth/forma/start")
async def auth_forma_start(request: Request, user: str = Depends(require_user)):
    """Initiate the Forma 3-legged OAuth flow.

    Redirects the browser to Autodesk's authorization page with a state
    token that ties the eventual callback back to this admin.
    """
    import secrets, hashlib, base64, time as _time

    cid, _sec = _forma_3l_creds(user)
    if not cid:
        # No 3-legged client_id configured — bounce back to settings with an error
        return RedirectResponse(url="/settings?forma_3l_error=no_client_id", status_code=302)

    # Generate state + PKCE verifier+challenge
    state    = secrets.token_urlsafe(24)
    verifier = secrets.token_urlsafe(48)  # 64 chars, in OAuth's required 43-128 range
    challenge_bytes = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(challenge_bytes).decode("ascii").rstrip("=")

    # Stash state → user mapping so callback can verify CSRF + look up verifier
    # Opportunistically prune expired entries to keep the dict small.
    now = int(_time.time())
    for stale_state, meta in list(_FORMA_OAUTH_STATES.items()):
        if now - meta.get("created_at", 0) > _FORMA_OAUTH_STATE_TTL:
            _FORMA_OAUTH_STATES.pop(stale_state, None)

    _FORMA_OAUTH_STATES[state] = {
        "user":          user,
        "code_verifier": verifier,
        "created_at":    now,
    }

    redirect_uri = _forma_redirect_uri(request)
    auth_url = (
        f"{FORMA_OAUTH_AUTHORIZE_URL}"
        f"?response_type=code"
        f"&client_id={urllib.parse.quote(cid)}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri)}"
        f"&scope={urllib.parse.quote(FORMA_OAUTH_SCOPES)}"
        f"&state={urllib.parse.quote(state)}"
        f"&code_challenge={challenge}"
        f"&code_challenge_method=S256"
    )
    return RedirectResponse(url=auth_url, status_code=302)


@app.get("/auth/forma/callback")
async def auth_forma_callback(request: Request, code: str = "", state: str = "",
                              error: str = "", error_description: str = ""):
    """Handle the redirect back from Autodesk's authorization page.

    Validates the state token, exchanges the code for tokens, persists the
    session block on the originating admin's config. Then redirects back
    to /settings with a success or error flag.
    """
    # If Autodesk reported an error directly, surface it
    if error:
        return RedirectResponse(
            url=f"/settings?forma_3l_error={urllib.parse.quote(error)}",
            status_code=302,
        )
    if not code or not state:
        return RedirectResponse(url="/settings?forma_3l_error=missing_code_or_state", status_code=302)

    # Validate state — must match an entry in our in-memory map
    state_meta = _FORMA_OAUTH_STATES.pop(state, None)  # one-shot use
    if not state_meta:
        return RedirectResponse(url="/settings?forma_3l_error=invalid_state", status_code=302)

    user = state_meta["user"]
    verifier = state_meta["code_verifier"]

    cid, sec = _forma_3l_creds(user)
    if not cid or not sec:
        return RedirectResponse(url="/settings?forma_3l_error=no_credentials", status_code=302)

    redirect_uri = _forma_redirect_uri(request)

    # Exchange authorization code for tokens. Autodesk's v2 token endpoint
    # accepts client credentials via Basic auth (preferred) or in the form body.
    import base64
    basic = base64.b64encode(f"{cid}:{sec}".encode("ascii")).decode("ascii")

    try:
        # Run the blocking HTTP request in a worker thread so we don't
        # block the event loop while talking to Autodesk
        loop = asyncio.get_event_loop()
        def _exchange():
            return http_requests.post(
                FORMA_OAUTH_TOKEN_URL,
                headers={
                    "Authorization": f"Basic {basic}",
                    "Content-Type":  "application/x-www-form-urlencoded",
                    "Accept":        "application/json",
                },
                data={
                    "grant_type":    "authorization_code",
                    "code":          code,
                    "redirect_uri":  redirect_uri,
                    "code_verifier": verifier,
                },
                timeout=20,
            )
        resp = await loop.run_in_executor(None, _exchange)
    except Exception as e:
        print(f"[forma-oauth] Token exchange request failed: {e}")
        return RedirectResponse(url="/settings?forma_3l_error=exchange_failed", status_code=302)

    if resp.status_code != 200:
        # Log the body for debugging (it includes Autodesk's specific error)
        print(f"[forma-oauth] Token exchange returned {resp.status_code}: {resp.text[:300]}")
        return RedirectResponse(
            url=f"/settings?forma_3l_error=exchange_{resp.status_code}",
            status_code=302,
        )

    try:
        token_data = resp.json()
    except ValueError:
        return RedirectResponse(url="/settings?forma_3l_error=exchange_bad_json", status_code=302)

    if not token_data.get("access_token"):
        return RedirectResponse(url="/settings?forma_3l_error=no_access_token", status_code=302)

    _forma_save_session(user, token_data)
    return RedirectResponse(url="/settings?forma_3l_connected=1", status_code=302)


@app.post("/auth/forma/disconnect")
async def auth_forma_disconnect(user: str = Depends(require_user)):
    """Clear this admin's stored Forma 3-legged session."""
    _forma_clear_session(user)
    return JSONResponse({"status": "ok"})


@app.get("/api/auth/forma/status")
async def auth_forma_status(user: str = Depends(require_user)):
    """Report whether this admin has a stored Forma 3-legged session, and
    when it expires. Used by the Settings UI to show the connection pill."""
    import time as _time
    cfg = load_config(user).get("forma", {}) or {}
    sess = cfg.get("3l_session") or {}
    if not sess.get("access_token"):
        return JSONResponse({"connected": False})
    expires_at = int(sess.get("expires_at") or 0)
    now = int(_time.time())
    seconds_remaining = max(0, expires_at - now)
    refresh_present = bool(sess.get("refresh_token"))
    return JSONResponse({
        "connected":          True,
        "access_expires_in":  seconds_remaining,
        "refresh_present":    refresh_present,
        "scope":              sess.get("scope", ""),
        "obtained_at":        sess.get("obtained_at"),
    })


@app.get("/packages", response_class=HTMLResponse)
async def packages_page(request: Request, user: str = Depends(require_user)):
    """Packages page — shows baked extractions visible to this admin."""
    return templates.TemplateResponse(request=request, name="packages.html", context={
        "active_page": "packages", "current_user": user,
    })

# ---------------------------------------------------------------------------
# Script config builder — merges admin credentials + registry project context
# ---------------------------------------------------------------------------

def build_script_config(user: str) -> dict:
    """Build the merged config dict that scripts should see when run by this admin.

    Merge order (later wins):
      1. Default config structure
      2. Admin's own blob config (personal api_key, access_code, active_context)
      3. Forma shared credentials from owner config (if admin has none)
      4. Project-level IDs injected from registry for the active project key
    """
    config = load_config(user)

    # ── Forma shared credentials fallback ──────────────────────────────────
    fc = config.get("forma", {})
    if not fc.get("client_id") or not fc.get("client_secret"):
        owner_fc = load_config(OWNER_EMAIL).get("forma", {})
        config.setdefault("forma", {}).update({
            k: owner_fc.get(k, "")
            for k in ["client_id", "client_secret", "client_id_3l",
                      "client_secret_3l", "account_id", "admin_user_id"]
            if owner_fc.get(k)
        })

    # ── Inject registry project IDs into active_context ───────────────────
    ctx = config.get("active_context", {})
    project_key = ctx.get("active_project_key", "")
    if project_key:
        registry = load_registry()
        reg_proj = registry.get(project_key, {})
        platforms = reg_proj.get("platforms", {})

        # Only inject if the values aren't already set from the sidebar selection
        if platforms.get("morta", {}).get("hub_id") and not ctx.get("morta_hub_id"):
            ctx["morta_hub_id"] = platforms["morta"]["hub_id"]
        if platforms.get("revizto", {}).get("project_uuid") and not ctx.get("revizto_project_uuid"):
            ctx["revizto_project_uuid"] = platforms["revizto"]["project_uuid"]
            ctx["revizto_region"]       = platforms["revizto"].get("region", "dubai")
        if platforms.get("forma", {}).get("project_id") and not ctx.get("forma_project_id"):
            ctx["forma_project_id"] = platforms["forma"]["project_id"]

        config["active_context"] = ctx

        # Also mirror IDs into top-level platform blocks so scripts that read
        # config["morta"]["hub_id"] directly (not via active_context) get the right value
        if ctx.get("morta_hub_id"):
            config.setdefault("morta", {})["hub_id"] = ctx["morta_hub_id"]
        if ctx.get("revizto_project_uuid"):
            region = ctx.get("revizto_region", "dubai")
            config.setdefault(f"revizto_{region}", {})["project_uuid"] = ctx["revizto_project_uuid"]
        if ctx.get("forma_project_id"):
            config.setdefault("forma", {})["project_id"] = ctx["forma_project_id"]

    return config


def write_script_config(user: str, user_temp: Path) -> Path:
    """Write the merged script config to a temp JSON file and return its path.
    The file lives in the user's temp dir so it's cleaned up with other temp files.
    """
    merged = build_script_config(user)
    config_path = user_temp / "_cockpit_config.json"
    with open(config_path, "w") as f:
        json.dump(merged, f, indent=2)
    return config_path


# ═══════════════════════════════════════════════════════════════════════════
# Forma 3-legged token: refresh + per-admin file writer
# ═══════════════════════════════════════════════════════════════════════════
#
# Round 1 of this feature stored the OAuth session in the admin's blob
# config under forma.3l_session. Round 2 (this code) bridges that storage
# into the running script:
#
#   1. Before launching a Forma subprocess, we check if the admin has a
#      stored session
#   2. If it's expired (or expiring within 60s), we refresh it server-side
#      using the stored refresh_token and persist the new tokens
#   3. We write the (possibly-refreshed) access_token to a per-admin token
#      file in the admin's temp dir
#   4. We pass that file path to the subprocess via COCKPIT_FORMA_3L_TOKEN_FILE
#   5. The patched Forma_MasterScript.get_3legged_token() reads from that
#      env-var path instead of its hardcoded default — eliminating the race
#      condition where simultaneous extractions by different admins would
#      cross-contaminate each other's tokens via a shared file
#
# All of this is best-effort. If anything fails (refresh rejected, file
# write fails, etc.), the script falls back to its 2-legged path with a
# message in the terminal.

def _forma_refresh_3l_session(user: str, session: dict) -> dict | None:
    """Use the stored refresh_token to get a fresh access_token.

    Saves the new tokens to the admin's blob config (Autodesk rotates
    refresh_tokens, so we have to capture the new one). Returns the
    updated session dict, or None on failure.
    """
    import time as _time

    refresh_token = session.get("refresh_token") or ""
    if not refresh_token:
        return None

    cid, sec = _forma_3l_creds(user)
    if not cid or not sec:
        return None

    import base64
    basic = base64.b64encode(f"{cid}:{sec}".encode("ascii")).decode("ascii")

    try:
        resp = http_requests.post(
            FORMA_OAUTH_TOKEN_URL,
            headers={
                "Authorization": f"Basic {basic}",
                "Content-Type":  "application/x-www-form-urlencoded",
                "Accept":        "application/json",
            },
            data={
                "grant_type":    "refresh_token",
                "refresh_token": refresh_token,
            },
            timeout=15,
        )
    except Exception as e:
        print(f"[forma-3l] Refresh request failed: {e}")
        return None

    if resp.status_code != 200:
        print(f"[forma-3l] Refresh returned {resp.status_code}: {resp.text[:200]}")
        return None

    try:
        data = resp.json()
    except ValueError:
        return None

    if not data.get("access_token"):
        return None

    expires_in = int(data.get("expires_in") or 3600)
    new_session = {
        "access_token":  data["access_token"],
        # Autodesk rotates refresh tokens — use the new one if returned,
        # otherwise carry over the existing one.
        "refresh_token": data.get("refresh_token") or refresh_token,
        "expires_at":    int(_time.time()) + expires_in - 30,
        "scope":         data.get("scope", session.get("scope", "")),
        "obtained_at":   int(_time.time()),
    }

    # Persist the rotated refresh token + fresh access token back to blob
    cfg = load_config(user)
    cfg.setdefault("forma", {})["3l_session"] = new_session
    save_config(cfg, user)
    return new_session


def prepare_forma_3l_token(user: str, user_temp: Path) -> Path | None:
    """Best-effort: ensure a fresh 3-legged token is available on disk for
    the running subprocess. Returns the path to the token file, or None
    if the admin has no stored session (in which case the script will
    fall back to 2-legged on its own).

    Behavior:
      • No stored session   → return None (no file written)
      • Session valid       → write file, return path
      • Session expiring    → refresh, write file, return path
      • Refresh fails       → return None (script falls back to 2-legged)
    """
    import time as _time

    cfg = load_config(user)
    session = (cfg.get("forma", {}) or {}).get("3l_session")
    if not session or not session.get("access_token"):
        return None

    # Refresh if within 60s of expiry (or already past)
    expires_at = int(session.get("expires_at") or 0)
    if expires_at <= int(_time.time()) + 60:
        refreshed = _forma_refresh_3l_session(user, session)
        if not refreshed:
            return None
        session = refreshed

    # Write to user's temp dir using the same JSON shape Forma_MasterScript
    # expects (access_token, refresh_token, expires_at). We deliberately do
    # NOT use the script's default filename so it's clear the cockpit owns
    # this file. The patched get_3legged_token() picks up the path via
    # COCKPIT_FORMA_3L_TOKEN_FILE env var.
    token_path = user_temp / "_forma_3l_token.json"
    payload = {
        "access_token":  session["access_token"],
        "refresh_token": session.get("refresh_token", ""),
        "expires_at":    session.get("expires_at", 0),
    }
    try:
        with open(token_path, "w") as f:
            json.dump(payload, f)
    except OSError as e:
        print(f"[forma-3l] Failed to write token file: {e}")
        return None
    return token_path


def prepare_revizto_tokens(user: str, user_temp: Path) -> Path | None:
    """Best-effort: stage the admin's per-region Revizto access tokens to
    disk in the user's temp dir, so subprocess scripts that need to query
    multiple Revizto regions in one run can find them without going
    through Revizto_MasterScript (which is single-region by design).

    Mirrors the Forma 3-legged pattern (prepare_forma_3l_token):

      1. For each known region (dubai, ireland), call
         revizto_get_token_for_region() — which reads from blob, refreshes
         via refresh_token if needed, and falls back to access_code re-auth.
         This is exactly the auth flow the cockpit's own Revizto API calls
         use, so the token returned here is fresh and valid.
      2. If a token is obtained, write it to
         <user_temp>/_revizto_tokens_<region>.json with shape
         {"access_token": "..."} — same shape multi-region scripts expect.
      3. Skip regions where token acquisition fails (no access_code, expired
         refresh, etc.) — the script will see the file missing and skip
         that region with a clear log line.
      4. Return the user_temp path if we wrote at least one file, else None.

    The script picks up the directory via the COCKPIT_REVIZTO_TOKENS_DIR
    env var. Single-region Revizto scripts (the older ones that go through
    Revizto_MasterScript) are NOT affected — they continue to use whatever
    auth flow they always used.
    """
    config = load_config(user)
    wrote_any = False

    for region in ("dubai", "ireland"):
        try:
            token = revizto_get_token_for_region(config, region, email=user)
        except Exception as e:
            print(f"[revizto-tokens] {region}: token fetch raised {type(e).__name__}: {e}")
            continue
        if not token:
            # No access_code, refresh failed, or this region isn't configured
            # for this admin. Move on quietly — the script will skip this
            # region and report it via its own log line.
            continue
        token_path = user_temp / f"_revizto_tokens_{region}.json"
        try:
            with open(token_path, "w") as f:
                json.dump({"access_token": token}, f)
            wrote_any = True
        except OSError as e:
            print(f"[revizto-tokens] Failed to write {token_path.name}: {e}")
            continue

    return user_temp if wrote_any else None


@app.get("/api/l4/status")
async def api_l4_status(script: str, user: str = Depends(require_user)):
    """Tell the UI whether an L4 script is ready to run for the user's
    current active context. Used to gate Run buttons on report cards.

    Cache-Control: no-store — the answer depends on rapidly-changing
    state (active model, last-run records). Without this header, the
    browser may serve a stale response when the model is changed and
    the UI re-checks via the same URL within seconds.
    """
    if not script:
        return JSONResponse(
            {"ready": False, "reason": "no_script", "message": "No script specified."},
            headers={"Cache-Control": "no-store"},
        )
    return JSONResponse(
        _check_l4_dependency(user, script),
        headers={"Cache-Control": "no-store"},
    )


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

    # ── L4 dependency pre-flight ───────────────────────────────────────
    # If the script is a registered L4 (depends on another script's
    # output), refuse to run when the user's current selection doesn't
    # match what was last processed. The UI also gates the Run button
    # but this is the safety net.
    if script_name in L4_DEPENDENCIES:
        dep_check = _check_l4_dependency(user, script_name)
        if not dep_check.get("ready"):
            return JSONResponse({
                "status":  "error",
                "message": dep_check.get("message", "Dependency not satisfied."),
                "output":  [],
                "files":   [],
                "l4_blocked": True,
                "reason":     dep_check.get("reason"),
            }, status_code=409)

    # Resolve per-user temp dir and clean up stale ones opportunistically
    cleanup_stale_temp_dirs()
    user_temp = get_user_temp_dir(user)

    if project_uuid:
        config = load_config(user)
        if platform_key == "revizto":
            config.setdefault("revizto", {})["project_uuid"] = project_uuid
        elif platform_key == "forma":
            config.setdefault("forma", {})["project_id"] = project_uuid
        save_config(config, user)

    # Env vars passed to the subprocess:
    #   COCKPIT_USER        — the logged-in admin's email (for per-user token storage)
    #   COCKPIT_EXPORTS_DIR — where the script should write its output files
    #   COCKPIT_CONFIG      — path to the admin's merged config JSON (credentials + project context)
    config_path = write_script_config(user, user_temp)
    script_env = os.environ.copy()
    script_env["COCKPIT_USER"]        = user
    script_env["COCKPIT_EXPORTS_DIR"] = str(user_temp)
    script_env["COCKPIT_CONFIG"]      = str(config_path)
    # If this admin has a stored Forma 3-legged session, refresh if needed
    # and write the access_token to a per-admin file. Patched
    # Forma_MasterScript.get_3legged_token() reads this path. Best-effort:
    # if the token can't be prepared, the script falls back to 2-legged.
    forma_3l_path = prepare_forma_3l_token(user, user_temp)
    if forma_3l_path:
        script_env["COCKPIT_FORMA_3L_TOKEN_FILE"] = str(forma_3l_path)

    # Stage Revizto access tokens (per region) to disk for any script that
    # needs to query multiple regions in one run. The cockpit's normal
    # token storage is in blob (USE_BLOB), which subprocess scripts can't
    # reach. prepare_revizto_tokens reads from blob, refreshes if needed,
    # writes to user_temp, and returns the dir. Multi-region scripts pick
    # up the dir via COCKPIT_REVIZTO_TOKENS_DIR. Single-region scripts
    # (older ones using Revizto_MasterScript) are unaffected.
    revizto_tokens_dir = prepare_revizto_tokens(user, user_temp)
    if revizto_tokens_dir:
        script_env["COCKPIT_REVIZTO_TOKENS_DIR"] = str(revizto_tokens_dir)

    # Snapshot existing files' mtimes BEFORE the run so we can identify
    # files that were actually created/modified by THIS extraction.
    # Otherwise the response would include every file from every previous
    # run that's still in the temp dir, leading to incorrect Bake input.
    pre_run_mtimes = {}
    for ext in [".csv", ".json"]:
        for f in user_temp.glob(f"*{ext}"):
            if not f.name.startswith("_"):
                pre_run_mtimes[f.name] = f.stat().st_mtime

    try:
        loop = asyncio.get_event_loop()
        def run_script():
            return subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True, text=True, timeout=300,
                cwd=str(SCRIPT_DIRS[platform_key]),
                env=script_env,
            )
        result = await loop.run_in_executor(None, run_script)
        lines  = [l for l in (result.stdout + result.stderr).strip().splitlines() if l.strip()]
        saved  = any("saved" in l.lower() or "export" in l.lower() for l in lines)
        ok     = result.returncode == 0 or saved

        # Collect files that are new OR were modified during this run.
        # Exclude internal files (starting with _) like _cockpit_config.json.
        files = []
        for ext in [".csv", ".json"]:
            for f in sorted(user_temp.glob(f"*{ext}")):
                if f.name.startswith("_"):
                    continue
                cur_mtime = f.stat().st_mtime
                prev = pre_run_mtimes.get(f.name)
                # New file (no prev) OR modified after this run started
                if prev is None or cur_mtime > prev + 0.001:
                    files.append({
                        "filename": f.name,
                        "ext":      ext.lstrip("."),
                        "url":      f"/api/extractions/download/{f.name}",
                        "size_kb":  round(f.stat().st_size / 1024, 1),
                    })

        # If this script is a registered L4 prerequisite, record the run
        # so future L4 reports can validate the user's context against it.
        # No-op for scripts not in _L3_TRACKED_PREREQS.
        if ok:
            try:
                _stamp_l3_run(user, script_name, [f["filename"] for f in files])
            except Exception as e:
                print(f"[l4] Failed to stamp L3 run for {script_name}: {e}")

        # Log the extraction event to the activity feed
        try:
            cfg_ctx = (load_config(user).get("active_context") or {})
            append_activity(
                "extraction",
                project_key=cfg_ctx.get("active_project_key"),
                actor=user,
                script=script_name,
                platform=platform_key,
                status="success" if ok else "error",
                file_count=len(files),
            )
        except Exception:
            pass  # never block the response on logging

        return JSONResponse({
            "status":  "success" if ok else "error",
            "message": "Extraction complete." if ok else f"Script exited with code {result.returncode}.",
            "output":  lines,
            "files":   files,
        })
    except subprocess.TimeoutExpired:
        return JSONResponse({"status": "error", "message": "Timed out after 5 minutes.", "output": [], "files": []})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e), "output": [], "files": []})


# ═══════════════════════════════════════════════════════════════════════════
# /api/extract/stream — Server-Sent Events version of /api/extract
# ═══════════════════════════════════════════════════════════════════════════
#
# Same behavior as /api/extract above, but streams the script's stdout/stderr
# line-by-line as they arrive (rather than waiting for the script to finish
# and dumping everything at once). Used by the Extractions page terminal
# to give a live "VS Code"-like feel during a run.
#
# Wire format: text/event-stream
#   - Each line of output:        data: {"type":"line","text":"..."}\n\n
#   - Periodic heartbeat:         :keepalive\n\n   (SSE comment, no data)
#   - Final completion event:     event: done
#                                 data: {"status":"...","files":[...],...}\n\n
#
# Why an additive endpoint instead of replacing /api/extract:
#   - The scheduler / any future programmatic caller may rely on the batch
#     JSON shape — leaving it untouched eliminates regression risk
#   - Frontend opt-in: extractions.html switches to this; everywhere else
#     stays on the old endpoint
#
# Per-line latency: subprocess.Popen + readline + PYTHONUNBUFFERED=1 in the
# child env means each print() in the script flushes immediately. The
# StreamingResponse writes each yielded chunk to the wire on-the-fly.

@app.post("/api/extract/stream")
async def api_extract_stream(request: Request, user: str = Depends(require_user)):
    from fastapi.responses import StreamingResponse

    body         = await request.json()
    script_name  = body.get("script", "")
    project_uuid = body.get("project_uuid")

    # ── Validation (same checks as /api/extract; we just emit them as
    #    SSE events so the client renders them in the terminal). ────────
    if not script_name:
        async def _err():
            yield 'event: done\ndata: {"status":"error","message":"No script specified.","files":[]}\n\n'
        return StreamingResponse(_err(), media_type="text/event-stream")

    PLATFORM_MAP = {"Revizto": "revizto", "Morta": "morta", "Forma": "forma", "Proccesed": "forma"}
    platform_key = next((v for k, v in PLATFORM_MAP.items() if script_name.startswith(k)), None)
    if not platform_key:
        async def _err():
            payload = json.dumps({"status": "error", "message": f"Unknown platform for: {script_name}", "files": []})
            yield f'event: done\ndata: {payload}\n\n'
        return StreamingResponse(_err(), media_type="text/event-stream")

    script_path = SCRIPT_DIRS[platform_key] / f"{script_name}.py"
    if not script_path.exists():
        async def _err():
            payload = json.dumps({"status": "error", "message": f"Script not found: {script_name}.py", "files": []})
            yield f'event: done\ndata: {payload}\n\n'
        return StreamingResponse(_err(), media_type="text/event-stream")

    # ── L4 dependency pre-flight ───────────────────────────────────────
    # Same check as the batch endpoint, but the error is delivered via
    # an SSE `done` event so the live terminal sees it inline.
    if script_name in L4_DEPENDENCIES:
        dep_check = _check_l4_dependency(user, script_name)
        if not dep_check.get("ready"):
            err_msg = dep_check.get("message", "Dependency not satisfied.")
            async def _err_dep():
                payload = json.dumps({
                    "status":     "error",
                    "message":    err_msg,
                    "files":      [],
                    "l4_blocked": True,
                })
                yield f'event: done\ndata: {payload}\n\n'
            return StreamingResponse(_err_dep(), media_type="text/event-stream")

    # Resolve per-user temp dir and clean up stale ones opportunistically
    cleanup_stale_temp_dirs()
    user_temp = get_user_temp_dir(user)

    if project_uuid:
        config = load_config(user)
        if platform_key == "revizto":
            config.setdefault("revizto", {})["project_uuid"] = project_uuid
        elif platform_key == "forma":
            config.setdefault("forma", {})["project_id"] = project_uuid
        save_config(config, user)

    # Same env contract as /api/extract, plus PYTHONUNBUFFERED so the
    # script's print() flushes immediately instead of getting batched.
    config_path = write_script_config(user, user_temp)
    script_env = os.environ.copy()
    script_env["COCKPIT_USER"]        = user
    script_env["COCKPIT_EXPORTS_DIR"] = str(user_temp)
    script_env["COCKPIT_CONFIG"]      = str(config_path)
    script_env["PYTHONUNBUFFERED"]    = "1"
    # See /api/extract for full notes on the Forma 3-legged token plumbing.
    forma_3l_path = prepare_forma_3l_token(user, user_temp)
    if forma_3l_path:
        script_env["COCKPIT_FORMA_3L_TOKEN_FILE"] = str(forma_3l_path)
    # Same notes for Revizto multi-region token staging — see /api/extract.
    revizto_tokens_dir = prepare_revizto_tokens(user, user_temp)
    if revizto_tokens_dir:
        script_env["COCKPIT_REVIZTO_TOKENS_DIR"] = str(revizto_tokens_dir)

    # Snapshot existing files' mtimes for the "files produced by this run"
    # filter (same logic as /api/extract).
    pre_run_mtimes = {}
    for ext in [".csv", ".json"]:
        for f in user_temp.glob(f"*{ext}"):
            if not f.name.startswith("_"):
                pre_run_mtimes[f.name] = f.stat().st_mtime

    cwd_str = str(SCRIPT_DIRS[platform_key])

    async def event_stream():
        import time as _time

        # Use asyncio.create_subprocess_exec so stdout reads integrate with
        # the event loop. This is critical for keepalives during long quiet
        # periods (e.g. the script downloading a big properties.ldjson.gz
        # from S3) — we can `wait_for(readline, timeout)` and fall through
        # to keepalive logic when the timeout fires. The previous
        # subprocess.Popen + run_in_executor(readline) approach blocked
        # indefinitely on quiet stdout, never firing keepalives, and Azure
        # killed the SSE connection at ~230s.
        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-u", str(script_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                cwd=cwd_str,
                env=script_env,
            )
        except Exception as e:
            payload = json.dumps({"status": "error", "message": f"Failed to start: {e}", "files": []})
            yield f'event: done\ndata: {payload}\n\n'
            return

        last_heartbeat = _time.time()
        # Bumped from 5min → 10min. Big Forma model exports legitimately
        # take that long (the S3 properties download is the long pole).
        timeout_sec = 600
        start_ts = _time.time()
        collected_lines = []

        try:
            while True:
                # Hard cap: kill long-running processes
                if _time.time() - start_ts > timeout_sec:
                    try: proc.kill()
                    except Exception: pass
                    payload = json.dumps({"status": "error", "message": f"Timed out after {timeout_sec // 60} minutes.", "files": []})
                    yield f'event: done\ndata: {payload}\n\n'
                    return

                # Read a line with a short timeout. If the script is in a
                # quiet phase (downloading from S3, no stdout output), the
                # timeout fires and we fall through to keepalive.
                try:
                    line_bytes = await asyncio.wait_for(proc.stdout.readline(), timeout=5.0)
                except asyncio.TimeoutError:
                    # No new line in 5s. Emit a keepalive if it's been long
                    # enough since the last frame. SSE comments (lines starting
                    # with ':') are ignored by the client but keep the TCP
                    # connection alive across Azure's ~230s proxy idle limit.
                    if _time.time() - last_heartbeat > 15:
                        yield ":keepalive\n\n"
                        last_heartbeat = _time.time()
                    # Did the process exit while we were waiting?
                    if proc.returncode is not None:
                        break
                    continue

                if not line_bytes:
                    # Empty bytes from readline = real EOF
                    if proc.returncode is not None:
                        break
                    # Process still running but stdout closed (unusual). Wait
                    # for it to exit and break out.
                    await asyncio.sleep(0.1)
                    continue

                line = line_bytes.decode("utf-8", errors="replace")
                stripped = line.rstrip("\r\n")
                # Skip purely-blank lines so the terminal stays compact
                if stripped.strip():
                    collected_lines.append(stripped)
                    payload = json.dumps({"type": "line", "text": stripped})
                    yield f'data: {payload}\n\n'
                    last_heartbeat = _time.time()

            # Drain any final bytes the OS pipe still holds
            try:
                rest_bytes = await proc.stdout.read()
                if rest_bytes:
                    rest = rest_bytes.decode("utf-8", errors="replace")
                    for ln in rest.splitlines():
                        if ln.strip():
                            collected_lines.append(ln)
                            payload = json.dumps({"type": "line", "text": ln})
                            yield f'data: {payload}\n\n'
            except Exception:
                pass

            returncode = await proc.wait()
            saved = any("saved" in l.lower() or "export" in l.lower() for l in collected_lines)
            ok    = returncode == 0 or saved

            # Collect new/modified files (same logic as /api/extract)
            files = []
            for ext in [".csv", ".json"]:
                for f in sorted(user_temp.glob(f"*{ext}")):
                    if f.name.startswith("_"):
                        continue
                    cur_mtime = f.stat().st_mtime
                    prev = pre_run_mtimes.get(f.name)
                    if prev is None or cur_mtime > prev + 0.001:
                        files.append({
                            "filename": f.name,
                            "ext":      ext.lstrip("."),
                            "url":      f"/api/extractions/download/{f.name}",
                            "size_kb":  round(f.stat().st_size / 1024, 1),
                        })

            # Stamp L3 prerequisite run for L4 dependency tracking.
            # See _stamp_l3_run / L4_DEPENDENCIES for the full flow.
            if ok:
                try:
                    _stamp_l3_run(user, script_name, [f["filename"] for f in files])
                except Exception as e:
                    print(f"[l4] Failed to stamp L3 run for {script_name}: {e}")

            # Log the extraction event to the activity feed
            try:
                cfg_ctx = (load_config(user).get("active_context") or {})
                append_activity(
                    "extraction",
                    project_key=cfg_ctx.get("active_project_key"),
                    actor=user,
                    script=script_name,
                    platform=platform_key,
                    status="success" if ok else "error",
                    file_count=len(files),
                )
            except Exception:
                pass  # never block the stream on logging

            done_payload = json.dumps({
                "status":  "success" if ok else "error",
                "message": "Extraction complete." if ok else f"Script exited with code {returncode}.",
                "files":   files,
            })
            yield f'event: done\ndata: {done_payload}\n\n'

        except asyncio.CancelledError:
            # Client disconnected — kill the subprocess so we don't leak it.
            try: proc.kill()
            except Exception: pass
            raise
        except Exception as e:
            try: proc.kill()
            except Exception: pass
            payload = json.dumps({"status": "error", "message": f"Stream error: {e}", "files": []})
            yield f'event: done\ndata: {payload}\n\n'

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":  "no-cache, no-transform",
            "X-Accel-Buffering": "no",  # disables proxy buffering on nginx-style proxies
            "Connection":     "keep-alive",
        },
    )


@app.get("/api/extractions/download/{filename}")
async def api_extraction_download(filename: str, user: str = Depends(require_user)):
    """Stream a file from the user's temp folder as a browser download."""
    from fastapi.responses import FileResponse
    safe_name = Path(filename).name  # strip any path traversal
    if not safe_name.endswith((".csv", ".json")):
        raise HTTPException(status_code=400, detail="Only CSV and JSON files are supported.")
    file_path = get_user_temp_dir(user) / safe_name
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"{safe_name} not found. Try running the extraction again.")
    media_type = "text/csv" if safe_name.endswith(".csv") else "application/json"
    return FileResponse(
        path=file_path,
        filename=safe_name,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{safe_name}"'},
    )


@app.get("/api/extractions/files")
async def api_extractions_files(user: str = Depends(require_user)):
    """List files available in the user's temp folder (for widget builder)."""
    user_temp = get_user_temp_dir(user)
    files = []
    for ext in [".csv", ".json"]:
        for f in sorted(user_temp.glob(f"*{ext}")):
            if f.name.startswith("_"):
                continue
            files.append({
                "name":     f.stem,
                "filename": f.name,
                "ext":      ext.lstrip("."),
                "url":      f"/api/extractions/download/{f.name}",
                "size_kb":  round(f.stat().st_size / 1024, 1),
                "modified": datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
            })
    return JSONResponse({"files": files})


# ═══════════════════════════════════════════════════════════════════════════
# Packages — "baked" extractions, persistent across the 24h temp cleanup.
# ═══════════════════════════════════════════════════════════════════════════
#
# Flow:
#   1. Admin runs an extraction (existing /api/extract). Files land in
#      user's temp dir, where they expire after 24 hours.
#   2. If the run succeeds, the frontend enables the "Bake" button.
#   3. Admin clicks Bake → POST /api/extractions/bake. This:
#       - Generates a pkg_id
#       - Copies temp files into PACKAGES_DIR/{pkg_id}/
#       - Writes an entry to packages_index.json (blob-backed)
#   4. Packages are auto-deleted 30 days after baking.
#
# Storage:
#   PACKAGES_DIR = BASE_DIR / "packages" / "{pkg_id}/"
#       contains the actual files
#   packages_index.json (blob)
#       master index, one entry per pkg_id
#
# Visibility:
#   - Owner sees all packages
#   - Other admins see packages where:
#       a) they baked it themselves, OR
#       b) the package's project_key is in their registry-admin list
#   - Account-wide packages with no project_key (Forma L1) are excluded
#     from baking entirely (bake endpoint rejects them).

PACKAGES_DIR = BASE_DIR / "packages"
PACKAGES_DIR.mkdir(exist_ok=True)
PACKAGES_RETENTION_DAYS = 30

# Inline-preview guardrails. Files larger than this byte threshold can't be
# rendered inline — the browser would lock up and the server would risk
# OOM trying to read them. Big files always download fine (download is a
# stream from disk straight to the wire), it's only the in-memory parsing
# that's restricted.
PREVIEW_MAX_BYTES = 50 * 1024 * 1024  # 50 MB

# When scanning a CSV linearly to compute pagination, stop after this many
# data rows. Past this point we report a truncated preview rather than
# pinning the worker for tens of seconds. Most files are nowhere near
# this; only catastrophically wide datasets hit it.
PREVIEW_MAX_ROW_SCAN = 500_000

# Forma L1 scripts cannot be baked (per ALEC's call: account-wide data
# isn't useful as a per-admin shareable package).
FORMA_L1_SCRIPTS = {
    "Forma_GET_Account_Projects",
    "Forma_GET_Hub_Users",
    "Forma_GET_2L_Status",
    "Forma_GET_3L_Status",
}

# ═══════════════════════════════════════════════════════════════════════════
# L4 dependency tracking
# ═══════════════════════════════════════════════════════════════════════════
#
# L4 scripts (Reporting layer) consume the output of an L3 extraction and
# turn it into a domain-specific report. The hazard is silent staleness:
# if the user runs Model Properties for Model A, then changes the active
# selection to Model B and clicks the Health Report, the report would run
# against Model A's data while presenting itself as Model B's results.
#
# The fix has two parts:
#   1. After every L3 prerequisite run succeeds, stamp the user's config
#      with `forma.last_runs[<script>] = {context_key, context_value,
#      ran_at, files}`. This records "what was being processed when this
#      script last ran successfully".
#   2. When the user clicks Run on an L4, look up the dependency in
#      L4_DEPENDENCIES, read the recorded context_value, compare to the
#      current value at the same path. Match → proceed. Mismatch / never
#      run → refuse with a clear error.
#
# Adding new L4 reports is a one-line entry here. The cockpit handles the
# rest — UI gating in extractions.html reads from /api/l4/status, the
# extract endpoints enforce the same check before spawning subprocesses.

L4_DEPENDENCIES = {
    "Proccesed_Model_Properties": {
        "platform":           "forma",
        "requires":           "Forma_GET_Model_Properties",
        "context_path":       "forma.model_item_id",
        "context_label_path": "forma.model_name",
        "human_prereq":       "Model Properties",
        "human_thing":        "model",
    },
}

# Build the inverse: scripts that we should stamp on success because at
# least one L4 depends on them. Scripts not in this set don't need stamping.
_L3_TRACKED_PREREQS = {dep["requires"] for dep in L4_DEPENDENCIES.values()}


def _read_dotted_path(d: dict, path: str):
    """Walk a config dict by 'a.b.c' path. Returns '' on any miss."""
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict):
            return ""
        cur = cur.get(part, "")
    return cur if isinstance(cur, str) else (str(cur) if cur else "")


def _stamp_l3_run(user: str, script_name: str, files: list):
    """Record a successful L3 run on the user's config so dependent L4
    scripts can later check whether the user's currently-selected context
    still matches what was processed.

    No-op for scripts that aren't a known prerequisite of any L4."""
    if script_name not in _L3_TRACKED_PREREQS:
        return

    # Find the dependency entry that points at this script (only one for now,
    # but we look it up generically so multiple-L4-on-same-prereq is fine)
    dep = next((d for d in L4_DEPENDENCIES.values() if d["requires"] == script_name), None)
    if not dep:
        return

    cfg = load_config(user)
    context_value = _read_dotted_path(cfg, dep["context_path"])
    context_label = _read_dotted_path(cfg, dep["context_label_path"]) or context_value
    if not context_value:
        # Without a context value, we can't meaningfully stamp the run —
        # the L4 dependency check would be ambiguous. Skip silently.
        return

    cfg.setdefault("forma", {}).setdefault("last_runs", {})[script_name] = {
        "context_path":  dep["context_path"],
        "context_value": context_value,
        "context_label": context_label,
        "ran_at":        int(_time_for_cache.time()),
        "files":         list(files or []),
    }
    save_config(cfg, user)


def _check_l4_dependency(user: str, l4_script: str) -> dict:
    """Check whether the L4 script can run given the user's current state.

    Returns a dict with:
      ready:           bool
      reason:          str (one of 'ready', 'never_run', 'context_mismatch',
                            'no_context', 'unknown_script')
      message:         human-readable
      current_context: {value, label} of current selection
      prereq:          {script, ran_at, context_value, context_label, files}
                       — only populated if a previous run was recorded
    """
    dep = L4_DEPENDENCIES.get(l4_script)
    if not dep:
        return {
            "ready":   False,
            "reason":  "unknown_script",
            "message": f"No dependency configured for '{l4_script}'.",
        }

    cfg = load_config(user)
    current_value = _read_dotted_path(cfg, dep["context_path"])
    current_label = _read_dotted_path(cfg, dep["context_label_path"]) or current_value

    if not current_value:
        return {
            "ready":   False,
            "reason":  "no_context",
            "message": f"No active {dep['human_thing']} selected. Pick a {dep['human_thing']} above first.",
            "current_context": {"value": "", "label": ""},
        }

    last_runs = (cfg.get("forma", {}) or {}).get("last_runs", {}) or {}
    prereq    = last_runs.get(dep["requires"])

    if not prereq:
        return {
            "ready":   False,
            "reason":  "never_run",
            "message": (
                f"Run {dep['human_prereq']} for '{current_label}' first. "
                f"This report consumes its output."
            ),
            "current_context": {"value": current_value, "label": current_label},
            "prereq":          None,
        }

    if prereq.get("context_value") != current_value:
        prev_label = prereq.get("context_label") or prereq.get("context_value", "?")
        return {
            "ready":   False,
            "reason":  "context_mismatch",
            "message": (
                f"{dep['human_prereq']} was last run for '{prev_label}', but the active "
                f"{dep['human_thing']} is now '{current_label}'. Re-run "
                f"{dep['human_prereq']} for '{current_label}' before running this report."
            ),
            "current_context": {"value": current_value, "label": current_label},
            "prereq":          {
                "script":        dep["requires"],
                "context_value": prereq.get("context_value", ""),
                "context_label": prereq.get("context_label", ""),
                "ran_at":        prereq.get("ran_at"),
                "files":         prereq.get("files", []),
            },
        }

    # Match — ready to run.
    return {
        "ready":   True,
        "reason":  "ready",
        "message": f"{dep['human_prereq']} ready for '{current_label}'.",
        "current_context": {"value": current_value, "label": current_label},
        "prereq":          {
            "script":        dep["requires"],
            "context_value": prereq.get("context_value", ""),
            "context_label": prereq.get("context_label", ""),
            "ran_at":        prereq.get("ran_at"),
            "files":         prereq.get("files", []),
        },
    }


def load_packages_index() -> dict:
    """Load the package index (blob or local). Cached like other loaders."""
    cached = _cache_get("packages_index.json")
    if cached is not None:
        return cached

    if USE_BLOB:
        data = blob_read("packages_index.json")
        if data is not None:
            result = data if isinstance(data, dict) else {}
            _cache_set("packages_index.json", result)
            return result
    p = BASE_DIR / "packages_index.json"
    if p.exists():
        with open(p) as f:
            loaded = json.load(f)
        result = loaded if isinstance(loaded, dict) else {}
        _cache_set("packages_index.json", result)
        return result
    _cache_set("packages_index.json", {})
    return {}

def save_packages_index(data: dict):
    if USE_BLOB:
        blob_write("packages_index.json", data)
    else:
        with open(BASE_DIR / "packages_index.json", "w") as f:
            json.dump(data, f, indent=2)
    _cache_invalidate("packages_index.json")

def cleanup_expired_packages():
    """Delete packages older than PACKAGES_RETENTION_DAYS. Runs at startup."""
    import shutil
    from datetime import datetime as _dt, timedelta as _td

    index = load_packages_index()
    cutoff = _dt.now() - _td(days=PACKAGES_RETENTION_DAYS)
    removed = []
    for pkg_id, meta in list(index.items()):
        try:
            baked_at = _dt.strptime(meta.get("baked_at", ""), "%Y-%m-%d %H:%M:%S")
            if baked_at < cutoff:
                # Delete the files dir
                pkg_dir = PACKAGES_DIR / pkg_id
                if pkg_dir.exists():
                    shutil.rmtree(pkg_dir, ignore_errors=True)
                index.pop(pkg_id, None)
                removed.append(pkg_id)
        except (ValueError, KeyError):
            continue
    if removed:
        save_packages_index(index)
        print(f"[packages] Cleaned up {len(removed)} expired package(s)")

def _user_can_see_package(user: str, pkg: dict, registry: dict) -> bool:
    """Visibility rule for a single package."""
    if is_owner(user):
        return True
    if pkg.get("baked_by") == user:
        return True
    pkey = pkg.get("project_key")
    if pkey and user in (registry.get(pkey, {}).get("admins") or []):
        return True
    return False


@app.post("/api/extractions/bake")
async def api_bake(request: Request, user: str = Depends(require_user)):
    """Bake a successful extraction run into a permanent package.

    Body: {
        platform:     "morta" | "revizto" | "forma",
        level:        1 | 2 | 3,
        script:       "Morta_GET_members" (e.g.),
        label:        "Hub Members" (display name)
        project_key:  "rak-central" or "" if account-wide
        project_name: "RAK Central" or "" if account-wide
        files:        ["morta_members.csv", "morta_members.json"]
    }

    Files must currently exist in the user's temp directory. They're
    copied — not moved — so the run output stays inspectable until
    24-hour cleanup.
    """
    import shutil
    import secrets
    from datetime import datetime as _dt, timedelta as _td

    body = await request.json()
    platform     = body.get("platform", "")
    level        = body.get("level", 0)
    script       = body.get("script", "")
    label        = body.get("label", script)
    project_key  = body.get("project_key", "") or ""
    project_name = body.get("project_name", "") or ""
    files        = body.get("files") or []

    # Validate platform/script
    if platform not in ("morta", "revizto", "forma"):
        raise HTTPException(status_code=400, detail="Invalid platform.")
    if not script:
        raise HTTPException(status_code=400, detail="No script specified.")
    if not files:
        raise HTTPException(status_code=400, detail="No files to bake.")

    # Forma L1 cannot be baked
    if script in FORMA_L1_SCRIPTS:
        raise HTTPException(status_code=400, detail="Account-wide Forma extractions cannot be baked.")

    # Validate access — non-owners must be admin of the project_key (if set)
    if project_key and not is_owner(user):
        registry = load_registry()
        if user not in (registry.get(project_key, {}).get("admins") or []):
            raise HTTPException(status_code=403, detail="Not an admin of this project.")

    # Validate files exist in user's temp dir
    user_temp = get_user_temp_dir(user)
    src_paths = []
    for fname in files:
        # Defensive: only accept simple file names, no path traversal
        safe_name = Path(fname).name
        src = user_temp / safe_name
        if not src.exists() or not src.is_file():
            raise HTTPException(status_code=400, detail=f"File not in temp: {safe_name}")
        src_paths.append(src)

    # Generate pkg_id and copy files
    now = _dt.now()
    pkg_id = f"pkg_{now.strftime('%Y-%m-%d_%H-%M-%S')}_{secrets.token_hex(3)}"
    pkg_dir = PACKAGES_DIR / pkg_id
    pkg_dir.mkdir(parents=True, exist_ok=True)

    file_meta = []
    for src in src_paths:
        dst = pkg_dir / src.name
        shutil.copy2(src, dst)
        file_meta.append({
            "name":    src.name,
            "size_kb": round(dst.stat().st_size / 1024, 1),
        })

    expires_at = (now + _td(days=PACKAGES_RETENTION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    pkg_entry = {
        "id":           pkg_id,
        "baked_by":     user,
        "baked_at":     now.strftime("%Y-%m-%d %H:%M:%S"),
        "expires_at":   expires_at,
        "platform":     platform,
        "level":        int(level) if level else 0,
        "script":       script,
        "label":        label,
        "project_key":  project_key,
        "project_name": project_name,
        "files":        file_meta,
    }

    index = load_packages_index()
    index[pkg_id] = pkg_entry
    save_packages_index(index)

    append_activity("package_bake", project_key=project_key or None, actor=user,
                    pkg_id=pkg_id, platform=platform, script=script, label=label,
                    file_count=len(file_meta))

    return JSONResponse({"status": "ok", "pkg_id": pkg_id, "expires_at": expires_at})


@app.get("/api/packages")
async def api_packages_list(user: str = Depends(require_user)):
    """List packages this admin is allowed to see.

    Resolves the baker's first_name for display and computes days_remaining
    from baked_at + 30 days. Sorted newest-first.
    """
    from datetime import datetime as _dt, timedelta as _td

    index = load_packages_index()
    registry = load_registry()
    admins = load_users()  # admin records — used to resolve first_name

    visible = []
    now = _dt.now()
    for pkg in index.values():
        if not _user_can_see_package(user, pkg, registry):
            continue

        # Compute days remaining
        try:
            baked_dt = _dt.strptime(pkg.get("baked_at", ""), "%Y-%m-%d %H:%M:%S")
            expires_dt = baked_dt + _td(days=PACKAGES_RETENTION_DAYS)
            days_remaining = max(0, (expires_dt - now).days)
        except ValueError:
            days_remaining = 0

        # Resolve baker first name
        baker_email = pkg.get("baked_by", "")
        baker_rec   = admins.get(baker_email) or {}
        baker_name  = get_user_first_name(baker_email, rec=baker_rec)

        visible.append({**pkg,
                        "baker_first_name": baker_name,
                        "days_remaining":   days_remaining})

    # Newest first
    visible.sort(key=lambda p: p.get("baked_at", ""), reverse=True)
    return JSONResponse({"packages": visible})


@app.get("/api/packages/{pkg_id}/files/{filename}")
async def api_package_download(pkg_id: str, filename: str, user: str = Depends(require_user)):
    """Download a single file from a baked package."""
    from fastapi.responses import FileResponse

    index = load_packages_index()
    pkg = index.get(pkg_id)
    if not pkg:
        raise HTTPException(status_code=404, detail="Package not found.")

    registry = load_registry()
    if not _user_can_see_package(user, pkg, registry):
        raise HTTPException(status_code=403, detail="Not authorized to access this package.")

    safe_name = Path(filename).name
    fpath = PACKAGES_DIR / pkg_id / safe_name
    if not fpath.exists():
        raise HTTPException(status_code=404, detail="File not in package.")

    media_type = "application/json" if fpath.suffix == ".json" else "text/csv"
    return FileResponse(fpath, media_type=media_type, filename=safe_name)


@app.get("/api/packages/{pkg_id}/preview/{filename}")
async def api_package_preview(pkg_id: str, filename: str,
                              request: Request,
                              page: int = 1, page_size: int = 25,
                              user: str = Depends(require_user)):
    """Inline preview for a baked package file.

    For .csv: returns paginated rows + column list. Streams the file
              line-by-line so memory stays O(page_size) regardless of
              file size. Supports per-column filtering via query params
              of the form `q[<column>]=<value>` (case-insensitive
              substring, multiple filters AND together). Truncates the
              scan at PREVIEW_MAX_ROW_SCAN rows for pathological cases.

    For .json: returns the parsed JSON (no pagination — clients render trees).

    Files larger than PREVIEW_MAX_BYTES return a "too_large" response —
    the user sees a "download to view" message in the UI rather than
    locking up the browser or OOM'ing the server.
    """
    index = load_packages_index()
    pkg = index.get(pkg_id)
    if not pkg:
        raise HTTPException(status_code=404, detail="Package not found.")

    registry = load_registry()
    if not _user_can_see_package(user, pkg, registry):
        raise HTTPException(status_code=403, detail="Not authorized to access this package.")

    safe_name = Path(filename).name
    fpath = PACKAGES_DIR / pkg_id / safe_name
    if not fpath.exists():
        raise HTTPException(status_code=404, detail="File not in package.")

    # ── Size guardrail ─────────────────────────────────────────────────
    # Cheap stat() — no read. If the file's too big, bail before doing
    # any I/O on its contents.
    try:
        size_bytes = fpath.stat().st_size
    except OSError:
        size_bytes = 0
    if size_bytes > PREVIEW_MAX_BYTES:
        return JSONResponse({
            "kind":         "too_large",
            "size_bytes":   size_bytes,
            "size_mb":      round(size_bytes / (1024 * 1024), 1),
            "max_mb":       round(PREVIEW_MAX_BYTES / (1024 * 1024), 1),
            "download_url": f"/api/packages/{pkg_id}/files/{safe_name}",
        })

    if fpath.suffix == ".json":
        try:
            with open(fpath) as f:
                data = json.load(f)
            return JSONResponse({"kind": "json", "data": data})
        except (OSError, ValueError) as e:
            raise HTTPException(status_code=500, detail=f"Failed to parse JSON: {e}")

    if fpath.suffix == ".csv":
        import csv as _csv

        # ── Parse per-column filters from query string ────────────────
        # Accepts both q[colname]=value and q.colname=value. Empty values
        # are dropped (treated as "no filter for that column"). Values are
        # lowercased once for case-insensitive substring matching.
        column_filters: dict[str, str] = {}
        for key, value in request.query_params.multi_items():
            if not value:
                continue
            col_name = None
            if key.startswith("q[") and key.endswith("]"):
                col_name = key[2:-1]
            elif key.startswith("q."):
                col_name = key[2:]
            if col_name:
                column_filters[col_name] = value.lower()

        page = max(1, page)
        start_index = (page - 1) * page_size
        end_index   = start_index + page_size

        try:
            with open(fpath, newline="", encoding="utf-8", errors="replace") as f:
                reader = _csv.reader(f)
                # Header
                try:
                    columns = next(reader)
                except StopIteration:
                    return JSONResponse({
                        "kind": "csv", "columns": [], "rows": [],
                        "total": 0, "page": 1, "page_size": page_size,
                        "filtered": bool(column_filters), "truncated": False,
                    })

                # Pre-compute filter checks once: list of (col_index, lower_needle)
                # tuples. Filters on columns not in the file are silently dropped.
                filter_checks = []
                if column_filters:
                    col_index = {name: i for i, name in enumerate(columns)}
                    filter_checks = [
                        (col_index[c], n)
                        for c, n in column_filters.items() if c in col_index
                    ]

                def _matches(row):
                    for idx, needle in filter_checks:
                        if idx >= len(row):
                            return False
                        if needle not in (row[idx] or "").lower():
                            return False
                    return True

                # Streaming scan: walk the file once, collect only rows in
                # the current page's window, count matches. Memory is
                # O(page_size + columns), independent of file size.
                page_rows = []
                total_matches = 0
                rows_scanned = 0
                truncated = False

                for row in reader:
                    rows_scanned += 1
                    if rows_scanned > PREVIEW_MAX_ROW_SCAN:
                        truncated = True
                        break
                    if filter_checks and not _matches(row):
                        continue
                    # This row "counts" — increment total and possibly capture
                    if start_index <= total_matches < end_index:
                        page_rows.append(row)
                    total_matches += 1
                    # Optimisation: once we've passed end_index AND we have no
                    # filters, we can stop after a full extra page (just to
                    # confirm there's more) and return. We DO need an accurate
                    # total to drive infinite-scroll's "End of file" logic,
                    # so we can't bail early. Carry on counting.

            return JSONResponse({
                "kind":      "csv",
                "columns":   columns,
                "rows":      page_rows,
                "total":     total_matches,
                "page":      page,
                "page_size": page_size,
                "filtered":  bool(column_filters),
                "truncated": truncated,
            })
        except (OSError, UnicodeDecodeError) as e:
            raise HTTPException(status_code=500, detail=f"Failed to read CSV: {e}")

    raise HTTPException(status_code=400, detail=f"Unsupported file type: {fpath.suffix}")


@app.delete("/api/packages/{pkg_id}")
async def api_package_delete(pkg_id: str, user: str = Depends(require_user)):
    """Delete a baked package. Allowed for owner OR any admin on the package's project."""
    import shutil

    index = load_packages_index()
    pkg = index.get(pkg_id)
    if not pkg:
        raise HTTPException(status_code=404, detail="Package not found.")

    registry = load_registry()
    if not _user_can_see_package(user, pkg, registry):
        raise HTTPException(status_code=403, detail="Not authorized to delete this package.")

    pkg_dir = PACKAGES_DIR / pkg_id
    if pkg_dir.exists():
        shutil.rmtree(pkg_dir, ignore_errors=True)
    index.pop(pkg_id, None)
    save_packages_index(index)
    return JSONResponse({"status": "ok", "deleted": pkg_id})


# ═══════════════════════════════════════════════════════════════════════════
# End Packages
# ═══════════════════════════════════════════════════════════════════════════


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
    """Return saved projects filtered to only those the user is an admin of in the registry."""
    config   = load_config(user)
    projects = config.get("projects", {"morta": [], "revizto": [], "forma": []})
    context  = config.get("active_context", {})

    # Owner sees everything
    if not is_owner(user):
        registry      = load_registry()
        user_projects = {k: v for k, v in registry.items() if user in v.get("admins", [])}

        # Collect allowed platform IDs from the registry entries this admin belongs to
        allowed_morta_hubs    = {v["platforms"]["morta"]["hub_id"]
                                  for v in user_projects.values()
                                  if v.get("platforms", {}).get("morta", {}).get("hub_id")}
        allowed_revizto_uuids = {v["platforms"]["revizto"]["project_uuid"]
                                  for v in user_projects.values()
                                  if v.get("platforms", {}).get("revizto", {}).get("project_uuid")}
        allowed_forma_ids     = {v["platforms"]["forma"]["project_id"]
                                  for v in user_projects.values()
                                  if v.get("platforms", {}).get("forma", {}).get("project_id")}

        projects = {
            "morta":   [p for p in projects.get("morta",   []) if p.get("id")   in allowed_morta_hubs],
            "revizto": [p for p in projects.get("revizto", []) if p.get("uuid") in allowed_revizto_uuids],
            "forma":   [p for p in projects.get("forma",   []) if p.get("id")   in allowed_forma_ids],
        }

    return JSONResponse({"projects": projects, "active_context": context})


# ── Diagnostic endpoint — reports timings to find slow paths ──────────
@app.get("/api/_diag")
async def api_diag(user: str = Depends(require_user)):
    """Run a quick suite of timed operations and return ms per call.

    Useful for confirming the cache is doing its job, comparing cold
    vs warm reads, and isolating where a slow page load is spending
    its time. Owner-only output is detailed; admins get a summary.
    """
    import time as _time

    timings = {}

    def t(label, fn):
        s = _time.perf_counter()
        result = fn()
        timings[label] = round((_time.perf_counter() - s) * 1000, 1)
        return result

    # Cache state before
    cache_before = list(_blob_cache.keys())
    timings["_cache_keys_before"] = cache_before
    timings["_cache_size"] = len(cache_before)

    # Time individual loaders (will hit cache if warm, blob/disk if cold)
    t("load_users",          lambda: load_users())
    t("load_registry",       lambda: load_registry())
    t("load_config",         lambda: load_config(user))
    t("load_invite_log",     lambda: load_invite_log())
    t("_load_portal_reqs",   lambda: _load_portal_requests())
    t("get_user_record",     lambda: get_user_record(user))
    t("get_user_first_name", lambda: get_user_first_name(user))

    # Force a cold read directly (bypasses cache)
    if USE_BLOB:
        t("blob_read_admins_RAW", lambda: blob_read("admins.json"))
        t("blob_read_registry_RAW", lambda: blob_read("projects_registry.json"))

    timings["_use_blob"] = USE_BLOB
    timings["_members_cache_size"] = len(_members_cache)
    timings["_members_refreshing"] = list(_members_refreshing)

    return JSONResponse(timings)


@app.get("/api/_diag/revizto/{project_key}")
async def api_diag_revizto(project_key: str, user: str = Depends(require_user)):
    """Step-by-step diagnostic for the Revizto members fetch.

    Reports each phase of what happens when we try to count Revizto
    members for a given project_key. Helps isolate whether the bug is
    in token storage, token refresh, the API call, or response parsing.

    Hit: /api/_diag/revizto/rak-central
    """
    import time as _time

    out = {"project_key": project_key, "user": user}

    # Phase 1: registry lookup
    t0 = _time.perf_counter()
    registry = load_registry()
    proj = registry.get(project_key)
    out["registry_lookup_ms"] = round((_time.perf_counter() - t0) * 1000, 1)
    if not proj:
        out["error"] = "Project not in registry"
        return JSONResponse(out)
    out["project_name"] = proj.get("name", "")

    rev_cfg = proj.get("platforms", {}).get("revizto", {})
    rev_uuid = rev_cfg.get("project_uuid", "")
    rev_region = rev_cfg.get("region", "dubai")
    out["revizto_uuid"] = rev_uuid
    out["revizto_region"] = rev_region
    if not rev_uuid:
        out["error"] = "No Revizto project_uuid in registry"
        return JSONResponse(out)

    # Phase 2: token blob existence
    t0 = _time.perf_counter()
    blob_key = _revizto_token_blob_key(rev_region, user)
    out["token_blob_key"] = blob_key
    raw_tokens = blob_read(blob_key) if USE_BLOB else None
    out["token_blob_read_ms"] = round((_time.perf_counter() - t0) * 1000, 1)
    out["token_blob_found"]   = raw_tokens is not None
    if raw_tokens:
        out["token_has_access_token"]  = bool(raw_tokens.get("access_token"))
        out["token_has_refresh_token"] = bool(raw_tokens.get("refresh_token"))
    else:
        # Check for legacy shared file
        legacy_blob = f"revizto_tokens_{rev_region}.json"
        legacy = blob_read(legacy_blob) if USE_BLOB else None
        out["legacy_blob_key"]   = legacy_blob
        out["legacy_blob_found"] = legacy is not None

    # Phase 3: token resolution (this calls the API to refresh if needed)
    config = load_config(user)
    out["has_revizto_dubai_config"] = bool(config.get("revizto_dubai"))
    out["has_access_code"] = bool(config.get(f"revizto_{rev_region}", {}).get("access_code"))

    t0 = _time.perf_counter()
    try:
        token = revizto_get_token_for_region(config, rev_region, email=user)
        out["token_resolution_ms"] = round((_time.perf_counter() - t0) * 1000, 1)
        out["token_obtained"] = bool(token)
        out["token_preview"] = (token[:20] + "...") if token else "(empty)"
    except Exception as e:
        out["token_resolution_ms"] = round((_time.perf_counter() - t0) * 1000, 1)
        out["token_error"] = f"{type(e).__name__}: {e}"
        return JSONResponse(out)

    if not token:
        out["error"] = "Could not obtain Revizto token (refresh failed and no access_code or access_code failed)"
        return JSONResponse(out)

    # Phase 4: actual members API call
    base_url = f"https://api.{rev_region}.revizto.com/v5"
    url = f"{base_url}/project/{rev_uuid}/role/member/list"
    out["api_url"] = url

    t0 = _time.perf_counter()
    try:
        r = http_requests.get(
            url,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            timeout=10,
        )
        out["api_call_ms"] = round((_time.perf_counter() - t0) * 1000, 1)
        out["api_status"]  = r.status_code
        if r.status_code == 200:
            body = r.json()
            data = body.get("data", {})
            out["response_data_type"] = type(data).__name__
            out["response_data_size"] = len(data) if hasattr(data, "__len__") else 0
            if isinstance(data, dict):
                out["member_count"] = len(data)
                if data:
                    first_key = next(iter(data))
                    first_val = data[first_key]
                    out["first_member_keys"] = list(first_val.keys())[:8] if isinstance(first_val, dict) else "not a dict"
        else:
            out["api_error_body"] = r.text[:500]
    except Exception as e:
        out["api_call_ms"] = round((_time.perf_counter() - t0) * 1000, 1)
        out["api_error"]   = f"{type(e).__name__}: {e}"

    # Phase 5 (only if Phase 4 returned 404): try alternate URL patterns.
    # Revizto API has shifted endpoints over time and the dubai vs ireland
    # regions sometimes diverge. We probe a handful of plausible paths
    # and report the first one that returns 200.
    if out.get("api_status") == 404:
        candidates = [
            f"{base_url}/project/{rev_uuid}/members",
            f"{base_url}/project/{rev_uuid}/member/list",
            f"{base_url}/project/{rev_uuid}/team",
            f"{base_url}/project/{rev_uuid}/user/list",
            f"{base_url}/project/uuid/{rev_uuid}/member/list",
            f"{base_url}/project/uuid/{rev_uuid}/role/member/list",
            f"{base_url}/projects/{rev_uuid}/members",
        ]
        probes = []
        for u in candidates:
            try:
                pr = http_requests.get(
                    u,
                    headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                    timeout=5,
                )
                probes.append({"url": u, "status": pr.status_code, "body_preview": pr.text[:200] if pr.status_code != 200 else "(200 OK)"})
                if pr.status_code == 200:
                    # Found one that works — note the response shape
                    body = pr.json()
                    probes[-1]["data_preview"] = str(body)[:300]
                    break
            except Exception as e:
                probes.append({"url": u, "error": str(e)[:120]})
        out["url_probes"] = probes

    return JSONResponse(out)


# ── Per-project member counts (live API + 5-min cache) ─────────────────
# Used by /home's "Members on active project" tile to show live counts
# when the user picks a project from the sidebar. Cache is in-memory so
# it resets on every server restart — that's fine, 5 minutes is short.
_members_cache: dict = {}      # key: project_key -> (timestamp, counts dict)
_MEMBERS_TTL_SEC = 300         # 5 minutes
_members_refreshing: set = set()   # project_keys currently being refreshed in background

# Per-project Revizto roles, derived from /team responses by collecting
# distinct accessRole entries. Populated as a side-effect of the members
# background refresh — getting roles from /team is free since we're
# already calling that endpoint.
_revizto_roles_cache: dict = {}   # key: project_key -> (timestamp, [role dicts])


async def _refresh_members_background(project_key: str, user: str, force: bool = False):
    """Background task: do the live API fetches and update the cache.

    Runs Morta/Revizto/Forma in parallel via asyncio.gather. Each platform
    call uses a thread executor since the underlying http_requests/blob
    libraries are sync. Marks the project_key as 'refreshing' so we don't
    fire duplicate background fetches for the same project.

    `force=True` bypasses the cache-fresh check and always re-fetches.
    Default behaviour (force=False) is to skip if the members cache for
    this project is still fresh — saves API quota when sync-roles is
    called immediately after the user has been browsing.
    """
    import time as _time
    import asyncio as _asyncio
    if project_key in _members_refreshing:
        return

    # Skip if cache is fresh and caller didn't force
    if not force:
        cached = _members_cache.get(project_key)
        if cached and (_time.time() - cached[0]) < _MEMBERS_TTL_SEC:
            # Cache fresh → only refresh if roles cache is missing too
            # (members cache might exist from before we added roles tracking)
            if project_key in _revizto_roles_cache:
                return

    _members_refreshing.add(project_key)
    try:
        registry = load_registry()
        proj = registry.get(project_key)
        if not proj:
            return
        config = load_config(user)
        platforms = proj.get("platforms", {})

        # Each helper returns int or None and runs sync work in a thread
        loop = _asyncio.get_event_loop()

        def fetch_morta():
            morta_hub = platforms.get("morta", {}).get("hub_id", "")
            if not morta_hub:
                return None
            api_key = config.get("morta", {}).get("api_key", "") or load_config(OWNER_EMAIL).get("morta", {}).get("api_key", "")
            if not api_key:
                return None
            try:
                r = http_requests.get(
                    f"https://api.morta.io/v1/hub/{morta_hub}/members",
                    headers={"Authorization": f"Bearer {api_key}"},
                    timeout=10,
                )
                if r.status_code == 200:
                    return len(r.json().get("data", []))
            except Exception as e:
                print(f"[members/{project_key}] Morta error: {e}")
            return None

        def fetch_revizto():
            rev_uuid = platforms.get("revizto", {}).get("project_uuid", "")
            rev_region = platforms.get("revizto", {}).get("region", "dubai")
            if not rev_uuid:
                return None
            try:
                token = revizto_get_token_for_region(config, rev_region, email=user)
                if not token:
                    return None
                base_url = f"https://api.{rev_region}.revizto.com/v5"
                # Per Revizto v5 API docs: /v5/project/{projectUuid}/team
                # Response: {result: 0, data: {count: N, entities: [...]}}
                # Each entity has accessRole {id, uuid, name, system}.
                # We extract two things from this single call:
                #   1. The total member count (data.count)
                #   2. The distinct set of accessRoles actually in use on this
                #      project — written to _revizto_roles_cache as a side effect.
                r = http_requests.get(
                    f"{base_url}/project/{rev_uuid}/team",
                    headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                    timeout=10,
                )
                if r.status_code == 200:
                    body = r.json()
                    if body.get("result") == 0:
                        data = body.get("data") or {}
                        entities = data.get("entities") or []

                        # Extract distinct roles (deduped by uuid)
                        roles_by_uuid = {}
                        for ent in entities:
                            role = ent.get("accessRole") or {}
                            ruuid = role.get("uuid")
                            if ruuid and ruuid not in roles_by_uuid:
                                roles_by_uuid[ruuid] = {
                                    "id":     role.get("id"),
                                    "uuid":   ruuid,
                                    "name":   role.get("name", ""),
                                    "system": role.get("system", 0),
                                }
                        roles_list = list(roles_by_uuid.values())
                        roles_list.sort(key=lambda r: (r.get("system", 0), r.get("name", "")))
                        _revizto_roles_cache[project_key] = (_time.time(), roles_list)

                        count = data.get("count")
                        if isinstance(count, int):
                            return count
                        return len(entities)
            except Exception as e:
                print(f"[members/{project_key}] Revizto error: {e}")
            return None

        def fetch_forma():
            forma_pid = platforms.get("forma", {}).get("project_id", "")
            if not forma_pid:
                return None
            try:
                token = forma_get_token(config)
                if not token:
                    return None
                clean_pid = forma_pid.lstrip("b.") if forma_pid.startswith("b.") else forma_pid
                offset = 0
                total = 0
                while True:
                    r = http_requests.get(
                        f"https://developer.api.autodesk.com/construction/admin/v1/projects/{clean_pid}/users",
                        headers={"Authorization": f"Bearer {token}"},
                        params={"limit": 200, "offset": offset},
                        timeout=10,
                    )
                    if r.status_code != 200:
                        break
                    batch = r.json().get("results", [])
                    if not batch:
                        break
                    total += len(batch)
                    offset += len(batch)
                    if len(batch) < 200:
                        break
                return total
            except Exception as e:
                print(f"[members/{project_key}] Forma error: {e}")
            return None

        # Run all three in parallel via the default thread executor
        morta_task   = loop.run_in_executor(None, fetch_morta)
        revizto_task = loop.run_in_executor(None, fetch_revizto)
        forma_task   = loop.run_in_executor(None, fetch_forma)
        m, r, f = await _asyncio.gather(morta_task, revizto_task, forma_task)

        counts = {"morta": m, "revizto": r, "forma": f}
        _members_cache[project_key] = (_time.time(), counts)
    finally:
        _members_refreshing.discard(project_key)


@app.get("/api/project/{project_key}/members")
async def api_project_members(project_key: str, user: str = Depends(require_user)):
    """Return per-platform member counts for a registry project.

    Always returns immediately:
      - If we have fresh cached counts (< 5 min old), return them.
      - If cache is stale or missing, return last-known values (or
        {None, None, None}) AND kick off a background refresh.
      - Frontend can poll this endpoint every 2-3s to pick up
        the refreshed values.

    Response: {morta, revizto, forma, fresh: bool, refreshing: bool}
    """
    import time as _time
    import asyncio as _asyncio

    registry = load_registry()
    proj = registry.get(project_key)
    if not proj:
        raise HTTPException(status_code=404, detail="Project not found in registry.")
    if not is_owner(user) and user not in proj.get("admins", []):
        raise HTTPException(status_code=403, detail="Not an admin of this project.")

    cached = _members_cache.get(project_key)
    now = _time.time()
    fresh = bool(cached and (now - cached[0]) < _MEMBERS_TTL_SEC)

    # Always respond immediately with whatever we have
    counts = cached[1] if cached else {"morta": None, "revizto": None, "forma": None}

    # If cache is stale (or missing), kick off a background refresh.
    # We don't await it — the caller polls.
    if not fresh:
        _asyncio.create_task(_refresh_members_background(project_key, user))

    return JSONResponse({
        **counts,
        "fresh": fresh,
        "refreshing": project_key in _members_refreshing,
    })


@app.get("/api/project/{project_key}/revizto-roles")
async def api_project_revizto_roles(project_key: str, user: str = Depends(require_user)):
    """Return distinct Revizto roles in use on this project.

    Roles are populated as a side-effect of /api/project/{key}/members
    (since both come from the same /team API call). If the cache is
    cold for this project, we kick off a refresh and return whatever
    we have — same pattern as members.

    Response:
        {
          "roles": [{"id": ..., "uuid": ..., "name": ..., "system": ...}, ...],
          "fresh": bool,
          "refreshing": bool
        }
    """
    import time as _time
    import asyncio as _asyncio

    registry = load_registry()
    proj = registry.get(project_key)
    if not proj:
        raise HTTPException(status_code=404, detail="Project not found in registry.")
    if not is_owner(user) and user not in proj.get("admins", []):
        raise HTTPException(status_code=403, detail="Not an admin of this project.")

    cached = _revizto_roles_cache.get(project_key)
    now = _time.time()
    fresh = bool(cached and (now - cached[0]) < _MEMBERS_TTL_SEC)
    roles = cached[1] if cached else []

    if not fresh:
        _asyncio.create_task(_refresh_members_background(project_key, user))

    return JSONResponse({
        "roles": roles,
        "fresh": fresh,
        "refreshing": project_key in _members_refreshing,
    })


@app.post("/api/registry/sync-revizto-roles")
async def api_sync_revizto_roles(user: str = Depends(require_owner)):
    """Sync Revizto roles for every project in the registry.

    For each project with a Revizto UUID configured:
      1. Trigger the members-refresh (which also populates the
         _revizto_roles_cache as a side-effect).
      2. Wait for all to complete in parallel.
      3. Build a per-project roles dict.
      4. Write to revizto_project_roles.json (replacing the existing
         license-level flat list with a project-keyed map).
      5. Embed a lightweight roles reference (uuid + name only) into
         each project's registry entry under platforms.revizto.roles.

    This is a write operation, so it's owner-only. Run it after adding
    or modifying registry projects, or whenever you want to refresh.
    """
    import asyncio as _asyncio
    import time as _time

    registry = load_registry()

    # Targets: projects with a Revizto UUID
    targets = [
        (key, proj) for key, proj in registry.items()
        if proj.get("platforms", {}).get("revizto", {}).get("project_uuid")
    ]

    if not targets:
        return JSONResponse({
            "status": "ok",
            "message": "No registry projects have a Revizto UUID configured.",
            "projects_synced": 0,
        })

    # Run all refreshes in parallel
    tasks = [_refresh_members_background(key, user) for key, _ in targets]
    await _asyncio.gather(*tasks, return_exceptions=True)

    # Collect results — keyed by project_key
    per_project_roles = {}
    embedded_count = 0
    for key, proj in targets:
        cached = _revizto_roles_cache.get(key)
        roles = cached[1] if cached else []
        per_project_roles[key] = {
            "project_name": proj.get("name", key),
            "fetched_at":   _time.strftime("%Y-%m-%d %H:%M:%S"),
            "roles":        roles,
        }

        # Embed a lightweight reference into the registry entry so
        # downstream consumers (UI, scripts) can read roles without
        # touching the file.
        if roles:
            registry[key]["platforms"]["revizto"]["roles"] = [
                {"uuid": r["uuid"], "name": r["name"]} for r in roles
            ]
            embedded_count += 1

    # Persist:
    #   1. revizto_project_roles_by_project.json — new per-project keyed dict
    #      (the original revizto_project_roles.json stays as the license-level
    #      flat list produced by the Revizto_GET_Project_Roles.py extraction).
    #   2. The registry — each project's platforms.revizto gets a `roles` field
    #      with a lightweight {uuid, name} list for quick UI lookups.
    if USE_BLOB:
        blob_write("revizto_project_roles_by_project.json", per_project_roles)
    else:
        with open(BASE_DIR / "revizto_project_roles_by_project.json", "w") as f:
            json.dump(per_project_roles, f, indent=2)
    save_registry(registry)

    return JSONResponse({
        "status": "ok",
        "projects_synced":     len(targets),
        "projects_with_roles": embedded_count,
        "details":             {k: len(v["roles"]) for k, v in per_project_roles.items()},
    })


# ── Invite stats — sent + received with time filter ─────────────────────
@app.get("/api/invites/stats")
async def api_invites_stats(
    period: str = "30d",                       # one of '7d', '30d', 'all'
    user: str = Depends(require_user)
):
    """Count invites SENT and RECEIVED by the current admin, scoped to
    their registry projects, within the requested time window.

    SENT     = entries where this admin acted as the inviter. The invite
               log doesn't currently track who sent the invite, so we
               approximate by counting all invite events on projects this
               admin manages (matches what /profile uses).
    RECEIVED = entries where the invited email == this admin's email.

    period: '7d', '30d', or 'all'. Anything else is treated as '30d'.
    """
    from datetime import timedelta

    log = load_invite_log() or []

    # Resolve scope (registry projects user admins) — same logic as /profile
    registry = load_registry()
    if is_owner(user):
        scoped_keys = set(registry.keys())
    else:
        scoped_keys = {k for k, v in registry.items() if user in v.get("admins", [])}

    # Build the time-window cutoff
    now = datetime.now()
    if period == "7d":
        cutoff = now - timedelta(days=7)
    elif period == "all":
        cutoff = None
    else:
        cutoff = now - timedelta(days=30)
        period = "30d"   # normalise

    def in_window(entry):
        if cutoff is None:
            return True
        try:
            d = datetime.strptime(entry.get("date", "")[:19], "%Y-%m-%d %H:%M:%S")
            return d >= cutoff
        except (ValueError, TypeError):
            return False

    sent = 0
    received = 0
    user_lower = user.lower()
    for e in log:
        if not in_window(e):
            continue
        # received: the invitee email is this admin
        if (e.get("email") or "").lower() == user_lower:
            received += 1
        # sent: any entry on a project this admin manages (proxy for
        # "this admin's team sent an invite", since invite_log doesn't
        # record who clicked the button)
        if e.get("project_key") in scoped_keys:
            sent += 1

    return JSONResponse({
        "period":   period,
        "sent":     sent,
        "received": received,
    })


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
            dubai_ok   = bool(config.get("revizto_dubai",   {}).get("access_code")) or bool(load_revizto_tokens("dubai",   user))
            ireland_ok = bool(config.get("revizto_ireland", {}).get("access_code")) or bool(load_revizto_tokens("ireland", user))
            if not dubai_ok and not ireland_ok:
                results["revizto"] = {"count": 0, "items": [], "error": "No Revizto access code configured — check Settings"}
            else:
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor() as pool:
                    projects = await loop.run_in_executor(pool, fetch_revizto_projects, config, user)
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

    # For non-owners, filter the saved project lists down to only registry-allowed projects
    # so their config never stores projects they shouldn't have access to.
    if not is_owner(user):
        registry      = load_registry()
        user_reg      = {k: v for k, v in registry.items() if user in v.get("admins", [])}
        allowed_morta    = {v["platforms"]["morta"]["hub_id"]         for v in user_reg.values() if v.get("platforms", {}).get("morta", {}).get("hub_id")}
        allowed_revizto  = {v["platforms"]["revizto"]["project_uuid"] for v in user_reg.values() if v.get("platforms", {}).get("revizto", {}).get("project_uuid")}
        allowed_forma    = {v["platforms"]["forma"]["project_id"]      for v in user_reg.values() if v.get("platforms", {}).get("forma", {}).get("project_id")}
        p = config.get("projects", {})
        config["projects"] = {
            "morta":   [x for x in p.get("morta",   []) if x.get("id")   in allowed_morta],
            "revizto": [x for x in p.get("revizto", []) if x.get("uuid") in allowed_revizto],
            "forma":   [x for x in p.get("forma",   []) if x.get("id")   in allowed_forma],
        }
        # Update results counts to reflect filtered lists
        for plt in ["morta", "revizto", "forma"]:
            if plt in results:
                results[plt]["items"] = config["projects"][plt]
                results[plt]["count"] = len(config["projects"][plt])

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

    # For non-owners, validate the selected project key is one they're an admin of
    project_key = body.get("active_project_key", "")
    if project_key and not is_owner(user):
        registry = load_registry()
        if user not in registry.get(project_key, {}).get("admins", []):
            raise HTTPException(status_code=403, detail="You are not an admin of this project.")

    config["active_context"] = {
        "active_project_key":     project_key,
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
    """Return a list of available CSV files in the user's temp folder."""
    user_temp = get_user_temp_dir(user)
    files = sorted(user_temp.glob("*.csv"))
    return JSONResponse({
        "files": [
            {
                "name":     f.stem,
                "filename": f.name,
                "size_kb":  round(f.stat().st_size / 1024, 1),
                "modified": datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
            }
            for f in files
        ]
    })

@app.get("/api/exports/{filename}")
async def api_exports_read(filename: str, user: str = Depends(require_user)):
    """Read and return the contents of a specific CSV from the user's temp folder."""
    safe_name = Path(filename).name
    if not safe_name.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")
    file_path = get_user_temp_dir(user) / safe_name
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"{safe_name} not found. Run the extraction first.")
    with open(file_path, newline="", encoding="utf-8") as f:
        reader  = csv.DictReader(f)
        rows    = list(reader)
        headers = reader.fieldnames or []
    return JSONResponse({
        "filename": safe_name,
        "headers":  headers,
        "rows":     rows,
        "total":    len(rows),
    })

# ---------------------------------------------------------------------------
# API — Invite
# ---------------------------------------------------------------------------

@app.get("/api/invite/log")
async def api_invite_log(
    project_key: str = None,
    limit: int = 50,
    type: str = None,
    user: str = Depends(require_user)
):
    """Return activity log entries scoped to the user's registry projects.

    project_key: if provided, filter to that specific project only.
    limit: max entries to return (most recent first), default 50.
    type:  filter by event type. Supported values:
           - 'invite'  → invite events only (entries with no `type` field
                         are treated as invites for backward-compat)
           - 'extraction', 'registry_edit', 'metadata_sync',
             'package_bake', 'portal_decision' → match exactly
           - None (default) → all event types
    """
    log           = load_invite_log()
    user_projects = get_user_projects(user)

    if not is_owner(user):
        # Non-owners only see entries for projects they manage
        log = [e for e in log
               if e.get("project_key") and e.get("project_key") in user_projects]

    # If a specific project is selected, filter to that project only
    if project_key:
        log = [e for e in log if e.get("project_key") == project_key]

    # Filter by event type (invites are entries without an explicit type)
    if type:
        if type == "invite":
            log = [e for e in log if not e.get("type")]
        else:
            log = [e for e in log if e.get("type") == type]

    # Most recent first, capped at limit
    log = log[-limit:][::-1]

    return JSONResponse({"entries": log, "total": len(log), "project_key": project_key or "all"})

@app.post("/api/invite/single")
async def api_invite_single(request: Request, user: str = Depends(require_user)):
    data   = await request.json()
    config = load_config(user)
    email  = data.get("email", "").strip()
    if not email:
        return JSONResponse({"status": "error", "detail": "Email is required"}, status_code=400)

    ctx         = config.get("active_context", {})
    project_key = ctx.get("active_project_key", "")

    # Pull platform IDs from registry for the active project (single source of truth)
    registry  = load_registry()
    reg_proj  = registry.get(project_key, {})
    platforms = reg_proj.get("platforms", {})

    morta_hub_id     = platforms.get("morta", {}).get("hub_id", "") or ctx.get("morta_hub_id", "")
    revizto_uuid     = platforms.get("revizto", {}).get("project_uuid", "") or data.get("revizto_project_uuid", "")
    forma_project_id = platforms.get("forma", {}).get("project_id", "") or data.get("forma_project_id", "")

    # Access parameters — same structure as pending process
    morta_tag_id         = data.get("morta_tag_id", "")
    revizto_license_role = int(data.get("revizto_license_role_id") or config.get(f"revizto_{ctx.get('revizto_region','dubai')}", {}).get("default_license_role", 2))
    revizto_project_role = data.get("revizto_project_role_id") or None
    revizto_to_license   = data.get("revizto_invite_license", True)
    revizto_to_project   = data.get("revizto_invite_project", False)
    forma_company_id     = data.get("forma_company_id", "")
    forma_role_ids       = data.get("forma_role_ids", [])
    forma_products_raw   = data.get("forma_products", [])
    forma_products       = forma_products_raw if forma_products_raw else [{"key": "docs", "access": "member"}]

    first_name = data.get("first_name", "")
    last_name  = data.get("last_name", "")

    # No top-level Company / Department — each platform owns its own.
    # The result dict still has the keys (for backward-compat with
    # invite-log shape) but they're empty for direct invites.
    revizto_company    = data.get("revizto_company", "")
    revizto_department = data.get("revizto_department", "")
    revizto_location   = data.get("revizto_location", "")

    result = {
        "date":             datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode":             "single",
        "email":            email,
        "first_name":       first_name,
        "last_name":        last_name,
        "company":          "",
        "department":       "",
        "morta_status":     "skipped",
        "revizto_status":   "skipped",
        "forma_status":     "skipped",
        "revizto_project":  reg_proj.get("name", ""),
        "forma_project":    reg_proj.get("name", ""),
        "project_key":      project_key,
    }

    if data.get("morta"):
        r = morta_invite(email, config, hub_id=morta_hub_id, tag_reference_id=morta_tag_id)
        result["morta_status"] = r["status"]
        if r["status"] != "success":
            result["morta_detail"] = r.get("detail", "")

    if data.get("revizto"):
        r = revizto_invite(
            email, first_name, last_name, revizto_company, revizto_department, config,
            project_uuid=revizto_uuid,
            license_role_id=revizto_license_role,
            project_role_id=int(revizto_project_role) if revizto_project_role else None,
            invite_to_license=revizto_to_license,
            invite_to_project=revizto_to_project,
            location=revizto_location,
            admin_email=user,
        )
        result["revizto_status"] = r["status"]
        if r["status"] != "success":
            result["revizto_detail"] = r.get("detail", "")

    if data.get("forma"):
        r = forma_invite(
            email, config,
            project_id=forma_project_id,
            company_id=forma_company_id or None,
            role_ids=forma_role_ids if forma_role_ids else None,
            products=forma_products,
        )
        result["forma_status"] = r["status"]
        if r["status"] != "success":
            result["forma_detail"] = r.get("detail", "")

    append_invite_log([result])
    has_error = any(result[k].startswith("error") for k in ["morta_status", "revizto_status", "forma_status"] if result[k] != "skipped")
    return JSONResponse({"status": "success" if not has_error else "partial", "result": result})

@app.post("/api/invite/preview")
async def api_invite_preview(file: UploadFile = File(...), user: str = Depends(require_user)):
    """Parse uploaded CSV / XLSX into a list of user dicts.

    Column expectations match the Single-tab fields, so a Bulk import is
    just N parallel Singles. All fields except Email are optional; missing
    fields fall back to platform defaults.

    Recognized columns (case-insensitive, exact name except where noted):

      Email                            (REQUIRED)
      First Name
      Last Name                        (also accepts double-space "Last  Name")

      Morta Access                     YES / NO   (default NO)
      Morta Tag                        Permission tag NAME from registry
                                       (resolved to tag_reference_id)

      Revizto Access                   YES / NO   (default NO)
      Revizto License Role             Role NAME (Collaborator / Content Creator / Guest)
                                       (default: project's default_license_role)
      Revizto Project Role             Role NAME (resolved against registry)
      Revizto To License               YES / NO   (default YES)
      Revizto To Project               YES / NO   (default YES)
      Revizto Company                  Company NAME for license membership
      Revizto Department               Department for license membership
      Revizto Location                 Office location for license membership

      ACC Access                       YES / NO   (default NO)
      Forma Company                    Company NAME (resolved to id from registry)
      Forma Role                       Role NAME (resolved to id from registry)
      Forma Docs                       YES / NO   (default YES if Forma=YES)
      Forma Design Coordination        YES / NO   (default NO)
      Forma Model Coordination         YES / NO   (default NO)
    """
    import pandas as pd

    contents = await file.read()
    if file.filename.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(contents))
    else:
        df = pd.read_excel(io.BytesIO(contents), header=1)
    df.columns = df.columns.str.strip()

    # Resolve registry-backed lookups for the ACTIVE project so name -> id
    # mapping happens server-side. Same source the Single tab uses.
    config    = load_config(user)
    ctx       = config.get("active_context", {})
    project_key = ctx.get("active_project_key", "")
    registry  = load_registry()
    plat      = (registry.get(project_key, {}) or {}).get("platforms", {}) or {}

    morta_tags_by_name        = {(t.get("name") or "").strip().lower(): t.get("tag_reference_id", "")
                                  for t in (plat.get("morta", {}) or {}).get("tags", []) or []}
    revizto_lic_roles_by_name = {(r.get("name") or "").strip().lower(): r.get("id")
                                  for r in (plat.get("revizto", {}) or {}).get("license_roles", []) or []}
    revizto_proj_roles_by_name= {(r.get("name") or "").strip().lower(): r.get("id")
                                  for r in (plat.get("revizto", {}) or {}).get("roles", []) or []}
    forma_cos_by_name         = {(c.get("name") or "").strip().lower(): c.get("id", "")
                                  for c in (plat.get("forma", {}) or {}).get("companies", []) or []}
    forma_roles_by_name       = {(r.get("name") or "").strip().lower(): r.get("id", "")
                                  for r in (plat.get("forma", {}) or {}).get("roles", []) or []}

    default_lic_role = int(config.get(f"revizto_{ctx.get('revizto_region','dubai')}", {}).get("default_license_role", 2))

    def cell(row, *names) -> str:
        """Read first matching column. Tolerates dropped/renamed columns."""
        for n in names:
            v = row.get(n)
            if v is not None and not (isinstance(v, float) and pd.isna(v)):
                s = str(v).strip()
                if s and s.lower() != "nan":
                    return s
        return ""

    def yesno(row, *names, default: bool = False) -> bool:
        v = cell(row, *names).upper()
        if v in ("YES", "Y", "TRUE", "1"):    return True
        if v in ("NO",  "N", "FALSE", "0"):   return False
        return default

    users = []
    warnings = []
    for idx, row in df.iterrows():
        email = cell(row, "Email")
        if not email:
            continue

        morta_on   = yesno(row, "Morta Access")
        revizto_on = yesno(row, "Revizto Access")
        forma_on   = yesno(row, "ACC Access", "Forma Access")

        # Morta tag — resolve name -> id via registry
        morta_tag_id = ""
        morta_tag_name = cell(row, "Morta Tag", "Permission Tag")
        if morta_tag_name:
            tid = morta_tags_by_name.get(morta_tag_name.lower(), "")
            if tid:
                morta_tag_id = tid
            else:
                warnings.append(f"Row {idx+2}: Morta tag '{morta_tag_name}' not found in registry")

        # Revizto license role — name OR numeric id; fall back to default
        rev_lic_role = default_lic_role
        rev_lic_name = cell(row, "Revizto License Role", "License Role")
        if rev_lic_name:
            if rev_lic_name.isdigit():
                rev_lic_role = int(rev_lic_name)
            else:
                rid = revizto_lic_roles_by_name.get(rev_lic_name.lower())
                if rid is not None:
                    rev_lic_role = int(rid)
                else:
                    warnings.append(f"Row {idx+2}: Revizto license role '{rev_lic_name}' not found")

        # Revizto project role — name -> id; null if not specified
        rev_proj_role = None
        rev_proj_name = cell(row, "Revizto Project Role", "Project Role")
        if rev_proj_name:
            pid = revizto_proj_roles_by_name.get(rev_proj_name.lower())
            if pid is not None:
                rev_proj_role = pid
            else:
                warnings.append(f"Row {idx+2}: Revizto project role '{rev_proj_name}' not found")

        # Forma company — name -> id
        forma_co_id = ""
        forma_co_name = cell(row, "Forma Company", "ACC Company")
        if forma_co_name:
            cid = forma_cos_by_name.get(forma_co_name.lower(), "")
            if cid:
                forma_co_id = cid
            else:
                warnings.append(f"Row {idx+2}: Forma company '{forma_co_name}' not found in registry")

        # Forma role — name -> id (single role; cockpit treats as a list)
        forma_role_ids = []
        forma_role_name = cell(row, "Forma Role", "ACC Role")
        if forma_role_name:
            rid = forma_roles_by_name.get(forma_role_name.lower(), "")
            if rid:
                forma_role_ids = [rid]
            else:
                warnings.append(f"Row {idx+2}: Forma role '{forma_role_name}' not found in registry")

        # Forma products — at least Docs by default if Forma is on
        products = []
        if yesno(row, "Forma Docs",                 default=forma_on): products.append({"key": "docs",                 "access": "member"})
        if yesno(row, "Forma Design Coordination",  default=False):    products.append({"key": "designCollaboration",   "access": "member"})
        if yesno(row, "Forma Model Coordination",   default=False):    products.append({"key": "modelCoordination",     "access": "member"})

        # Each platform owns its own identity fields. There's no top-level
        # Company / Department in the direct invite flow — Revizto carries
        # its own Company / Department / Location, Forma uses its own
        # company_id from its registry. Empty Revizto cells stay empty;
        # there's no top-level to fall back to.
        rev_company    = cell(row, "Revizto Company")
        rev_department = cell(row, "Revizto Department")
        rev_location   = cell(row, "Revizto Location")

        users.append({
            "email":      email,
            "first_name": cell(row, "First Name"),
            "last_name":  cell(row, "Last  Name", "Last Name"),
            "morta":      morta_on,
            "revizto":    revizto_on,
            "forma":      forma_on,
            "morta_tag_id":            morta_tag_id,
            "revizto_license_role_id": rev_lic_role,
            "revizto_project_role_id": rev_proj_role,
            "revizto_invite_license":  yesno(row, "Revizto To License",  default=True),
            "revizto_invite_project":  yesno(row, "Revizto To Project",  default=True),
            "revizto_company":         rev_company,
            "revizto_department":      rev_department,
            "revizto_location":        rev_location,
            "forma_company_id":        forma_co_id,
            "forma_role_ids":          forma_role_ids,
            "forma_products":          products if products else [{"key": "docs", "access": "member"}],
        })

    return JSONResponse({
        "status":   "success",
        "users":    users,
        "total":    len(users),
        "warnings": warnings,
    })

@app.post("/api/invite/bulk")
async def api_invite_bulk(request: Request, user: str = Depends(require_user)):
    """Bulk invite — runs the same per-user logic as /api/invite/single,
    once per row. Each user dict in the payload accepts the same fields
    Single does, so the only difference between Single and Bulk is iteration.

    Per-user fields (all optional except email):
      first_name, last_name, company, department
      morta            (bool)
      morta_tag_id
      revizto          (bool)
      revizto_license_role_id  (int)
      revizto_project_role_id  (int|null)
      revizto_invite_license   (bool, default True)
      revizto_invite_project   (bool, default True)
      forma            (bool)
      forma_company_id
      forma_role_ids   (list[str])
      forma_products   (list[{key, access}])
    """
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

    # Pull platform IDs from registry for the active project (single source of truth)
    registry  = load_registry()
    reg_proj  = registry.get(project_key, {})
    platforms = reg_proj.get("platforms", {})
    morta_hub_id = platforms.get("morta", {}).get("hub_id", "") or ctx.get("morta_hub_id", "")

    default_lic_role = int(config.get(f"revizto_{ctx.get('revizto_region','dubai')}", {}).get("default_license_role", 2))

    results = []
    for u in users:
        email      = (u.get("email") or "").strip()
        first_name = u.get("first_name", "")
        last_name  = u.get("last_name", "")

        # No top-level Company / Department in the direct invite flow.
        # Each platform owns its own identity values. (The invite_log
        # row keeps the company/department fields for backward
        # compatibility with portal-derived entries — direct invites
        # just write empty strings.)
        result = {
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "mode": "bulk",
            "email": email,
            "first_name": first_name,
            "last_name": last_name,
            "company": "",
            "department": "",
            "morta_status": "skipped",
            "revizto_status": "skipped",
            "forma_status": "skipped",
            "revizto_project": revizto_project_name,
            "forma_project": forma_project_name,
            "project_key": project_key,
        }

        # ── Morta ──
        if u.get("morta"):
            r = morta_invite(email, config, hub_id=morta_hub_id,
                             tag_reference_id=u.get("morta_tag_id", ""))
            result["morta_status"] = r["status"]
            if r["status"] != "success":
                result["morta_detail"] = r.get("detail", "")

        # ── Revizto ──
        if u.get("revizto"):
            lic_role  = int(u.get("revizto_license_role_id") or default_lic_role)
            proj_role = u.get("revizto_project_role_id") or None
            to_lic    = bool(u.get("revizto_invite_license", True))
            to_proj   = bool(u.get("revizto_invite_project", True))
            # Revizto-specific identity values come straight from the per-user
            # record. No fallback — if admin left them blank, they go blank.
            rev_co    = u.get("revizto_company", "")
            rev_dept  = u.get("revizto_department", "")
            rev_loc   = u.get("revizto_location", "")
            r = revizto_invite(
                email, first_name, last_name, rev_co, rev_dept, config,
                project_uuid=revizto_project_uuid,
                license_role_id=lic_role,
                project_role_id=proj_role,
                invite_to_license=to_lic,
                invite_to_project=to_proj,
                location=rev_loc,
                admin_email=user,
            )
            result["revizto_status"] = r["status"]
            if r["status"] != "success":
                result["revizto_detail"] = r.get("detail", "")

        # ── Forma ──
        if u.get("forma"):
            forma_products = u.get("forma_products") or [{"key": "docs", "access": "member"}]
            r = forma_invite(
                email, config,
                project_id=forma_project_id,
                company_id=u.get("forma_company_id", "") or None,
                role_ids=u.get("forma_role_ids", []) or None,
                products=forma_products,
            )
            result["forma_status"] = r["status"]
            if r["status"] != "success":
                result["forma_detail"] = r.get("detail", "")

        results.append(result)

    if results:
        append_invite_log(results)

    success = sum(1 for r in results if not any(
        r[k].startswith("error") for k in ["morta_status", "revizto_status", "forma_status"] if r[k] != "skipped"
    ))
    return JSONResponse({
        "status":  "success",
        "total":   len(results),
        "success": success,
        "errors":  len(results) - success,
    })

# ---------------------------------------------------------------------------
# API — User Search
# ---------------------------------------------------------------------------

@app.get("/api/user/search")
async def api_user_search(email: str, user: str = Depends(require_user)):
    config = load_config(user)
    email  = email.strip().lower()
    if not email:
        return JSONResponse({"found": False})

    # Registry is the single source of truth for search scope
    registry       = load_registry()
    admin_projects = registry if is_owner(user) else {
        k: v for k, v in registry.items() if user in v.get("admins", [])
    }

    if not admin_projects:
        return JSONResponse({
            "found": False, "email": email,
            "morta": None, "revizto": None, "forma": None,
            "detail": "You have no projects assigned in the registry.",
        })

    # Extract scoped platform IDs from registry
    allowed_morta_hubs    = [v["platforms"]["morta"]["hub_id"]
                              for v in admin_projects.values()
                              if v.get("platforms", {}).get("morta", {}).get("hub_id")]
    allowed_revizto_uuids = [v["platforms"]["revizto"]["project_uuid"]
                              for v in admin_projects.values()
                              if v.get("platforms", {}).get("revizto", {}).get("project_uuid")]
    allowed_forma_ids     = [v["platforms"]["forma"]["project_id"]
                              for v in admin_projects.values()
                              if v.get("platforms", {}).get("forma", {}).get("project_id")]

    # Build a lookup of project names from the registry for enriching results
    revizto_name_map = {
        v["platforms"]["revizto"]["project_uuid"]: k
        for k, v in admin_projects.items()
        if v.get("platforms", {}).get("revizto", {}).get("project_uuid")
    }
    forma_name_map = {
        v["platforms"]["forma"]["project_id"]: admin_projects[k].get("name", k)
        for k, v in admin_projects.items()
        if v.get("platforms", {}).get("forma", {}).get("project_id")
    }

    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        morta_fut   = loop.run_in_executor(
            pool, morta_search_user, email, config, allowed_morta_hubs
        )
        revizto_fut = loop.run_in_executor(
            pool, lambda: revizto_search_user(
                email, config, allowed_revizto_uuids,
                admin_email=user, registry_projects=admin_projects
            )
        )
        forma_fut   = loop.run_in_executor(
            pool, forma_search_user, email, config, allowed_forma_ids
        )
        morta_result, revizto_result, forma_result = await asyncio.gather(
            morta_fut, revizto_fut, forma_fut
        )

    # Enrich Revizto project results with names from registry
    if revizto_result and revizto_result.get("projects"):
        for p in revizto_result["projects"]:
            p["name"] = revizto_name_map.get(p.get("uuid"), p.get("uuid", ""))

    # Enrich Forma project results with names from registry
    if forma_result and forma_result.get("projects"):
        for p in forma_result["projects"]:
            p["name"] = forma_name_map.get(p.get("project_id"), p.get("project_id", ""))

    found = any(r is not None for r in [morta_result, revizto_result, forma_result])
    name  = ""
    for r in [morta_result, forma_result, revizto_result]:
        if r and r.get("name"):
            name = r["name"]
            break

    # Build a helpful detail message if not found, explaining why each platform returned nothing
    detail = None
    if not found:
        reasons = []
        if not config.get("morta", {}).get("api_key"):
            reasons.append("Morta API key not set in Settings")
        if not allowed_revizto_uuids:
            reasons.append("no Revizto project in your registry")
        if not allowed_morta_hubs:
            reasons.append("no Morta hub in your registry")
        if reasons:
            detail = "User not found. Note: " + " · ".join(reasons)
        else:
            detail = None  # genuinely not found, no config issues

    return JSONResponse({
        "found":   found,
        "name":    name,
        "email":   email,
        "morta":   morta_result,
        "revizto": revizto_result,
        "forma":   forma_result,
        "detail":  detail,
    })

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

# Pre-warm the cache now that all loaders are defined. This pays the
# blob round-trip cost once at worker startup so the first user request
# lands on an already-warm cache.
try:
    _prewarm_cache()
except Exception as _prewarm_err:
    print(f"[prewarm] Outer error (non-fatal): {type(_prewarm_err).__name__}: {_prewarm_err}")

# Clean up expired packages (>30 days old) once at startup. Cheap operation;
# index is small. Failures are non-fatal — just log and continue serving.
try:
    cleanup_expired_packages()
except Exception as _pkg_err:
    print(f"[packages] Cleanup error (non-fatal): {type(_pkg_err).__name__}: {_pkg_err}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)