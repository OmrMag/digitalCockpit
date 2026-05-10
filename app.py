"""
Digital Construction Cockpit — FastAPI Application
"""

import json
import csv
import asyncio
import time
import hashlib
import os
import requests as http_requests
import io
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, Request, Form, UploadFile, File, Depends, Cookie, HTTPException, BackgroundTasks, Header
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Block 4 — Scheduler runner + blob client for export containers
from codeShared import Scheduler_Runner
from codeShared import Blob_Client
from codeShared import Scheduler_Engine   # Block 5 — cron firing engine
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

# ── Response compression ───────────────────────────────────────────────
# GZip every response body bigger than 1 KB. Crucial for /api/browse/users
# which is multi-MB JSON; compression typically takes the wire payload
# down by 85-90% (e.g. 2.7 MB → ~350 KB). Without this, slow clients
# spend serialization time on the connection that should be freeing up
# the worker for the next request. FastAPI ships GZipMiddleware ready
# to use; the minimum_size threshold spares small JSON responses where
# compression cost would outweigh savings.
from starlette.middleware.gzip import GZipMiddleware
app.add_middleware(GZipMiddleware, minimum_size=1024)

BASE_DIR    = Path(__file__).parent
EXPORTS_DIR = BASE_DIR / "exports"
EXPORTS_DIR.mkdir(exist_ok=True)
TEMP_DIR    = BASE_DIR / "temp"
TEMP_DIR.mkdir(exist_ok=True)
# Audit log dir for the Browse Users edit feature. Phase 2 only writes the
# scaffolding; phase 3 (inline edits) appends to audit_log.jsonl on every
# write-back. Kept here so the dir exists from day one and any helpers
# elsewhere can reference AUDIT_DIR without import surprises.
AUDIT_DIR   = BASE_DIR / "audit"
AUDIT_DIR.mkdir(exist_ok=True)
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

def audit_log_event(event_type: str, actor: str, target: dict, before: dict = None, after: dict = None, status: str = "ok", error: str = ""):
    """Append a single audit record to AUDIT_DIR/audit_log.jsonl.

    JSONL = one JSON object per line, append-friendly, easy to tail.
    Fields are intentionally simple so the file stays human-readable on
    `cat` / `tail`. The Browse Users edit flow will call this on every
    write-back; phase 1+2 doesn't write yet but the helper exists so phase
    3 doesn't need a separate plumbing change.

    target.platform / target.email / target.project_id together identify
    what was changed; before/after are the diff snapshots. Failures are
    logged too with status="error" so we have a complete trail.
    """
    record = {
        "ts":         datetime.now(timezone.utc).isoformat(),
        "actor":      actor,
        "event":      event_type,
        "status":     status,
        "target":     target,
        "before":     before,
        "after":      after,
    }
    if error:
        record["error"] = error
    try:
        path = AUDIT_DIR / "audit_log.jsonl"
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str) + "\n")
    except Exception as e:
        # Audit log failures must never break the calling code path.
        print(f"[audit] failed to write event: {e}")

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

# ---------------------------------------------------------------------------
# Scheduler — Data Layer
# ---------------------------------------------------------------------------
# State for the Scheduler page: shards (atomic runnable units), compilers
# (schedule wrappers that fire a list of shards), and run history. Stored
# alongside the existing JSON files in cockpit-data so persistence and
# locking semantics match what's already in place. The cache TTL applies
# the same way — saves invalidate, reads coalesce.
#
# Layered design:
#   - SCRIPT_CATALOG: hardcoded source-of-truth for what scripts can be
#     scheduled, plus what context fields each one requires.
#   - load_/save_/append_ helpers: same pattern as load_invite_log().
#   - Validators: enforce schema invariants before persistence.
#
# What's NOT here (intentionally — added in later building blocks):
#   - The actual cron firing engine (APScheduler boot)
#   - Run-now execution that invokes scripts
#   - Token refresh for headless runs
#
# This block is pure CRUD + validation. The endpoints below expose it.

# Hardcoded catalog of schedulable scripts. Adding a new script means:
#   1. Add an entry here
#   2. Make sure the script file actually exists in code{Forma,Revizto,Morta}/
#   3. Make sure the runner (later block) knows how to invoke it
SCRIPT_CATALOG = {
    # Forma
    "Forma_GET_Models": {
        "platform":         "forma",
        "display_name":     "Models list",
        "required_context": ["project_id"],
        "description":      "List all models in a project. Useful as input to other automations.",
    },
    "Forma_GET_Model_Properties": {
        "platform":         "forma",
        "display_name":     "Model properties (L3)",
        "required_context": ["project_id", "model_item_id", "model_name"],
        "description":      "Full element-level properties dump from a single model. Large output (~5GB on Wynn-scale models).",
    },
    "Forma_GET_Model_Properties_Raw": {
        "platform":         "forma",
        "display_name":     "Model properties — full (no filter)",
        "required_context": ["project_id", "model_item_id", "model_name"],
        "description":      "Same model property data as L3 but with NO category filter and NO grouping. Single flat JSON array — every element Forma's index returned. Useful for ad-hoc analysis where you need categories the L3 drops (Views, Sheets, etc).",
    },
    "Proccesed_Model_Properties": {
        "platform":         "forma",
        "display_name":     "Model health report (L4)",
        "required_context": ["project_id", "model_item_id", "model_name"],
        "description":      "Summary report + aggregate breakdown built from L3 output. Requires L3 to have run first.",
    },
    # Revizto
    "Revizto_GET_Project_Members": {
        "platform":         "revizto",
        "display_name":     "Project members",
        "required_context": ["project_uuid"],
        "description":      "Members + roles for a single Revizto project.",
    },
    "Revizto_GET_Project_Issues": {
        "platform":         "revizto",
        "display_name":     "Project issues",
        "required_context": ["project_uuid"],
        "description":      "All issues with current state (status, assignees, comments).",
    },
    "Revizto_GET_Clash_Tests": {
        "platform":         "revizto",
        "display_name":     "Clash tests",
        "required_context": ["project_uuid"],
        "description":      "Clash test definitions + last-run statistics.",
    },
    "Revizto_GET_License_Members": {
        "platform":         "revizto",
        "display_name":     "License members",
        "required_context": [],
        "description":      "All members across the license. License-level (no project required).",
    },
    # Morta
    "Morta_GET_Hub_Members": {
        "platform":         "morta",
        "display_name":     "Hub members",
        "required_context": [],
        "description":      "All members of the Morta hub.",
    },
    "Morta_GET_Member_Tags": {
        "platform":         "morta",
        "display_name":     "Member tags",
        "required_context": [],
        "description":      "Tag assignments per member.",
    },
}

# Output formats the scheduler accepts. Restricted to csv and json — xlsx
# was removed from the picker because no current script produces xlsx
# natively, and the runner would have to convert. Add it back if a future
# script needs it.
VALID_OUTPUT_FORMATS = {"csv", "json"}

# Schedule frequencies the UI offers. The actual cron firing logic
# interprets these in a later block; here we just validate the shape.
VALID_FREQUENCIES = {"daily", "weekdays", "weekend", "weekly", "monthly"}

# Destination types. "blob" is the only one supported this round; others
# are listed so we reject unknown values explicitly rather than silently
# accept and break later.
VALID_DESTINATIONS = {"blob"}    # future: "egnyte", "sharepoint", "morta"

AUTO_DISABLE_AFTER_FAILURES = 3
RUN_LOG_MAX_ENTRIES         = 1000

def _new_id(prefix: str) -> str:
    """Generate a short-ish, sortable-ish unique ID. Format:
    <prefix>_<base36 timestamp ms>_<4 base36 random chars>. Collisions
    are essentially impossible for our scale; the timestamp prefix means
    new IDs sort after old ones lexicographically too.
    """
    import time, random, string
    t = int(time.time() * 1000)
    base36 = ""
    while t > 0:
        t, r = divmod(t, 36)
        base36 = (string.digits + string.ascii_lowercase)[r] + base36
    rand = "".join(random.choice(string.digits + string.ascii_lowercase) for _ in range(4))
    return f"{prefix}_{base36}{rand}"

# ── load/save: shards ─────────────────────────────────────────────────────

def load_shards() -> list:
    """Return the full list of shard records (cached)."""
    cached = _cache_get("shards.json")
    if cached is not None:
        return cached
    if USE_BLOB:
        data = blob_read("shards.json")
        if data is not None:
            _cache_set("shards.json", data)
            return data
    # No legacy disk file — this is a new feature
    _cache_set("shards.json", [])
    return []

def save_shards(shards: list) -> None:
    """Persist shards. Also invalidates cache so next read sees the change."""
    if USE_BLOB:
        blob_write("shards.json", shards)
    _cache_invalidate("shards.json")

# ── load/save: compilers ──────────────────────────────────────────────────

def load_compilers() -> list:
    cached = _cache_get("compilers.json")
    if cached is not None:
        return cached
    if USE_BLOB:
        data = blob_read("compilers.json")
        if data is not None:
            _cache_set("compilers.json", data)
            return data
    _cache_set("compilers.json", [])
    return []

def save_compilers(compilers: list) -> None:
    if USE_BLOB:
        blob_write("compilers.json", compilers)
    _cache_invalidate("compilers.json")

# ── load/save: run log ────────────────────────────────────────────────────

def load_runs() -> list:
    cached = _cache_get("automation_runs.json")
    if cached is not None:
        return cached
    if USE_BLOB:
        data = blob_read("automation_runs.json")
        if data is not None:
            _cache_set("automation_runs.json", data)
            return data
    _cache_set("automation_runs.json", [])
    return []

def save_runs(runs: list) -> None:
    if USE_BLOB:
        blob_write("automation_runs.json", runs)
    _cache_invalidate("automation_runs.json")

def append_run(record: dict) -> None:
    """Append a run record, trimming to RUN_LOG_MAX_ENTRIES."""
    runs = load_runs()
    runs.append(record)
    if len(runs) > RUN_LOG_MAX_ENTRIES:
        runs = runs[-RUN_LOG_MAX_ENTRIES:]
    save_runs(runs)

# ── Validators ────────────────────────────────────────────────────────────
# Pure functions returning (ok, error_message). Each endpoint that mutates
# state runs the matching validator first; on failure the request is
# rejected with a 400 and a helpful message.

def validate_shard(payload: dict, *, allow_partial: bool = False) -> tuple[bool, str]:
    """Validate a shard create/update payload. allow_partial=True relaxes
    the required-field check for PUT (where the existing record fills
    in missing fields)."""
    # Name
    name = (payload.get("name") or "").strip()
    if not name and not allow_partial:
        return False, "Shard name is required."
    if name and len(name) > 100:
        return False, "Shard name must be 100 characters or fewer."

    # Script (required even for partial updates if present)
    script = payload.get("script")
    if script is None and not allow_partial:
        return False, "Shard script is required."
    if script is not None:
        if script not in SCRIPT_CATALOG:
            return False, f"Unknown script {script!r}. Allowed: {sorted(SCRIPT_CATALOG.keys())}"

    # Platform must match the catalog's view of the script
    platform = payload.get("platform")
    if script is not None and platform is not None:
        expected = SCRIPT_CATALOG[script]["platform"]
        if platform != expected:
            return False, f"Platform {platform!r} doesn't match script {script!r} (expected {expected!r})."

    # Context: required keys must be present + non-empty
    if script is not None:
        ctx = payload.get("context") or {}
        if not isinstance(ctx, dict):
            return False, "Context must be an object/dict."
        for required_key in SCRIPT_CATALOG[script]["required_context"]:
            v = ctx.get(required_key)
            if v is None or (isinstance(v, str) and not v.strip()):
                return False, f"Script {script} requires context.{required_key}."

    # Output
    out = payload.get("output") or {}
    if out:
        # Block 7: support both legacy "format" (single string) and new
        # "formats" (list). Old shards saved before the multi-format change
        # keep working without migration.
        legacy_fmt  = out.get("format")
        formats_list = out.get("formats")

        if legacy_fmt is not None:
            if legacy_fmt not in VALID_OUTPUT_FORMATS:
                return False, f"Invalid output.format {legacy_fmt!r}. Allowed: {sorted(VALID_OUTPUT_FORMATS)}"

        if formats_list is not None:
            if not isinstance(formats_list, list):
                return False, "output.formats must be a list."
            if len(formats_list) == 0:
                return False, "output.formats must contain at least one format. Pick CSV, JSON, or both."
            for f in formats_list:
                if f not in VALID_OUTPUT_FORMATS:
                    return False, f"Invalid output.formats entry {f!r}. Allowed: {sorted(VALID_OUTPUT_FORMATS)}"

        # Custom filename — overrides script-natural filename when set
        custom_name = out.get("custom_name")
        if custom_name is not None:
            if not isinstance(custom_name, str):
                return False, "output.custom_name must be a string."
            cn = custom_name.strip()
            if cn:  # only validate format when non-empty
                if len(cn) > 100:
                    return False, "output.custom_name must be 100 characters or fewer."
                # Reject path separators + characters Windows forbids in filenames
                # (\ / : * ? " < > | plus null). Allowing dots/spaces/dashes is fine.
                bad_chars = set('\\/:*?"<>|\x00')
                if any(c in bad_chars for c in cn):
                    return False, ("output.custom_name contains invalid characters. "
                                   "Allowed: letters, numbers, dashes, underscores, dots, spaces.")

    # Destination
    dest = payload.get("destination") or {}
    if dest:
        dtype = dest.get("type")
        if dtype is not None and dtype not in VALID_DESTINATIONS:
            return False, f"Destination type {dtype!r} not yet supported. Available now: {sorted(VALID_DESTINATIONS)}"
        # Block 6A: optional local_path used by the cockpit-agent. Just
        # cap the length to avoid pathological inputs; we don't validate
        # path format because it might be Windows ('C:\\...'), POSIX,
        # or a UNC share. The agent itself handles invalid paths.
        local_path = dest.get("local_path")
        if local_path is not None:
            if not isinstance(local_path, str):
                return False, "destination.local_path must be a string."
            if len(local_path) > 500:
                return False, "destination.local_path must be 500 characters or fewer."

    return True, ""


def validate_compiler(payload: dict, *, allow_partial: bool = False) -> tuple[bool, str]:
    """Validate a compiler create/update payload."""
    name = (payload.get("name") or "").strip()
    if not name and not allow_partial:
        return False, "Compiler name is required."
    if name and len(name) > 100:
        return False, "Compiler name must be 100 characters or fewer."

    sched = payload.get("schedule")
    if sched is None and not allow_partial:
        return False, "Compiler schedule is required."
    if sched is not None:
        if not isinstance(sched, dict):
            return False, "Schedule must be an object/dict."
        freq = sched.get("frequency")
        if freq not in VALID_FREQUENCIES:
            return False, f"Invalid frequency {freq!r}. Allowed: {sorted(VALID_FREQUENCIES)}"
        # Time format HH:MM (24h)
        t = sched.get("time", "")
        if t:
            import re
            if not re.match(r"^[0-2]\d:[0-5]\d$", t):
                return False, f"Time must be HH:MM (24-hour). Got {t!r}."

    # shard_ids: optional (empty compilers are allowed); each ID must
    # exist + must not already belong to another compiler.
    shard_ids = payload.get("shard_ids")
    if shard_ids is not None:
        if not isinstance(shard_ids, list):
            return False, "shard_ids must be a list."
        if len(set(shard_ids)) != len(shard_ids):
            return False, "shard_ids contains duplicates."

    return True, ""

# ── Sync helper: keep shards' in_compiler field consistent ────────────────
# The reverse pointer (shard.in_compiler -> compiler.id) is a denormalised
# view of which shards belong to which compiler. We keep it in sync after
# any compiler write so the UI doesn't have to scan all compilers to
# answer "where is this shard?".

def _sync_shard_compiler_links() -> None:
    """Recompute every shard's in_compiler field by scanning all compilers.
    Called after compiler create/update/delete to keep the denormalised
    view consistent. O(shards * compilers) but both lists are small."""
    shards = load_shards()
    compilers = load_compilers()
    # Build authoritative shard_id -> compiler_id map
    shard_to_compiler: dict[str, str] = {}
    for c in compilers:
        for sid in c.get("shard_ids") or []:
            shard_to_compiler[sid] = c["id"]
    # Apply
    changed = False
    for s in shards:
        new_link = shard_to_compiler.get(s["id"])
        if s.get("in_compiler") != new_link:
            s["in_compiler"] = new_link
            changed = True
    if changed:
        save_shards(shards)


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

# Revizto license tier enum — values match the official Revizto API docs
# (POST /v5/license/{licenseUuid}/invite/bulk → role param):
#   1 = Guest
#   2 = Collaborator
#   3 = Content creator
#   4 = License administrator
#   5 = Super administrator (license owner — observed in /v5/license/{uuid}/team
#       data, NOT in the documented invite enum since you can't invite a
#       Super admin; they're created when the license is provisioned)
#
# Single source of truth across revizto_search_user, the browse sweep,
# and any future code that needs to translate the integer tier to a
# label. NOTE: prior to May 2026 this map had 0=SuperAdmin, 1=Admin,
# 2=Collaborator, 3=ContentCreator, 4=Guest — that ordering was wrong
# (an admin showed up as "Guest" in the browse table). The 5=Super admin
# slot was confirmed empirically against ALEC's licenses.
REVIZTO_LICENSE_ROLE_MAP = {
    1: "Guest",
    2: "Collaborator",
    3: "Content Creator",
    4: "License administrator",
    5: "Super administrator",
}


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
                        "uuid":         uuid,
                        "title":        p.get("title", "Unnamed"),
                        "region":       region,
                        # Tag each project with its parent license. The
                        # browse sweep needs this to fetch per-license
                        # team membership (one /v5/license/{uuid}/team
                        # call per unique license, NOT per project).
                        # Cheap to add here since we already have it.
                        "license_uuid": lic_uuid,
                        "license_name": lic_name,
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


def morta_set_user_tag(email: str, config: dict, hub_id: str = None,
                       new_tag_reference_id: str = None) -> dict:
    """Replace the user's tag on a Morta hub.

    Used by the browse edit flow. Locates the Morta user_id by email,
    removes any tags currently assigned to them on this hub, then
    optionally adds the new tag. Empty new_tag_reference_id means
    "remove all tags, leave untagged".

    Returns: {"status": "success" | "error", "detail": "..."}.

    Endpoint shapes (from Morta REST conventions and existing assign
    code at morta_invite):
      - GET    /v1/hub/{hub_id}/members         → list members + tags
      - DELETE /v1/user/{user_id}/tags/{tag_reference_id}  → remove
      - POST   /v1/user/{user_id}/tags          → add (existing path)

    The DELETE endpoint shape is the standard REST guess. If Morta uses
    a different verb/path the function logs a clear error and the edit
    is reported as failed (caller will revert the cache value).
    """
    api_key = config.get("morta", {}).get("api_key", "")
    hub_id  = hub_id or config.get("active_context", {}).get("morta_hub_id") or config.get("morta", {}).get("hub_id", "")
    if not api_key or not hub_id:
        return {"status": "error", "detail": "Morta not configured"}
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json", "Accept": "application/json"}

    # Step 1: find the user_id and any existing tags they hold on this hub.
    try:
        members_resp = http_requests.get(
            f"https://api.morta.io/v1/hub/{hub_id}/members",
            headers={"Authorization": f"Bearer {api_key}"}, timeout=10,
        )
    except Exception as e:
        return {"status": "error", "detail": f"members fetch raised: {e}"}
    if members_resp.status_code != 200:
        return {"status": "error", "detail": f"members fetch {members_resp.status_code}: {members_resp.text[:200]}"}

    user_id = ""
    existing_tag_ids: list = []
    for m in members_resp.json().get("data", []):
        u = m.get("user", {}) if isinstance(m, dict) else {}
        if (u.get("email") or "").strip().lower() == email.strip().lower():
            user_id = u.get("publicId", "") or ""
            # Tags on the membership object — Morta returns them as a list
            # of {"tagReferenceId": "...", "name": "..."} or similar.
            tags = m.get("tags") or u.get("tags") or []
            for t in tags:
                if isinstance(t, dict):
                    tid = t.get("tagReferenceId") or t.get("tag_reference_id") or t.get("id") or ""
                    if tid:
                        existing_tag_ids.append(tid)
            break
    if not user_id:
        return {"status": "error", "detail": f"User not found on hub: {email}"}

    # Step 2: remove all currently assigned tags. Each delete is best-effort —
    # if one fails (e.g. tag already removed by another admin), we keep going
    # rather than aborting the whole edit.
    delete_failures = []
    for tid in existing_tag_ids:
        # Don't bother deleting the tag we're about to re-add; Morta would
        # accept the redundant POST anyway, but skipping the round-trip keeps
        # the edit faster and avoids visible blink in the admin UI.
        if new_tag_reference_id and tid == new_tag_reference_id:
            continue
        try:
            del_url = f"https://api.morta.io/v1/user/{user_id}/tags/{tid}"
            d = http_requests.delete(del_url, headers={"Authorization": f"Bearer {api_key}"}, timeout=10)
            if d.status_code not in (200, 204):
                delete_failures.append(f"{tid[:8]}…: {d.status_code}")
        except Exception as e:
            delete_failures.append(f"{tid[:8]}…: {type(e).__name__}")

    # Step 3: add the new tag (if any).
    if new_tag_reference_id:
        # Skip add if the user already had this exact tag (kept it in step 2).
        if new_tag_reference_id not in existing_tag_ids:
            try:
                add = http_requests.post(
                    f"https://api.morta.io/v1/user/{user_id}/tags",
                    headers=headers,
                    json={"tagReferenceId": new_tag_reference_id},
                    timeout=10,
                )
                if add.status_code not in (200, 201):
                    return {"status": "error", "detail": f"tag add {add.status_code}: {add.text[:200]}"}
            except Exception as e:
                return {"status": "error", "detail": f"tag add raised: {e}"}

    if delete_failures:
        # Tag(s) were not removed cleanly — surface as a partial-success
        # warning. The new tag (if any) was assigned, but old ones may
        # linger. Admin can re-edit to retry.
        return {"status": "error", "detail": f"Could not remove old tag(s): {', '.join(delete_failures)}"}

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
                   admin_email: str = None,
                   preserve_roles: bool = True):
    """Invite a user to a Revizto license and/or project.

    The same /v5/license/{uuid}/invite/bulk endpoint is used for both
    inviting new users and editing existing ones — the behavior is
    controlled by `preserve_roles`:
      - True  (default, used by the invite flow): if the user already
              exists in the license, keep their current license role
              unchanged. Only company/department/location/auth get
              updated. This is the safe behavior for re-invites.
      - False (used by the browse edit flow): overwrite the existing
              user's license role, company, department, and location
              with the values supplied. This is how the browse-page
              "unlock → edit values → lock" flow updates an existing
              license member.

    Note `makeGuests=False` always — we never silently downgrade an
    Administrator to a Guest just because their email matches another
    license. The admin chose a role; we honor it.
    """
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
            "preserveRoles": preserve_roles, "makeGuests": False, "authMethod": auth_method, "deactivate": False,
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


def forma_update_user(user_id, config, project_id=None,
                      company_id: str = None,
                      role_ids: list = None,
                      products: list = None):
    """Update an existing user's company / roles / products on a Forma
    project. Documented endpoint:
        PATCH /construction/admin/v1/projects/{projectId}/users/{userId}
    Per the Autodesk Project Admin API, this writes back to ACC and is
    visible immediately on the Forma website. forma_invite() (the
    :import endpoint) does NOT reliably update existing users — that's
    why edits weren't landing before.

    Strip `b.` from the project_id if present (Forma stores project_ids
    with this BIM 360 prefix on some accounts; the admin v1 endpoint
    rejects it).

    Only fields explicitly passed are updated; passing None for a field
    leaves it unchanged. The admin v1 endpoint expects:
        {"companyId": "...", "roleIds": ["..."], "products": [...]}
    Returns: {"status": "success" | "error", "detail": "..."}.
    """
    fc  = config["forma"]
    pid = project_id or fc.get("project_id", "")
    if not fc.get("admin_user_id") or not pid or not user_id:
        return {"status": "error", "detail": "Forma update: missing admin_user_id, project_id, or user_id"}
    token = forma_get_token(config)
    if not token:
        return {"status": "error", "detail": "Forma auth failed"}

    clean_pid = pid.lstrip("b.") if pid.startswith("b.") else pid
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
        "User-Id":       fc["admin_user_id"],
    }

    body = {}
    if company_id is not None:
        body["companyId"] = company_id or None  # explicit None clears it
    if role_ids is not None:
        body["roleIds"] = role_ids
    if products is not None:
        body["products"] = products
    if not body:
        return {"status": "success"}  # nothing to update — no-op

    url = f"https://developer.api.autodesk.com/construction/admin/v1/projects/{clean_pid}/users/{user_id}"
    try:
        response = http_requests.patch(url, headers=headers, json=body, timeout=30)
    except Exception as e:
        return {"status": "error", "detail": f"PATCH raised: {type(e).__name__}: {e}"}
    if response.status_code in (200, 202, 204):
        return {"status": "success"}
    return {"status": "error", "detail": f"PATCH {response.status_code}: {response.text[:200]}"}


# ---------------------------------------------------------------------------
# Platform Helpers — Search
# ---------------------------------------------------------------------------

def _populate_invited_user_ids(row, email, platform, project_key, proj, config, admin_email):
    """After a successful invite, fetch the freshly-added user's IDs from
    the platform and write them into row[platform]. This is what makes
    the same-session invite→deactivate flow work: without it, the row
    has empty user_uuid/user_id/firebase_id and a follow-up deactivate
    would fail with "missing on row — refresh first".

    Each platform requires a different lookup:
      - Revizto: GET /v5/project/{uuid}/team paginated, find entity by email
      - Forma:   GET /admin/v1/projects/{pid}/users paginated, find by email
      - Morta:   GET /v1/hub/{hub_id}/members paginated, find by email

    SAFETY BOUNDS to prevent server hangs:
      - Hard cap of 10 pages (so worst case is 10 × 200 = 2000 records
        scanned, far enough for any project but bounded). The user we
        just invited was added moments ago and is almost certainly on
        page 1 or 2 anyway.
      - Per-request timeout of 8s (tighter than the typical 15s) so a
        slow-responding platform can't extend the chain.
      - Total budget enforced via wall-clock check between pages.

    Failures are swallowed silently — populating IDs is a best-effort
    enhancement. If lookup fails, the row's IDs remain empty and the
    admin will get the existing "refresh first" UX on next deactivate.
    Logged so it's debuggable from Azure log stream.

    Returns nothing — mutates row in place.
    """
    plats = (proj.get("platforms") or {})
    sub = row.get(platform) or {}
    MAX_PAGES = 10
    MAX_TOTAL_SECONDS = 20.0
    PER_REQUEST_TIMEOUT = 8
    started = time.time()

    def _budget_exhausted():
        return (time.time() - started) > MAX_TOTAL_SECONDS

    print(f"[edit-postinvite] starting {platform} lookup for {email}")

    try:
        if platform == "revizto":
            project_uuid = (plats.get("revizto") or {}).get("project_uuid", "")
            region       = (plats.get("revizto") or {}).get("region", "dubai")
            if not project_uuid:
                print(f"[edit-postinvite] revizto: no project_uuid in registry, aborting")
                return
            token = revizto_get_token_for_region(config, region, email=admin_email)
            if not token:
                print(f"[edit-postinvite] revizto: token fetch failed, aborting")
                return
            # The /v5/project/{uuid}/team endpoint is NOT paginated — every
            # other call site (sweep, search) treats it as a single
            # request. Match that pattern exactly to avoid response-shape
            # surprises from unrecognized params.
            url = f"https://api.{region}.revizto.com/v5/project/{project_uuid}/team"
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
            target = email.lower()
            try:
                resp = http_requests.get(url, headers=headers, timeout=PER_REQUEST_TIMEOUT)
            except Exception as e:
                print(f"[edit-postinvite] revizto /team request raised: {type(e).__name__}: {e}")
                return
            if resp.status_code != 200:
                print(f"[edit-postinvite] revizto /team status={resp.status_code}: {resp.text[:120]}")
                return
            try:
                body = resp.json()
            except Exception as e:
                print(f"[edit-postinvite] revizto /team JSON parse raised: {type(e).__name__}: {e}")
                return
            if body.get("result", 0) != 0:
                print(f"[edit-postinvite] revizto /team result={body.get('result')}, aborting")
                return
            entities = (body.get("data") or {}).get("entities") or []
            print(f"[edit-postinvite] revizto /team returned {len(entities)} entities; searching for {target}")
            for ent in entities:
                em = (ent.get("email") or "").strip().lower()
                if em != target:
                    continue
                # Per Revizto docs: real member UUID is at entity.member.uuid,
                # user UUID at entity.member.user.uuid. Old fallbacks kept for
                # API drift safety.
                member_obj  = ent.get("member") if isinstance(ent.get("member"), dict) else {}
                member_uuid = (member_obj.get("uuid") or "") or (ent.get("uuid") or "") or ""
                user_obj    = member_obj.get("user") if isinstance(member_obj.get("user"), dict) else {}
                user_uuid   = (user_obj.get("uuid") or "") or ""
                if not user_uuid:
                    legacy_user = ent.get("user") if isinstance(ent.get("user"), dict) else {}
                    user_uuid   = (legacy_user.get("uuid") or "") or ""
                pending     = bool(ent.get("invited")) or bool(ent.get("pending"))
                sub["user_uuid"]   = user_uuid
                sub["member_uuid"] = member_uuid
                sub["pending"]     = pending
                row["revizto"]     = sub
                print(f"[edit-postinvite] revizto MATCH for {target}: member={member_uuid[:8] if member_uuid else '(none)'}, user={user_uuid[:8] if user_uuid else '(none)'}, pending={pending}")
                return
            print(f"[edit-postinvite] revizto: {target} not found in {len(entities)}-entity team — invite may not have propagated yet")

        elif platform == "forma":
            project_id = (plats.get("forma") or {}).get("project_id", "")
            if not project_id:
                print(f"[edit-postinvite] forma: no project_id in registry, aborting")
                return
            token = forma_get_token(config)
            if not token:
                print(f"[edit-postinvite] forma: token fetch failed, aborting")
                return
            # Match the sweep's pattern exactly: raw fpid (not stripped),
            # paginated with limit/offset. The sweep at line ~4513 works
            # in production with this shape.
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
            target = email.lower()
            total_scanned = 0
            for page in range(MAX_PAGES):
                if _budget_exhausted():
                    print(f"[edit-postinvite] forma lookup budget exhausted at page {page}, scanned {total_scanned}")
                    return
                try:
                    resp = http_requests.get(
                        f"https://developer.api.autodesk.com/construction/admin/v1/projects/{project_id}/users",
                        headers=headers,
                        params={"limit": 200, "offset": page * 200},
                        timeout=PER_REQUEST_TIMEOUT,
                    )
                except Exception as e:
                    print(f"[edit-postinvite] forma /users page {page} raised: {type(e).__name__}: {e}")
                    return
                if resp.status_code != 200:
                    print(f"[edit-postinvite] forma /users page {page} status={resp.status_code}: {resp.text[:120]}")
                    return
                batch = (resp.json() or {}).get("results") or []
                if not batch:
                    break
                total_scanned += len(batch)
                for u in batch:
                    if (u.get("email") or "").lower() != target:
                        continue
                    user_id = u.get("id") or u.get("autodeskId") or ""
                    sub["user_id"] = user_id
                    row["forma"]   = sub
                    print(f"[edit-postinvite] forma MATCH for {target} on page {page}: user_id={user_id}")
                    return
                if len(batch) < 200:
                    break
            print(f"[edit-postinvite] forma: {target} not found in {total_scanned} users scanned — invite may not have propagated yet")

        elif platform == "morta":
            hub_id = (plats.get("morta") or {}).get("hub_id", "")
            if not hub_id:
                print(f"[edit-postinvite] morta: no hub_id in registry, aborting")
                return
            api_key = (config.get("morta") or {}).get("api_key", "")
            if not api_key:
                print(f"[edit-postinvite] morta: no api_key in config, aborting")
                return
            target = email.lower()
            total_scanned = 0
            for page in range(MAX_PAGES):
                if _budget_exhausted():
                    print(f"[edit-postinvite] morta lookup budget exhausted at page {page}, scanned {total_scanned}")
                    return
                try:
                    resp = http_requests.get(
                        f"https://api.morta.io/v1/hub/{hub_id}/members",
                        headers={"Authorization": f"Bearer {api_key}"},
                        params={"page": page, "size": 500},
                        timeout=PER_REQUEST_TIMEOUT,
                    )
                except Exception as e:
                    print(f"[edit-postinvite] morta /members page {page} raised: {type(e).__name__}: {e}")
                    return
                if resp.status_code != 200:
                    print(f"[edit-postinvite] morta /members page {page} status={resp.status_code}: {resp.text[:120]}")
                    return
                data = resp.json() or {}
                members = data.get("data") or data.get("members") or data.get("results") or []
                if not members:
                    break
                total_scanned += len(members)
                for m in members:
                    u = m.get("user") if isinstance(m.get("user"), dict) else {}
                    if (u.get("email") or "").lower() != target:
                        continue
                    sub["public_id"]   = u.get("publicId", "") or sub.get("public_id", "")
                    sub["firebase_id"] = u.get("firebaseUserId", "") or sub.get("firebase_id", "")
                    row["morta"]       = sub
                    print(f"[edit-postinvite] morta MATCH for {target} on page {page}: firebase={(sub.get('firebase_id') or '')[:8]}")
                    return
                if len(members) < 500:
                    break
            print(f"[edit-postinvite] morta: {target} not found in {total_scanned} members scanned")

    except Exception as e:
        import traceback
        print(f"[edit-postinvite] {platform} lookup raised {type(e).__name__}: {e}")
        print(f"[edit-postinvite] traceback:\n{traceback.format_exc()}")


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
    """Search for a user in Revizto using a license-based project sweep.

    Mirrors the working extraction pattern (Revizto_GET_License_Projects.py
    and fetch_revizto_projects above):
      1. GET /v5/user/licenses           — all licenses the *admin* can see
      2. GET /v5/project/list/{lic}/paged — every project under each license
      3. GET /v5/project/{uuid}/team      — check membership per project

    The earlier registry-only sweep silently missed projects whose
    revizto.project_uuid wasn't filled in yet — admins saw fewer projects
    than the extraction scripts did. This version walks the admin's actual
    license access, so any project the admin can see in Revizto is a
    candidate, regardless of registry completeness.

    `allowed_project_uuids` and `registry_projects` are still accepted for
    backward compatibility but are now unused — search scope is the
    admin's license access. Owner and admin both see the same scope here;
    the separation of concerns ("admins see fewer projects") is enforced
    elsewhere (e.g. the project picker in base.html).

    Returns None if the user isn't found in any license-accessible project;
    otherwise returns the same shape as before:
      { found, region, name, email, license_role, status, company,
        department, last_active, projects: [{uuid, name, region, role,
        license_role, company, department, location}, ...] }
    """
    # License tier enum — names match the Revizto web UI (License → Members
    # screen) and the invite dropdown (Administrator / Content creator /
    # Collaborator / Guest). The "Super administrator" tier (license owner)
    # only appears on the web UI for the user who owns the license; rare in
    # practice. Mapping lives at module level — REVIZTO_LICENSE_ROLE_MAP —
    # so the browse sweep and any future caller share one source of truth.
    role_map_license = REVIZTO_LICENSE_ROLE_MAP

    # Token cache shared across phases. Phase 1+2 (license / project list
    # enumeration) populates it serially, one entry per region, so phase 3
    # (parallel /team checks across 8 worker threads) never has to fetch a
    # token concurrently. Concurrent fetches were the root cause of the
    # "no token" errors we saw for 3 of 5 projects — under parallel load,
    # multiple threads raced into revizto_get_token_for_region simultaneously
    # and some came back empty before the canonical fetch finished.
    tokens: dict = {}   # region -> token string ("" means tried-and-failed)

    # Diagnostic trace — attached to the response so the UI can surface why
    # certain projects didn't show up. Captures: regions probed, license
    # status, projects per license, /team status per project, and the final
    # shortlist of projects where `email` matched. The whole structure is
    # safe to log on the client (no secrets, only API outcomes).
    debug = {
        "searched_email":   email,
        "regions":          {},   # region -> {token_ok, licenses_status, licenses_count, licenses: [...]}
        "projects_total":   0,
        "projects_matched": 0,
        "match_details":    [],   # [{uuid, name, region, license_name}] for each match
    }

    # Step 1+2: enumerate all (project_uuid, region, name) triples across
    # both regions by walking the admin's licenses. Per-region token cache.
    project_pool: list = []   # [{uuid, region, name, license_uuid, license_name}]
    seen_uuids = set()

    for region in ["dubai", "ireland"]:
        rdbg = {"token_ok": False, "licenses_status": None, "licenses_count": 0, "licenses": []}
        debug["regions"][region] = rdbg

        rc = config.get(f"revizto_{region}", {})
        has_tokens = bool(load_revizto_tokens(region, admin_email)) if admin_email else (BASE_DIR / f"revizto_tokens_{region}.json").exists()
        if not rc.get("access_code") and not has_tokens:
            rdbg["skip_reason"] = "no access_code and no saved tokens"
            tokens[region] = ""
            continue
        try:
            token = revizto_get_token_for_region(config, region, email=admin_email)
        except Exception as e:
            print(f"[revizto-search] token fetch for {region} raised: {e}")
            rdbg["skip_reason"] = f"token fetch raised: {type(e).__name__}"
            tokens[region] = ""
            continue
        if not token:
            rdbg["skip_reason"] = "token fetch returned empty"
            tokens[region] = ""
            continue
        rdbg["token_ok"] = True
        # Stash the working token so phase 3 reuses it instead of re-fetching
        # under parallel load.
        tokens[region] = token

        base_url = f"https://api.{region}.revizto.com/v5"
        headers  = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        # Licenses the admin has access to in this region
        try:
            r = http_requests.get(f"{base_url}/user/licenses", headers=headers, timeout=15)
        except Exception as e:
            print(f"[revizto-search] /user/licenses {region} raised: {e}")
            rdbg["skip_reason"] = f"/user/licenses raised: {type(e).__name__}"
            continue
        rdbg["licenses_status"] = r.status_code
        if r.status_code != 200:
            continue
        try:
            licenses = r.json()["data"]["entities"]
        except (KeyError, TypeError):
            rdbg["skip_reason"] = "/user/licenses unexpected shape"
            continue
        rdbg["licenses_count"] = len(licenses)

        # Projects under each license
        for lic in licenses:
            lic_uuid = lic.get("uuid", "")
            lic_name = lic.get("name", lic_uuid)
            ldbg = {"name": lic_name, "uuid": lic_uuid, "projects_status": None, "projects_count": 0}
            rdbg["licenses"].append(ldbg)
            if not lic_uuid:
                ldbg["skip_reason"] = "no license uuid"
                continue
            try:
                r2 = http_requests.get(
                    f"{base_url}/project/list/{lic_uuid}/paged",
                    headers=headers, timeout=15
                )
            except Exception as e:
                print(f"[revizto-search] /project/list {lic_name} raised: {e}")
                ldbg["skip_reason"] = f"/project/list raised: {type(e).__name__}"
                continue
            ldbg["projects_status"] = r2.status_code
            if r2.status_code != 200:
                continue
            try:
                projects = r2.json()["data"]["data"]
            except (KeyError, TypeError):
                ldbg["skip_reason"] = "/project/list unexpected shape"
                continue
            ldbg["projects_count"] = len(projects)
            ldbg["project_titles"] = [p.get("title", "") for p in projects]
            for p in projects:
                uuid = p.get("uuid", "")
                if uuid and uuid not in seen_uuids:
                    seen_uuids.add(uuid)
                    project_pool.append({
                        "uuid":         uuid,
                        "region":       region,
                        "name":         p.get("title", uuid),
                        "license_uuid": lic_uuid,
                        "license_name": lic_name,
                    })

    debug["projects_total"] = len(project_pool)

    if not project_pool:
        # Wrap a no-results "not found" so the debug still surfaces. The
        # caller compares to None for "found", so we return a stub record
        # that's clearly not-found but carries debug.
        return None

    # ── Phase 2.5: license-level team fetch ────────────────────────────
    # The /v5/project/{uuid}/team entity does NOT carry license tier — its
    # `member.role` field is the project access level, not the license
    # tier. To get the actual license role (Administrator / Collaborator /
    # Content Creator / Guest, matching the Revizto web UI's License →
    # Members screen), we fetch /v5/license/{uuid}/team once per unique
    # license UUID and build an email → tier-int index. This is the
    # license-scoped sibling of /project/.../team and per Revizto's
    # consistent /team naming should return entities[] with a per-user
    # `role` (or `licenseRole`) field carrying the integer tier.
    #
    # If the endpoint 404s, we leave the index empty and License Role
    # falls back to blank. Same defensive posture as everywhere else.
    license_role_by_email: dict = {}   # email-lower -> int tier
    debug["license_team"] = {}

    unique_licenses = {}   # (lic_uuid, region) -> license_name (for debug)
    for proj in project_pool:
        key = (proj["license_uuid"], proj["region"])
        unique_licenses.setdefault(key, proj["license_name"])

    for (lic_uuid, region), lic_name in unique_licenses.items():
        ldbg = {"license_name": lic_name, "status": None, "members_count": 0}
        debug["license_team"][lic_uuid] = ldbg
        token = tokens.get(region, "")
        if not token:
            ldbg["error"] = "no token for region"
            continue
        url = f"https://api.{region}.revizto.com/v5/license/{lic_uuid}/team"
        headers  = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        try:
            lr = http_requests.get(url, headers=headers, timeout=15)
        except Exception as e:
            ldbg["error"] = f"raised: {type(e).__name__}"
            continue
        ldbg["status"] = lr.status_code
        if lr.status_code != 200:
            continue
        try:
            body = lr.json()
        except Exception:
            ldbg["error"] = "json parse failed"
            continue
        if body.get("result", 0) != 0:
            ldbg["error"] = f"result={body.get('result')}"
            continue
        ents = (body.get("data") or {}).get("entities") or []
        if not isinstance(ents, list):
            continue
        ldbg["members_count"] = len(ents)
        # Capture the searched user's full entity once for diagnosis — lets
        # us see exactly which field carries the tier integer without
        # dumping every member.
        for ent in ents:
            if not isinstance(ent, dict):
                continue
            ent_email = (ent.get("email") or "").lower()
            if not ent_email:
                # Try nested user field (mirrors the GET_License_Members script)
                user_sub = ent.get("user") if isinstance(ent.get("user"), dict) else {}
                ent_email = (user_sub.get("email") or "").lower()
            if not ent_email:
                continue
            # Resolution order for license tier: try several plausible field
            # names. We don't know the exact one for /license/.../team, but
            # `role` and `licenseRole` are the obvious candidates by
            # consistency with the rest of the API.
            tier = None
            for k in ("role", "licenseRole", "license_role", "tier"):
                v = ent.get(k)
                if isinstance(v, int):
                    tier = v
                    break
            if tier is not None:
                license_role_by_email[ent_email] = tier
            # Capture the searched user's raw entity once for visibility
            if ent_email == email.lower() and "matched_entity" not in ldbg:
                ldbg["matched_entity"] = {k: ent[k] for k in sorted(ent.keys())
                                           if not isinstance(ent[k], (list, dict))}

    debug["license_role_index_size"] = len(license_role_by_email)

    # Phase 3 token getter: read from the shared cache populated above.
    # Returns "" if the region had no usable token (skipped or failed).
    # No fetch happens here — phase 1+2 is the only place tokens get
    # acquired, so there's no race window when multiple worker threads
    # call this concurrently.
    def get_token(region: str) -> str:
        return tokens.get(region, "")

    # Step 3: check membership on each project in parallel.
    # Returns a dict with team_status + match details so the caller can
    # populate the debug trace.
    def check_project(proj: dict):
        """Return {project_match, user_record, team_status} on hit; on miss
        returns {project_match: None, team_status, team_count, error?}."""
        region = proj["region"]
        token  = get_token(region)
        if not token:
            return {"project_match": None, "team_status": None, "error": "no token"}
        base_url = f"https://api.{region}.revizto.com/v5"
        headers  = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        try:
            pr = http_requests.get(
                f"{base_url}/project/{proj['uuid']}/team",
                headers=headers, timeout=10
            )
        except Exception as e:
            print(f"[revizto-search] /team {proj['uuid'][:8]} raised: {e}")
            return {"project_match": None, "team_status": None, "error": f"raised: {type(e).__name__}"}
        if pr.status_code != 200:
            return {"project_match": None, "team_status": pr.status_code, "error": f"http {pr.status_code}"}
        try:
            body = pr.json()
        except Exception:
            return {"project_match": None, "team_status": pr.status_code, "error": "json parse failed"}
        if body.get("result", 0) != 0:
            return {"project_match": None, "team_status": pr.status_code, "error": f"result={body.get('result')}"}
        entities = (body.get("data") or {}).get("entities") or []
        if not isinstance(entities, list):
            return {"project_match": None, "team_status": pr.status_code, "error": "entities not a list"}
        for ent in entities:
            if not isinstance(ent, dict):
                continue
            if (ent.get("email") or "").lower() != email.lower():
                continue
            access_role = ent.get("accessRole") or {}
            project_role_name = (
                access_role.get("name", "") if isinstance(access_role, dict)
                else str(access_role or "")
            )
            # License tier resolution — read from the per-license email index
            # built in phase 2.5 (sourced from /license/{uuid}/team). This
            # is the only known place the license-level role lives; the
            # /project/{uuid}/team entity carries only the project access
            # level under member.role, which is a separate concept.
            license_role_id = license_role_by_email.get(email.lower())
            license_role_label = role_map_license.get(license_role_id, "") if license_role_id is not None else ""
            project_match = {
                "uuid":         proj["uuid"],
                "name":         proj["name"],
                "region":       proj["region"],
                "role":         project_role_name,
                "license_role": license_role_label,
                "company":      ent.get("company", ""),
                "department":   ent.get("department", ""),
                "location":     ent.get("location", ""),
            }
            user_record = {
                "found":        True,
                "region":       proj["region"],
                "license_uuid": proj["license_uuid"],
                "name":         ent.get("fullname", "")
                                  or (f"{ent.get('firstname','')} {ent.get('lastname','')}".strip()),
                "email":        ent.get("email", ""),
                # license_role here is for the user-record summary; never
                # fall back to project_role_name — that's what caused
                # license_role to silently show "BIM Coordinator" for users
                # whose actual license tier the API didn't return.
                "license_role": license_role_label,
                "status":       (
                    "deactivated" if ent.get("frozen")
                    else "active" if ent.get("activated")
                    else "pending"
                ),
                "company":      ent.get("company", ""),
                "department":   ent.get("department", ""),
                "invited_at":   ent.get("invitedAt", ""),
                "last_active":  ent.get("lastActive", ""),
                "user_uuid":    ent.get("uuid", ""),
                "projects":     [],
            }
            return {"project_match": project_match, "user_record": user_record,
                    "team_status": pr.status_code, "team_count": len(entities)}
        return {"project_match": None, "team_status": pr.status_code,
                "team_count": len(entities), "error": "email not in /team"}

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(check_project, project_pool))

    # Build per-project debug detail — every project we tried, with outcome.
    debug["projects_checked"] = []
    for proj, res in zip(project_pool, results):
        debug["projects_checked"].append({
            "name":         proj["name"],
            "uuid":         proj["uuid"],
            "region":       proj["region"],
            "license_name": proj["license_name"],
            "team_status":  res.get("team_status"),
            "team_count":   res.get("team_count"),
            "matched":      res.get("project_match") is not None,
            "error":        res.get("error"),
        })

    user_record = None
    project_hits = []
    for res in results:
        # results is always a list of dicts now (never None). Skip ones
        # where the email wasn't on /team.
        if not res or not res.get("project_match"):
            continue
        if user_record is None:
            user_record = res["user_record"]
        project_hits.append(res["project_match"])
        debug["match_details"].append({
            "name":   res["project_match"]["name"],
            "uuid":   res["project_match"]["uuid"],
            "region": res["project_match"]["region"],
        })

    debug["projects_matched"] = len(project_hits)

    # Always log the debug trace to server stdout so we can diagnose why a
    # particular search didn't return what was expected. Goes to Azure log
    # stream — visible in the portal's Log Stream tab.
    try:
        import json as _json
        print(f"[revizto-search] DEBUG: {_json.dumps(debug, default=str)[:4000]}")
    except Exception:
        pass

    if user_record is None:
        return None

    user_record["projects"] = project_hits
    user_record["_debug"]   = debug
    return user_record


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

    # Check project membership — only within registry-allowed projects.
    # The /projects/{id}/users endpoint returns user records with:
    #   email, status, accessLevels, addedOn, companyId, roles[], products[]
    # We pass through the raw IDs; the search endpoint resolves them to
    # display names using the registry's forma.companies / forma.roles
    # lookups (which the manage.html invite table also uses).
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
                    # Roles can come back as full {id, name} pairs or just
                    # role IDs. Capture both — the search endpoint will
                    # reconcile against the registry's role list.
                    roles_raw = u.get("roles") or u.get("industryRoles") or []
                    role_ids = []
                    role_names_inline = []
                    if isinstance(roles_raw, list):
                        for ro in roles_raw:
                            if isinstance(ro, dict):
                                rid = ro.get("id") or ro.get("roleId") or ""
                                rnm = ro.get("name") or ""
                                if rid: role_ids.append(rid)
                                if rnm: role_names_inline.append(rnm)
                            elif isinstance(ro, str):
                                role_ids.append(ro)
                    # Products — list of {key, access} per the invite payload.
                    # ACC may return "products" with the same shape, or a
                    # list of strings. Normalize to a list of keys for the UI.
                    products_raw = u.get("products") or []
                    product_keys = []
                    if isinstance(products_raw, list):
                        for p in products_raw:
                            if isinstance(p, dict) and p.get("key"):
                                product_keys.append(p["key"])
                            elif isinstance(p, str):
                                product_keys.append(p)
                    return {
                        "project_id":        project_id,
                        "status":            u.get("status", ""),
                        "access_levels":     ", ".join(access) if access else "member",
                        "added_on":          u.get("addedOn", ""),
                        "company_id":        u.get("companyId", "") or "",
                        "role_ids":          role_ids,
                        "role_names_inline": role_names_inline,
                        "product_keys":      product_keys,
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
                u_location = u.get("location", "")
                r = revizto_invite(email, first_name, last_name, company, department, config,
                                   project_uuid=revizto_uuid, license_role_id=u_rev_lic_role,
                                   project_role_id=u_rev_proj_role, invite_to_license=u_rev_to_license,
                                   invite_to_project=u_rev_to_project, location=u_location,
                                   admin_email=user)
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

    Performance: registry is small (~90 KB) but load_registry() does a
    SYNC blob read on cache miss. When the browse cache is also missing
    its in-memory entry, both blob reads can stack up on the same event
    loop and stall every other request. Run on a worker thread.
    """
    loop = asyncio.get_event_loop()
    registry = await loop.run_in_executor(None, load_registry)
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

# ─────────────────────────────────────────────────────────────────────
# BROWSE USERS — license-wide grid view (phase 1+2: read-only)
#
# The Manage page is search-driven (one email at a time). Browse Users
# inverts that: load every user across every license + platform, let the
# admin filter & inspect. Inline edit (phase 3) is intentionally not in
# this deploy — that's a separate write-path with audit + per-platform
# PATCH endpoints, scoped in a follow-up.
#
# Three routes:
#   GET /browse                    — page shell
#   GET /api/browse/projects       — filter dropdown options
#   GET /api/browse/users          — aggregated user list across platforms
# ─────────────────────────────────────────────────────────────────────

# ---------------------------------------------------------------------------
# Browse Users — shared cache + deactivation snapshots
# ---------------------------------------------------------------------------
# Two blobs in cockpit-data:
#
#   browse_users_cache.json       — shared row cache, keyed (email,project_key)
#   deactivation_snapshots.json   — per (email,platform,project_key) snapshot
#                                   used to reactivate with original fields
#
# Both shared across all admins. Reads cached via existing TTL layer; writes
# invalidate the matching cache key. Refresh logic does delete-and-replace
# scoped to the calling admin's swept project_keys.

def load_browse_cache() -> dict:
    """Return the shared browse-users cache.
    Schema: { rows: [...], updated_at: iso, updated_by: email }"""
    cached = _cache_get("browse_users_cache.json")
    if cached is not None:
        return cached
    if USE_BLOB:
        data = blob_read("browse_users_cache.json")
        if data is not None:
            _cache_set("browse_users_cache.json", data)
            return data
    p = BASE_DIR / "browse_users_cache.json"
    if p.exists():
        with open(p) as f:
            data = json.load(f)
            _cache_set("browse_users_cache.json", data)
            return data
    empty = {"rows": [], "updated_at": "", "updated_by": ""}
    _cache_set("browse_users_cache.json", empty)
    return empty


def save_browse_cache(data: dict):
    if USE_BLOB:
        blob_write("browse_users_cache.json", data)
    else:
        with open(BASE_DIR / "browse_users_cache.json", "w") as f:
            json.dump(data, f, indent=2)
    _cache_invalidate("browse_users_cache.json")


def load_deactivation_snapshots() -> dict:
    """Return all deactivation snapshots.
    Key shape: 'email|platform|project_key' → preserved row sub-object."""
    cached = _cache_get("deactivation_snapshots.json")
    if cached is not None:
        return cached
    if USE_BLOB:
        data = blob_read("deactivation_snapshots.json")
        if data is not None:
            _cache_set("deactivation_snapshots.json", data)
            return data
    p = BASE_DIR / "deactivation_snapshots.json"
    if p.exists():
        with open(p) as f:
            data = json.load(f)
            _cache_set("deactivation_snapshots.json", data)
            return data
    _cache_set("deactivation_snapshots.json", {})
    return {}


def save_deactivation_snapshots(data: dict):
    if USE_BLOB:
        blob_write("deactivation_snapshots.json", data)
    else:
        with open(BASE_DIR / "deactivation_snapshots.json", "w") as f:
            json.dump(data, f, indent=2)
    _cache_invalidate("deactivation_snapshots.json")


def _snapshot_key(email: str, platform: str, project_key: str) -> str:
    return f"{email.lower()}|{platform}|{project_key}"


def _sweep_browse_rows(user: str):
    """Sweep the calling admin's accessible projects across all three
    platforms. Returns (rows, project_keys_swept).

    Used by /api/browse/refresh to populate the shared cache. Each
    project_key in `project_keys_swept` will be delete-and-replaced
    in the cache with the new rows from this sweep.
    """
    config = load_config(user)
    rows_by_key = {}
    project_keys_swept = set()

    registry = load_registry()
    admin_projects = registry if is_owner(user) else {
        k: v for k, v in registry.items() if user in v.get("admins", [])
    }
    rev_uuid_to_key  = {}
    forma_id_to_key  = {}
    morta_hub_to_key = {}
    for k, rp in admin_projects.items():
        plats = rp.get("platforms", {}) or {}
        rev   = plats.get("revizto", {}) or {}
        forma = plats.get("forma", {})   or {}
        morta = plats.get("morta", {})   or {}
        if rev.get("project_uuid"):
            rev_uuid_to_key[rev["project_uuid"]] = k
        if forma.get("project_id"):
            forma_id_to_key[forma["project_id"]] = k
        mhub = morta.get("hub_id") or morta.get("project_id")
        if mhub:
            morta_hub_to_key[mhub] = k

    def upsert_row(email, project_key, project_name, **fields):
        if not email or not project_key:
            return None
        key = (email.lower(), project_key)
        rec = rows_by_key.get(key)
        if rec is None:
            rec = {
                "email":            email,
                "name":             "",
                "project_key":      project_key,
                "project_name":     project_name,
                "revizto":          None,
                "forma":            None,
                "morta":            None,
                "last_active_ts":   "",
                "last_active_days": None,
            }
            rows_by_key[key] = rec
        for fk, fv in fields.items():
            if fk == "name" and fv and not rec["name"]:
                rec["name"] = fv
        return rec

    # ── Revizto sweep ──────────────────────────────────────────────
    # Two-layer fetch per region:
    #   (a) /v5/license/{license_uuid}/team — license-level membership
    #       (who holds a seat) and license tier (Admin / Collaborator /
    #       Content Creator / Guest). The ONLY source for license_role
    #       and on_license. Hit ONCE PER UNIQUE LICENSE — not per region,
    #       not per project. fetch_revizto_projects now tags each project
    #       with its parent license_uuid so we can group projects by
    #       license without re-fetching the license list.
    #   (b) /v5/project/{uuid}/team — project members. accessRole here
    #       is the project role ("BIM Coordinator", etc.), not license
    #       tier. This is what the Project Role column shows.
    #
    # If the license-team call for a given license fails or returns
    # zero members, we still emit project rows from that license but
    # leave license_role blank and on_license=False — admins see
    # "L=· P=✓" rather than being told everyone holds a seat.
    #
    # Every branch logs explicitly. Silent failures are NOT acceptable
    # because the v1 attempt died silently and dropped 55% of rows
    # before we noticed.
    try:
        rev_projects = fetch_revizto_projects(config, email=user) or []
        rev_tokens = {}
        # (region, license_uuid) -> { email_lower: tier_int }
        license_team_index: dict = {}

        def _ensure_license_team(region: str, license_uuid: str, license_name: str, token: str):
            """Fetch /v5/license/{uuid}/team for this specific license once.
            Cached per (region, license_uuid). Always returns a dict (may
            be empty). Logs the outcome on every branch."""
            cache_key = (region, license_uuid)
            if cache_key in license_team_index:
                return license_team_index[cache_key]
            index: dict = {}
            license_team_index[cache_key] = index
            if not token:
                print(f"[browse-sweep] revizto {region} '{license_name}': no token, skipping license-team fetch")
                return index
            if not license_uuid:
                print(f"[browse-sweep] revizto {region} '{license_name}': no license_uuid, skipping")
                return index
            url = f"https://api.{region}.revizto.com/v5/license/{license_uuid}/team"
            try:
                # 10s timeout (down from 15s) — license teams can be large
                # but the request budget on App Service is ~30s total. With
                # up to 5 licenses to fetch we need to keep each one snappy.
                lr = http_requests.get(
                    url,
                    headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                    timeout=10,
                )
            except Exception as e:
                print(f"[browse-sweep] revizto {region} '{license_name}' /license/team raised: {type(e).__name__}: {e}")
                return index
            if lr.status_code != 200:
                print(f"[browse-sweep] revizto {region} '{license_name}' /license/team status={lr.status_code} body={lr.text[:200]}")
                return index
            try:
                body = lr.json()
            except Exception as e:
                print(f"[browse-sweep] revizto {region} '{license_name}' /license/team json parse failed: {e}")
                return index
            api_result = body.get("result", 0)
            if api_result != 0:
                print(f"[browse-sweep] revizto {region} '{license_name}' /license/team result={api_result}")
                return index
            ents = (body.get("data") or {}).get("entities") or []
            if not isinstance(ents, list):
                print(f"[browse-sweep] revizto {region} '{license_name}' /license/team entities not a list")
                return index
            for ent in ents:
                if not isinstance(ent, dict):
                    continue
                ent_email = (ent.get("email") or "").strip().lower()
                if not ent_email:
                    user_sub = ent.get("user") if isinstance(ent.get("user"), dict) else {}
                    ent_email = (user_sub.get("email") or "").strip().lower()
                if not ent_email:
                    continue
                # Resolve the license-tier label. Two possible shapes:
                #   (1) Outer "role" is an object with a "name" string —
                #       Revizto already translated the int to a label
                #       (this is the shape the invite-bulk response uses;
                #       the /team endpoint MAY use the same shape).
                #   (2) "role" (or licenseRole / license_role / tier) is
                #       a bare int — we translate via REVIZTO_LICENSE_ROLE_MAP.
                # Prefer (1) when available because it's authoritative;
                # fall back to (2) so we still work if the endpoint only
                # returns the int.
                label = ""
                role_obj = ent.get("role")
                if isinstance(role_obj, dict):
                    name = role_obj.get("name")
                    if isinstance(name, str) and name.strip():
                        label = name.strip()
                if not label:
                    tier = None
                    for k in ("role", "licenseRole", "license_role", "tier"):
                        v = ent.get(k)
                        if isinstance(v, int):
                            tier = v
                            break
                    if tier is not None:
                        label = REVIZTO_LICENSE_ROLE_MAP.get(tier, "")
                if label:
                    index[ent_email] = label
            print(f"[browse-sweep] revizto {region} '{license_name}': license team raw={len(ents)} indexed={len(index)}")
            return index

        for proj in rev_projects:
            pid = proj.get("uuid", "")
            if not pid:
                continue
            project_key = rev_uuid_to_key.get(pid, f"revizto:{pid}")
            project_name = (
                admin_projects.get(rev_uuid_to_key[pid], {}).get("name")
                if pid in rev_uuid_to_key
                else proj.get("title", pid)
            )
            project_keys_swept.add(project_key)

            region       = proj.get("region", "dubai")
            license_uuid = proj.get("license_uuid", "")
            license_name = proj.get("license_name", "")

            token = rev_tokens.get(region)
            if token is None:
                token = revizto_get_token_for_region(config, region, email=user) or ""
                rev_tokens[region] = token
            if not token:
                continue

            # Index this project's license team (cached after first call
            # for any project sharing the same license).
            license_index = _ensure_license_team(region, license_uuid, license_name, token)

            try:
                tr = http_requests.get(
                    f"https://api.{region}.revizto.com/v5/project/{pid}/team",
                    headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                    timeout=15,
                )
            except Exception as e:
                print(f"[browse-sweep] revizto /team {pid[:8]} raised: {type(e).__name__}: {e}")
                continue
            if tr.status_code != 200:
                print(f"[browse-sweep] revizto /team {pid[:8]} status={tr.status_code}")
                continue
            try:
                body = tr.json()
            except Exception:
                continue
            if body.get("result", 0) != 0:
                continue
            ents = (body.get("data") or {}).get("entities") or []
            for ent in ents:
                if not isinstance(ent, dict):
                    continue
                em = (ent.get("email") or "").strip()
                if not em:
                    continue
                em_lower = em.lower()
                role_obj = ent.get("accessRole") if isinstance(ent.get("accessRole"), dict) else {}
                role = role_obj.get("name", "") if role_obj else ""
                full_name = ent.get("fullname") or (
                    f"{ent.get('firstname','')} {ent.get('lastname','')}".strip()
                )
                rec = upsert_row(em, project_key, project_name, name=full_name)
                if not rec:
                    continue
                # License-side enrichment from the per-license index.
                # The index stores resolved labels (strings) directly —
                # no further translation needed. Empty string means
                # "not on this license" (or license-team fetch failed).
                lic_role_label = license_index.get(em_lower, "")
                # Two distinct identifiers per entity:
                #   - member_uuid: the membership record's UUID, present
                #     even for invited-but-not-accepted users. Used for
                #     withdrawing pending invites.
                #   - user_uuid: the actual user's UUID (under entity.user
                #     in Revizto's API). Empty until the user accepts the
                #     invite. Used for removing active members.
                # The DELETE endpoint /v5/project/{pid}/role/member/{X}
                # accepts either form, so deactivate prefers user_uuid
                # for active members and falls back to member_uuid for
                # pending invites.
                # Per the Revizto API docs (Get project members), each
                # entity has shape:
                #   {
                #     "email": ...,
                #     "uuid": <some entity-level uuid, NOT the right one>,
                #     "member": {                  ← THIS is what bulk-delete wants
                #         "uuid": "...",            ← member UUID, even for pending
                #         "user": {
                #             "uuid": "...",        ← user UUID (after acceptance)
                #         }
                #     }
                #   }
                # bulk-delete takes memberUuids — i.e. entity.member.uuid.
                member_obj  = ent.get("member") if isinstance(ent.get("member"), dict) else {}
                member_uuid = (member_obj.get("uuid") or "") or ""
                # Fallback: some endpoints / older responses may put the
                # member uuid at the top level. Use it if member.uuid is empty.
                if not member_uuid:
                    member_uuid = (ent.get("uuid") or "") or ""
                # User UUID lives under member.user.uuid (post-acceptance).
                # Tolerate the older top-level user.uuid shape too.
                user_obj  = member_obj.get("user") if isinstance(member_obj.get("user"), dict) else {}
                user_uuid = (user_obj.get("uuid") or "") or ""
                if not user_uuid:
                    legacy_user = ent.get("user") if isinstance(ent.get("user"), dict) else {}
                    user_uuid   = (legacy_user.get("uuid") or "") or ""
                # Pending detection: Revizto sets entity.invited=true on
                # the project team endpoint for newly-invited users that
                # haven't accepted yet. Also tolerate ent.pending=true
                # from the license endpoint shape.
                pending = bool(ent.get("invited")) or bool(ent.get("pending"))
                if pending and not member_uuid:
                    # Should rarely happen now that we read entity.member.uuid,
                    # but log if it does — helps catch any further API drift.
                    print(f"[browse-sweep] revizto pending user {em} on {project_key} has no member_uuid; entity keys={list(ent.keys())}, member keys={list(member_obj.keys())}")
                rec["revizto"] = {
                    "role":         role,
                    "license_role": lic_role_label,
                    "company":      ent.get("company", "") or "",
                    "department":   ent.get("department", "") or "",
                    "location":     ent.get("location", "") or "",
                    "last_active":  ent.get("lastActive", "") or "",
                    "user_uuid":    user_uuid,
                    "member_uuid":  member_uuid,
                    "pending":      pending,
                    # on_license: true iff email appears in the parent
                    # license's team. False if the user was on the
                    # project but later removed from the license, OR
                    # if the license-team fetch failed (admin sees a
                    # blank LICENSE ROLE column and L=· in that case).
                    "on_license":   bool(lic_role_label),
                    # on_project: always True here — we're iterating
                    # /project/.../team, so they're a project member
                    # by construction.
                    "on_project":   True,
                }
    except Exception as e:
        # Top-level guard — should never fire because every inner branch
        # is now wrapped, but keep it as a safety net so a single Revizto
        # failure doesn't take down the whole sweep.
        import traceback
        print(f"[browse-sweep] revizto sweep failed: {type(e).__name__}: {e}")
        print(f"[browse-sweep] traceback:\n{traceback.format_exc()}")

    # ── Forma sweep — registry only ────────────────────────────────
    try:
        forma_token = forma_get_token(config)
        forma_headers = {"Authorization": f"Bearer {forma_token}",
                         "Accept": "application/json"} if forma_token else None
        for k, rp in admin_projects.items():
            forma = (rp.get("platforms", {}) or {}).get("forma", {}) or {}
            fpid = forma.get("project_id", "")
            if not fpid or not forma_headers:
                continue
            project_keys_swept.add(k)
            roles_cat     = {r["id"]: r["name"] for r in (forma.get("roles") or [])     if r.get("id")}
            companies_cat = {c["id"]: c["name"] for c in (forma.get("companies") or []) if c.get("id")}
            project_name  = rp.get("name", k)

            offset = 0
            while True:
                try:
                    rr = http_requests.get(
                        f"https://developer.api.autodesk.com/construction/admin/v1/projects/{fpid}/users",
                        headers=forma_headers, params={"limit": 200, "offset": offset}, timeout=15,
                    )
                except Exception as e:
                    print(f"[browse-sweep] forma users {fpid[:8]} raised: {e}")
                    break
                if rr.status_code != 200:
                    break
                batch = (rr.json() or {}).get("results", [])
                if not batch:
                    break
                for u_ in batch:
                    em = (u_.get("email") or "").strip()
                    if not em:
                        continue
                    rnames = []
                    for ro in (u_.get("roles") or []):
                        if isinstance(ro, dict) and ro.get("name"):
                            rnames.append(ro["name"])
                        elif isinstance(ro, dict) and ro.get("id"):
                            rnames.append(roles_cat.get(ro["id"], ro["id"]))
                        elif isinstance(ro, str):
                            rnames.append(roles_cat.get(ro, ro))
                    role_str = ", ".join(rnames)
                    company_id = u_.get("companyId", "")
                    company_name = companies_cat.get(company_id, company_id) if company_id else ""
                    full_name = (
                        (u_.get("name") or "").strip() or
                        f"{u_.get('firstName','')} {u_.get('lastName','')}".strip()
                    )
                    access_levels = u_.get("accessLevels") or {}
                    if not isinstance(access_levels, dict):
                        access_levels = {}
                    for p in (u_.get("products") or []):
                        if isinstance(p, dict) and p.get("key"):
                            access_levels.setdefault(p["key"], p.get("access") or "user")
                    is_project_admin = bool(access_levels.get("projectAdmin"))
                    rec = upsert_row(em, k, project_name, name=full_name)
                    if not rec:
                        continue
                    rec["forma"] = {
                        "role":         role_str,
                        "company":      company_name,
                        "company_id":   company_id,
                        "user_id":      u_.get("id", ""),
                        "access_level": "Project Administrator" if is_project_admin else "Project Member",
                        "docs":         access_levels.get("docs") or "",
                        "design":       access_levels.get("designCollaboration") or "",
                        "model":        access_levels.get("modelCoordination") or "",
                        "last_active":  u_.get("last_sign_in") or u_.get("lastSignIn") or "",
                        "on_project":   True,
                    }
                offset += len(batch)
                if len(batch) < 200:
                    break
    except Exception as e:
        print(f"[browse-sweep] forma sweep failed: {e}")

    # ── Morta sweep — full hub list ─────────────────────────────────
    try:
        morta_hubs = fetch_morta_hubs(config) or []
        api_key = (config.get("morta", {}) or {}).get("api_key", "")
        morta_headers = {"Authorization": f"Bearer {api_key}"} if api_key else None
        for h in morta_hubs:
            hub_id   = h.get("id", "")
            hub_name = h.get("name", hub_id)
            if not hub_id or not morta_headers:
                continue
            project_key  = morta_hub_to_key.get(hub_id, f"morta:{hub_id}")
            project_name = (
                admin_projects.get(morta_hub_to_key[hub_id], {}).get("name")
                if hub_id in morta_hub_to_key
                else hub_name
            )
            project_keys_swept.add(project_key)
            try:
                mr = http_requests.get(
                    f"https://api.morta.io/v1/hub/{hub_id}/members",
                    headers=morta_headers, timeout=15,
                )
            except Exception as e:
                print(f"[browse-sweep] morta /members {hub_id[:8]} raised: {e}")
                continue
            if mr.status_code != 200:
                continue
            try:
                body = mr.json()
            except Exception:
                continue
            for m in body.get("data", []) or []:
                if not isinstance(m, dict):
                    continue
                u_ = m.get("user", {}) or {}
                em = (u_.get("email") or "").strip()
                if not em:
                    continue
                full_name = (u_.get("name") or "").strip()
                role      = m.get("projectRole", "") or ""
                tags = []
                for t in (u_.get("tags") or []):
                    if isinstance(t, dict) and t.get("value"):
                        tags.append(t["value"])
                tag_str = ", ".join(tags)
                rec = upsert_row(em, project_key, project_name, name=full_name)
                if not rec:
                    continue
                rec["morta"] = {
                    "role":        role,
                    "tag":         tag_str,
                    "public_id":   u_.get("publicId", ""),
                    # Morta's hub-member delete endpoint addresses the user
                    # by Firebase UID, not publicId. Capture both — public_id
                    # is the user's Morta-side UUID (used for tag swap),
                    # firebase_id is the auth identity (used for delete).
                    "firebase_id": u_.get("firebaseUserId", "") or u_.get("firebase_id", "") or "",
                    "hub_id":      hub_id,
                    "last_active": "",
                    "on_hub":      True,
                }
    except Exception as e:
        print(f"[browse-sweep] morta sweep failed: {e}")

    now_utc = datetime.now(timezone.utc)
    def _parse_ts(s):
        if not s or not isinstance(s, str):
            return None
        try:
            iso = s.strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            try:
                dt = datetime.strptime(s.strip(), "%Y-%m-%d %H:%M:%S")
                return dt.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                return None

    rows = list(rows_by_key.values())
    for rec in rows:
        candidates = []
        for plat_key in ("revizto", "forma", "morta"):
            sub = rec.get(plat_key) or {}
            ts = sub.get("last_active") if isinstance(sub, dict) else None
            dt = _parse_ts(ts)
            if dt is not None:
                candidates.append((dt, ts))
        if candidates:
            best_dt, best_str = max(candidates, key=lambda x: x[0])
            rec["last_active_ts"]   = best_str
            rec["last_active_days"] = max(0, (now_utc - best_dt).days)

    # ── Restore deactivated rows from snapshots ─────────────────────
    # Platform APIs only return ACTIVE members. If a user was deactivated,
    # the sweep won't find them and would otherwise null out their sub-
    # object. To keep deactivated rows visible (red toggles, snapshot
    # data shown), we replay deactivation_snapshots.json: for every
    # (email, platform, project_key) snapshot, if the row exists but
    # the platform sub-object is missing, restore it from the snapshot
    # with _deactivated: true.
    #
    # If the row itself is missing entirely (all three platforms inactive,
    # so the sweep never saw the user), we materialize a row from the
    # snapshot. This is what fixes the "user vanishes when all three are
    # deactivated" bug.
    try:
        snapshots = load_deactivation_snapshots()
    except Exception as e:
        print(f"[browse-sweep] failed to load snapshots, skipping restore: {e}")
        snapshots = {}

    for snap_key, snap in snapshots.items():
        # Snapshot keys are formatted as "email|platform|project_key" by
        # _snapshot_key(). Parse it back out.
        parts = snap_key.split("|", 2)
        if len(parts) != 3:
            continue
        snap_email, snap_platform, snap_project_key = parts
        if snap_platform not in ("revizto", "forma", "morta"):
            continue
        # Only restore for projects we actually swept this round; otherwise
        # an admin's snapshot from another project would leak into the
        # current admin's view.
        if snap_project_key not in project_keys_swept:
            continue

        # Build the cache-row sub-object by stripping the metadata keys
        # the snapshot saver added (_name, _email).
        sub_data = {k: v for k, v in snap.items() if not k.startswith("_")}
        sub_data["_deactivated"] = True

        rec_key = (snap_email, snap_project_key)
        rec = rows_by_key.get(rec_key)
        if rec is None:
            # Row didn't exist in the sweep result — user is fully off
            # all three platforms. Materialize the row from the snapshot's
            # metadata fields so it stays visible.
            project_name = snap.get("_project_name") or snap_project_key
            rec = {
                "email":       snap_email,
                "name":        snap.get("_name", "") or snap_email,
                "project_key": snap_project_key,
                "project_name": project_name,
                "revizto":     None,
                "forma":       None,
                "morta":       None,
            }
            rows_by_key[rec_key] = rec
            rows.append(rec)

        # Only restore if the sweep didn't already find an active sub-object
        # for this platform — we don't want to overwrite live data.
        if rec.get(snap_platform) is None:
            rec[snap_platform] = sub_data

    return rows, project_keys_swept


def _do_deactivate(user, config, email, platform, project_key, proj, row):
    """Call the platform's removal API. Returns (ok: bool, detail: str)."""
    plats = proj.get("platforms", {}) or {}

    if platform == "revizto":
        rev = plats.get("revizto", {}) or {}
        project_uuid = rev.get("project_uuid", "")
        region       = rev.get("region", "dubai")
        rsub         = row.get("revizto") or {}
        # Per Revizto API docs ("Remove users from a project"), the
        # endpoint is POST /v5/project/{uuid}/role/bulk-delete with body
        # {"memberUuids": [...]}. It takes MEMBER uuids (the membership
        # records), NOT user uuids — same field for both active members
        # and pending invitees, so a single code path handles both cases.
        member_uuid = rsub.get("member_uuid", "")
        is_pending  = bool(rsub.get("pending"))
        if not project_uuid or not region:
            return False, "Revizto project_uuid/region not in registry"
        if not member_uuid:
            return False, "Revizto member_uuid missing on row — refresh first"
        token = revizto_get_token_for_region(config, region, email=user)
        if not token:
            return False, "Revizto auth failed"
        url = f"https://api.{region}.revizto.com/v5/project/{project_uuid}/role/bulk-delete"
        try:
            r = http_requests.post(url,
                headers={"Authorization": f"Bearer {token}",
                         "Content-Type": "application/json"},
                json={"memberUuids": [member_uuid], "operationId": None},
                timeout=15)
        except Exception as e:
            return False, f"Revizto request failed: {e}"
        # Revizto wraps responses as {result: <code>, data: {ok:{...}, error:{...}}, message: ...}
        # HTTP 200 only means "request was processed" — must inspect result + per-uuid status.
        if r.status_code != 200:
            return False, f"Revizto HTTP {r.status_code}: {r.text[:200]}"
        try:
            body = r.json()
        except Exception:
            return False, f"Revizto returned non-JSON: {r.text[:200]}"
        if body.get("result", -1) != 0:
            return False, f"Revizto API result={body.get('result')}: {body.get('message','no message')}"
        data = body.get("data") or {}
        ok_dict  = data.get("ok") or {}
        err_dict = data.get("error") or {}
        if member_uuid in ok_dict:
            print(f"[browse-toggle] revizto bulk-delete succeeded for {member_uuid[:8]}… ({'pending' if is_pending else 'active'})")
            return True, ""
        if member_uuid in err_dict:
            err_code = err_dict[member_uuid]
            return False, f"Revizto rejected delete: code {err_code}"
        # Server didn't acknowledge our UUID at all — odd but treat as failure.
        return False, f"Revizto bulk-delete didn't acknowledge {member_uuid[:8]}… (ok={list(ok_dict.keys())}, error={list(err_dict.keys())})"

    if platform == "forma":
        forma = plats.get("forma", {}) or {}
        fpid  = forma.get("project_id", "")
        fsub  = row.get("forma") or {}
        user_id = fsub.get("user_id", "")
        if not fpid:
            return False, "Forma project_id not in registry"
        if not user_id:
            return False, "Forma user_id missing on row — refresh first"
        token = forma_get_token(config)
        if not token:
            return False, "Forma auth failed"
        # Autodesk admin endpoints require a User-Id impersonation header
        # so the API knows which admin is making the call. Without it,
        # the request returns 401 Unauthorized even with a valid token —
        # same as forma_invite() at line ~1864. The admin_user_id comes
        # from config.forma.admin_user_id (set in Settings).
        admin_user_id = (config.get("forma", {}) or {}).get("admin_user_id", "")
        clean_pid = fpid.lstrip("b.") if fpid.startswith("b.") else fpid
        url = f"https://developer.api.autodesk.com/construction/admin/v1/projects/{clean_pid}/users/{user_id}"
        headers = {"Authorization": f"Bearer {token}"}
        if admin_user_id:
            headers["User-Id"] = admin_user_id
        try:
            r = http_requests.delete(url, headers=headers, timeout=20)
        except Exception as e:
            return False, f"Forma request failed: {e}"
        if r.status_code in (200, 204):
            return True, ""
        return False, f"Forma HTTP {r.status_code}: {r.text[:200]}"

    if platform == "morta":
        morta = plats.get("morta", {}) or {}
        hub_id = morta.get("hub_id", "")
        msub   = row.get("morta") or {}
        firebase_id = msub.get("firebase_id", "")
        if not hub_id:
            return False, "Morta hub_id not in registry"
        if not firebase_id:
            # Fallback for cache rows swept before firebase_id was being
            # captured. Tell the admin to refresh so the new sweep populates it.
            return False, "Morta firebase_id missing on row — refresh first"
        api_key = (config.get("morta", {}) or {}).get("api_key", "")
        if not api_key:
            return False, "Morta API key not set in Settings"
        # Documented endpoint: DELETE /v1/hub/{hub_id}/remove-user/{firebase_id}
        # The user is identified by Firebase UID (auth identity), not the
        # publicId we use elsewhere. Source: Morta API docs "Delete a User
        # from a Hub" page.
        url = f"https://api.morta.io/v1/hub/{hub_id}/remove-user/{firebase_id}"
        try:
            r = http_requests.delete(url,
                headers={"Authorization": f"Bearer {api_key}"}, timeout=15)
        except Exception as e:
            return False, f"Morta request failed: {e}"
        if r.status_code in (200, 204):
            return True, ""
        return False, f"Morta HTTP {r.status_code}: {r.text[:200]}"

    return False, f"unknown platform {platform}"


def _do_reactivate(user, config, email, platform, project_key, proj, snap):
    """Re-invite using snapshot fields. Returns (ok, detail)."""
    plats = proj.get("platforms", {}) or {}
    full_name = snap.get("_name", "")
    parts = full_name.split(" ", 1)
    first_name = parts[0] if parts else ""
    last_name  = parts[1] if len(parts) > 1 else ""

    if platform == "revizto":
        rev = plats.get("revizto", {}) or {}
        project_uuid = rev.get("project_uuid", "")
        if not project_uuid:
            return False, "Revizto project_uuid not in registry"
        license_role = config.get(
            f"revizto_{rev.get('region','dubai')}", {}
        ).get("default_license_role", 2)
        result = revizto_invite(
            email, first_name, last_name,
            snap.get("company", ""), snap.get("department", ""),
            config,
            project_uuid=project_uuid,
            license_role_id=license_role,
            project_role_id=None,
            invite_to_license=True,
            invite_to_project=True,
            location=snap.get("location", ""),
            admin_email=user,
        )
        if result.get("status") == "success":
            return True, ""
        return False, result.get("detail", "revizto_invite returned error")

    if platform == "forma":
        forma = plats.get("forma", {}) or {}
        fpid = forma.get("project_id", "")
        if not fpid:
            return False, "Forma project_id not in registry"
        products = []
        if snap.get("docs"):    products.append({"key": "docs",                "access": "member"})
        if snap.get("design"):  products.append({"key": "designCollaboration", "access": "member"})
        if snap.get("model"):   products.append({"key": "modelCoordination",   "access": "member"})
        if not products:
            products = [{"key": "docs", "access": "member"}]
        result = forma_invite(
            email, config,
            project_id=fpid,
            company_id=snap.get("company_id") or None,
            role_ids=None,
            products=products,
        )
        if result.get("status") == "success":
            return True, ""
        return False, result.get("detail", "forma_invite returned error")

    if platform == "morta":
        morta = plats.get("morta", {}) or {}
        hub_id = morta.get("hub_id", "")
        if not hub_id:
            return False, "Morta hub_id not in registry"
        result = morta_invite(email, config, hub_id=hub_id, tag_reference_id=None)
        if result.get("status") == "success":
            return True, ""
        return False, result.get("detail", "morta_invite returned error")

    return False, f"unknown platform {platform}"


@app.get("/browse", response_class=HTMLResponse)
async def browse_page(request: Request, user: str = Depends(require_user)):
    return templates.TemplateResponse(request=request, name="browse.html", context={
        "active_page":  "manage",   # nav highlight stays on Manage Users
        "current_user": user,
    })

@app.get("/api/browse/projects")
async def api_browse_projects(user: str = Depends(require_user)):
    """Return projects visible to this admin's licenses across all platforms.

    Per the brief, "loose" scope: every project the admin can see in any
    platform, not restricted to the registry. Each entry is tagged with
    platform + the platform-specific id so the picker can disambiguate.

    Shape: { projects: [{ platform, id, name }] }
    """
    config = load_config(user)
    out: list = []

    # Revizto — license-based sweep (matches the extraction script).
    # Reuses fetch_revizto_projects() which already walks user/licenses
    # then project/list/{lic}/paged across both regions.
    try:
        rev_projects = fetch_revizto_projects(config, email=user) or []
        for p in rev_projects:
            out.append({
                "platform": "revizto",
                "id":       p.get("uuid", ""),
                "name":     p.get("title", p.get("uuid", "")),
            })
    except Exception as e:
        print(f"[browse/projects] revizto failed: {e}")

    # Forma — registry-scoped only. The user explicitly chose to apply
    # the registry constraint to Forma alone; Revizto and Morta sweep
    # whatever the admin's account can see.
    try:
        registry       = load_registry()
        admin_projects = registry if is_owner(user) else {
            k: v for k, v in registry.items() if user in v.get("admins", [])
        }
        for k, rp in admin_projects.items():
            forma = (rp.get("platforms", {}) or {}).get("forma", {}) or {}
            pid = forma.get("project_id", "")
            if pid:
                out.append({
                    "platform": "forma",
                    "id":       pid,
                    "name":     rp.get("name", k),
                })
    except Exception as e:
        print(f"[browse/projects] registry walk failed: {e}")

    # Morta — full sweep of every hub favourited on the admin's account.
    # Mirrors the Revizto behaviour: registry constraint applies to Forma
    # only. Registered hubs reuse the registry's display name; the rest
    # surface their Morta hub name as-is.
    try:
        morta_hubs = fetch_morta_hubs(config) or []
        # Build hub_id -> registry name map for nicer labels on registered hubs.
        morta_id_to_regname: dict = {}
        for k, rp in admin_projects.items():
            morta_p = (rp.get("platforms", {}) or {}).get("morta", {}) or {}
            mh = morta_p.get("hub_id", "") or morta_p.get("project_id", "")
            if mh:
                morta_id_to_regname[mh] = rp.get("name", k)
        for h in morta_hubs:
            hid = h.get("id", "")
            if not hid:
                continue
            out.append({
                "platform": "morta",
                "id":       hid,
                "name":     morta_id_to_regname.get(hid, h.get("name", hid)),
            })
    except Exception as e:
        print(f"[browse/projects] morta failed: {e}")

    # Sort: platform first (revizto, forma, morta), name alpha second.
    plat_order = {"revizto": 0, "forma": 1, "morta": 2}
    out.sort(key=lambda p: (plat_order.get(p["platform"], 9), p["name"].lower()))
    return {"projects": out}

@app.get("/api/browse/users")
async def api_browse_users(user: str = Depends(require_user)):
    """Read-only cache reader. Returns rows the current admin has scope
    on, plus freshness metadata. Does NOT sweep platforms — that's what
    /api/browse/refresh is for.

    Scope: rows whose project_key is in the admin's registry membership
    (or all rows for the owner). Synthetic project_keys like
    'revizto:<uuid>' or 'morta:<hub_id>' are dropped for non-owner
    admins because they're not in the registry.

    Performance: the browse cache is large (~2-3 MB JSON for ~4500 rows
    after recent field additions). load_browse_cache() does a SYNC blob
    read on cache miss, which would block the event loop for several
    seconds and stall every other in-flight request behind it. Run it
    on a worker thread so the loop stays responsive. The 5-minute
    in-memory TTL means most requests never hit blob anyway.
    """
    loop = asyncio.get_event_loop()
    cache = await loop.run_in_executor(None, load_browse_cache)
    all_rows = cache.get("rows") or []

    if is_owner(user):
        visible = all_rows
    else:
        registry = await loop.run_in_executor(None, load_registry)
        admin_keys = {k for k, v in registry.items() if user in v.get("admins", [])}
        visible = [r for r in all_rows if r.get("project_key") in admin_keys]

    return {
        "rows":         visible,
        "updated_at":   cache.get("updated_at", ""),
        "updated_by":   cache.get("updated_by", ""),
        "total_cached": len(all_rows),
        "total_visible": len(visible),
    }


@app.post("/api/browse/refresh")
async def api_browse_refresh(user: str = Depends(require_user)):
    """Sweep the calling admin's accessible projects across all three
    platforms, then delete-and-replace those project_keys' rows in the
    shared cache.

    Concurrency: blob writes are last-wins. Two admins refreshing
    simultaneously may overwrite each other's writes — but their
    project_keys typically don't overlap, so the loss is bounded.
    Re-clicking Refresh recovers from the rare overlap case.
    """
    loop = asyncio.get_event_loop()
    rows_new, project_keys_swept = await loop.run_in_executor(
        None, _sweep_browse_rows, user
    )

    cache = load_browse_cache()
    existing = cache.get("rows") or []

    kept = [r for r in existing if r.get("project_key") not in project_keys_swept]
    merged = kept + rows_new

    cache["rows"]       = merged
    cache["updated_at"] = datetime.now(timezone.utc).isoformat()
    cache["updated_by"] = user
    save_browse_cache(cache)

    return {
        "status":          "ok",
        "swept_projects":  sorted(project_keys_swept),
        "rows_added":      len(rows_new),
        "rows_dropped":    len(existing) - len(kept),
        "total_in_cache":  len(merged),
        "updated_at":      cache["updated_at"],
        "updated_by":      cache["updated_by"],
    }


@app.post("/api/browse/toggle")
async def api_browse_toggle(request: Request, user: str = Depends(require_user)):
    """Apply one or more deactivate / reactivate operations.

    Body:
      { changes: [{ email, platform, project_key, action }] }
        action ∈ { 'deactivate', 'reactivate' }

    Per change:
      deactivate  — snapshot the row's pre-deactivate sub-object, then
                    call the platform's removal API. If snapshot save
                    fails, abort that change to preserve reactivate
                    ability.
      reactivate  — look up the snapshot, re-invite, clear snapshot
                    on success. No snapshot = error (c-strict).

    Each change is independent: one failing doesn't abort the others.
    Returns per-change result so the frontend can update toggle state.
    """
    body    = await request.json()
    changes = body.get("changes") or []
    if not isinstance(changes, list) or not changes:
        return JSONResponse({"error": "No changes provided."}, status_code=400)

    config       = load_config(user)
    registry     = load_registry()
    cache        = load_browse_cache()
    snapshots    = load_deactivation_snapshots()
    rows_by_pk   = {(r.get("email","").lower(), r.get("project_key","")): r
                    for r in cache.get("rows") or []}

    results = []

    for ch in changes:
        email        = (ch.get("email") or "").strip().lower()
        platform     = (ch.get("platform") or "").strip()
        project_key  = (ch.get("project_key") or "").strip()
        action       = (ch.get("action") or "").strip()

        if not (email and platform and project_key and action):
            results.append({"ok": False, "email": email, "platform": platform,
                            "project_key": project_key, "error": "missing fields"})
            continue
        if platform not in ("revizto", "forma", "morta"):
            results.append({"ok": False, "email": email, "platform": platform,
                            "project_key": project_key, "error": "unknown platform"})
            continue
        if action not in ("deactivate", "reactivate"):
            results.append({"ok": False, "email": email, "platform": platform,
                            "project_key": project_key, "error": "unknown action"})
            continue

        proj = registry.get(project_key)
        if not proj:
            results.append({"ok": False, "email": email, "platform": platform,
                            "project_key": project_key,
                            "error": "project not in registry"})
            continue
        if not is_owner(user) and user not in proj.get("admins", []):
            results.append({"ok": False, "email": email, "platform": platform,
                            "project_key": project_key,
                            "error": "not authorized for this project"})
            continue

        row    = rows_by_pk.get((email, project_key))
        snap_k = _snapshot_key(email, platform, project_key)

        try:
            if action == "deactivate":
                if not row or not row.get(platform):
                    results.append({"ok": False, "email": email, "platform": platform,
                                    "project_key": project_key,
                                    "error": "row or platform sub-object missing"})
                    continue
                snap_data = dict(row[platform])
                snap_data["_name"]    = row.get("name", "")
                snap_data["_email"]   = row.get("email", "")
                snap_data["_project_name"] = row.get("project_name", "")
                snapshots[snap_k]     = snap_data
                save_deactivation_snapshots(snapshots)

                ok, detail = _do_deactivate(
                    user, config, email, platform, project_key, proj, row
                )
                if ok:
                    # Mark as deactivated but keep the snapshot data
                    # visible on the row. Property cells render the
                    # historical values read-only, and the toggle
                    # renders red. Reactivate restores the original
                    # sub-object (re-stripping _deactivated).
                    row[platform] = {**row[platform], "_deactivated": True}
                    audit_log_event("browse_deactivate", user,
                        {"email": email, "platform": platform, "project_key": project_key},
                        before=snap_data, after=None, status="ok")
                    results.append({"ok": True, "email": email, "platform": platform,
                                    "project_key": project_key, "action": "deactivate"})
                else:
                    snapshots.pop(snap_k, None)
                    save_deactivation_snapshots(snapshots)
                    audit_log_event("browse_deactivate", user,
                        {"email": email, "platform": platform, "project_key": project_key},
                        before=snap_data, after=None, status="error", error=detail)
                    results.append({"ok": False, "email": email, "platform": platform,
                                    "project_key": project_key, "error": detail})

            elif action == "reactivate":
                snap = snapshots.get(snap_k)
                if not snap:
                    audit_log_event("browse_reactivate", user,
                        {"email": email, "platform": platform, "project_key": project_key},
                        status="error", error="no snapshot")
                    results.append({"ok": False, "email": email, "platform": platform,
                                    "project_key": project_key,
                                    "error": "no snapshot — reactivate via Manage Users"})
                    continue
                ok, detail = _do_reactivate(
                    user, config, email, platform, project_key, proj, snap
                )
                if ok:
                    if not row:
                        row = {
                            "email":         snap.get("_email", email),
                            "name":          snap.get("_name", ""),
                            "project_key":   project_key,
                            "project_name":  proj.get("name", project_key),
                            "revizto": None, "forma": None, "morta": None,
                            "last_active_ts": "", "last_active_days": None,
                        }
                        cache.setdefault("rows", []).append(row)
                        rows_by_pk[(email, project_key)] = row
                    restored = {k: v for k, v in snap.items() if not k.startswith("_")}
                    row[platform] = restored
                    snapshots.pop(snap_k, None)
                    save_deactivation_snapshots(snapshots)
                    audit_log_event("browse_reactivate", user,
                        {"email": email, "platform": platform, "project_key": project_key},
                        before=None, after=restored, status="ok")
                    results.append({"ok": True, "email": email, "platform": platform,
                                    "project_key": project_key, "action": "reactivate"})
                else:
                    audit_log_event("browse_reactivate", user,
                        {"email": email, "platform": platform, "project_key": project_key},
                        before=None, after=None, status="error", error=detail)
                    results.append({"ok": False, "email": email, "platform": platform,
                                    "project_key": project_key, "error": detail})
        except Exception as e:
            audit_log_event("browse_toggle", user,
                {"email": email, "platform": platform, "project_key": project_key,
                 "action": action},
                status="error", error=f"exception: {type(e).__name__}: {e}")
            results.append({"ok": False, "email": email, "platform": platform,
                            "project_key": project_key,
                            "error": f"{type(e).__name__}: {e}"})

    save_browse_cache(cache)

    successes = sum(1 for r in results if r.get("ok"))
    return {
        "ok":         successes == len(results),
        "results":    results,
        "succeeded":  successes,
        "failed":     len(results) - successes,
        "updated_at": cache.get("updated_at"),
    }


# ---------------------------------------------------------------------------
# /api/browse/edit
#
# Apply per-platform field edits to a single (email, project_key) row.
# Triggered when the admin unlocks a browse row, edits property cells,
# and clicks the lock icon to commit. Each field is independent: a
# failure on one field does not block the others, and the response
# reports per-field success/failure so the frontend can revert
# unsuccessful edits to their original values in the cache row.
#
# Body:
#   {
#     "email":       "user@x.com",
#     "project_key": "rak-central-alec",
#     "edits": {
#       "revizto": {                     # optional; absent = no Revizto edits
#         "license_role_id": 4,           # int (1..5)
#         "project_role_id": 17,          # int — the project access role id
#         "company":         "ALEC",
#         "department":      "Digital Construction",
#         "location":        "RAK"
#       },
#       "forma": {                       # optional
#         "company_id": "abc-...",        # uuid string
#         "role_ids":   ["..."],          # list of uuid strings
#         "products":   [                 # full products list to set
#             {"key": "docs",   "access": "member"},
#             {"key": "design", "access": "member"}
#         ]
#       },
#       "morta": {                       # optional
#         "tag_reference_id": "table/.../row/..."   # empty string = clear
#       }
#     }
#   }
#
# Response:
#   {
#     "ok": true|false,                  # true iff every applied field succeeded
#     "applied":  {"revizto": ["license_role","company"], ...},
#     "failed":   [{"platform":"morta", "field":"tag", "error":"..."}],
#     "row":      {...}                  # full updated cache row
#   }
# ---------------------------------------------------------------------------
@app.post("/api/browse/edit")
async def api_browse_edit(request: Request, user: str = Depends(require_user)):
    body         = await request.json()
    email        = (body.get("email") or "").strip().lower()
    project_key  = (body.get("project_key") or "").strip()
    edits        = body.get("edits") or {}
    if not email or not project_key:
        return JSONResponse({"error": "email and project_key required"}, status_code=400)
    if not isinstance(edits, dict) or not edits:
        return JSONResponse({"error": "no edits provided"}, status_code=400)

    config   = load_config(user)
    registry = load_registry()
    cache    = load_browse_cache()
    # Snapshots store the platform sub-object that existed at deactivation
    # time. Loaded here because both the deactivate (writes snapshot) and
    # reactivate (reads snapshot) paths need it. Saved back to disk
    # whenever modified — last-wins concurrency, same as the cache.
    snapshots = load_deactivation_snapshots()

    proj = registry.get(project_key)
    if not proj:
        return JSONResponse({"error": "project not in registry"}, status_code=404)
    if not is_owner(user) and user not in proj.get("admins", []):
        return JSONResponse({"error": "not authorized for this project"}, status_code=403)

    rows_by_pk = {(r.get("email","").lower(), r.get("project_key","")): r
                  for r in cache.get("rows") or []}
    row = rows_by_pk.get((email, project_key))
    if not row:
        return JSONResponse({"error": "row not found in cache — refresh first"}, status_code=404)

    applied: dict = {"revizto": [], "forma": [], "morta": []}
    failed:  list = []

    # Get the event loop once so the per-platform invite-postlookup
    # calls can offload their synchronous HTTP work onto a worker
    # thread instead of blocking the entire async worker.
    loop = asyncio.get_event_loop()

    # Helper: a row's platform sub-object is "inactive" (i.e. ready to
    # invite or reactivate) when either it's None OR it carries the
    # _deactivated flag set by a previous deactivate. With the new
    # display-keep-snapshot behaviour, deactivated sub-objects are
    # NOT None — they retain the saved values for read-only display
    # but should still be treated as "the user isn't currently on
    # this platform" for routing purposes.
    def _is_inactive(platform):
        sub = row.get(platform)
        return sub is None or (isinstance(sub, dict) and sub.get("_deactivated"))

    def _is_active(platform):
        sub = row.get(platform)
        return isinstance(sub, dict) and not sub.get("_deactivated")

    # ── Revizto edits ──────────────────────────────────────────────
    # Three operating modes share this block:
    #   - DEACTIVATE: rev_edits contains _toggle: false. Dispatches to
    #     _do_deactivate() and skips the rest of the branch. Field
    #     edits sent alongside a deactivate are silently ignored —
    #     no point invite-updating a user we're about to remove.
    #   - EDIT: row.revizto exists, _toggle is true or absent. Use
    #     revizto_invite() with preserve_roles tied to whether the
    #     admin touched license_role_id; the existing user gets updated.
    #   - INVITE: row.revizto is inactive (None or _deactivated) —
    #     user isn't on this Revizto project today. _invite_to_license /
    #     _invite_to_project flags gate which calls fire. After success,
    #     look up the new member's UUIDs.
    rev_edits = edits.get("revizto") or {}
    rev_toggle_off = rev_edits.get("_toggle") is False
    rev_toggle_on  = rev_edits.get("_toggle") is True
    rev_is_invite  = _is_inactive("revizto") and bool(rev_edits) and not rev_toggle_off and not rev_toggle_on
    rev_snap_key   = _snapshot_key(email, "revizto", project_key)
    if rev_edits and rev_toggle_off and _is_active("revizto"):
        # DEACTIVATE path — save snapshot first so reactivate can restore.
        # Snapshot saving mirrors /api/browse/toggle behaviour (line ~5102):
        # capture the platform sub-object plus name/email metadata, then
        # roll back the snapshot if the API delete fails so we don't end
        # up with a phantom snapshot for a still-active user.
        snap_data = dict(row["revizto"])
        snap_data["_name"]    = row.get("name", "")
        snap_data["_email"]   = row.get("email", "")
        snap_data["_project_name"] = row.get("project_name", "")
        snapshots[rev_snap_key] = snap_data
        save_deactivation_snapshots(snapshots)

        ok, err = _do_deactivate(user, config, email, "revizto", project_key, proj, row)
        if ok:
            # Mark deactivated but keep snapshot values on the row so
            # the property cells stay populated (read-only). Toggle
            # turns red via the _deactivated flag instead of grey faded.
            row["revizto"] = {**row["revizto"], "_deactivated": True}
            applied["revizto"].append("_deactivated")
        else:
            snapshots.pop(rev_snap_key, None)
            save_deactivation_snapshots(snapshots)
            failed.append({"platform": "revizto", "field": "_toggle", "error": err})
    elif rev_edits and rev_toggle_on and _is_inactive("revizto"):
        # REACTIVATE path — restore from a saved snapshot. If there's
        # no snapshot AND no real field edits, error out. If there's
        # no snapshot but the admin DID fill in fields, fall through
        # to invite — the toggle-on directive just signals intent;
        # the field values define the actual user record.
        snap = snapshots.get(rev_snap_key)
        # "Real field edits" = any non-toggle, non-gate, non-label key.
        has_field_edits = any(
            k for k in rev_edits.keys()
            if not k.startswith("_") and not k.endswith("_label")
        )
        if not snap and not has_field_edits:
            failed.append({"platform": "revizto", "field": "_toggle",
                           "error": "no Revizto snapshot — use Manage Users to re-invite, or fill in fields and try again"})
        elif not snap:
            # Fall through to invite. Set rev_is_invite so the next
            # branch handles it. (We re-derive it here since the original
            # rev_is_invite was False due to rev_toggle_on being True.)
            rev_is_invite = True
        else:
            ok, err = _do_reactivate(user, config, email, "revizto", project_key, proj, snap)
            if ok:
                row["revizto"] = {k: v for k, v in snap.items() if not k.startswith("_")}
                snapshots.pop(rev_snap_key, None)
                save_deactivation_snapshots(snapshots)
                applied["revizto"].append("_reactivated")
            else:
                failed.append({"platform": "revizto", "field": "_toggle", "error": err})
    # Run the EDIT/INVITE path only if we didn't already complete a
    # toggle-on reactivate. Two cases reach here:
    #   1. row.revizto was truthy AND no _toggle directive → EDIT
    #   2. row.revizto was None AND fields were provided → INVITE.
    #      This includes the "_toggle: true with no snapshot" fallthrough
    #      where rev_is_invite was promoted to True earlier.
    # Excluded: the deactivate path (handled above), and successful
    # reactivate (snapshot found, row restored — nothing more to do).
    skip_edit = rev_toggle_off or (rev_toggle_on and not rev_is_invite)
    if rev_edits and not skip_edit and (_is_active("revizto") or rev_is_invite):
        rev_pid = (proj.get("platforms", {}).get("revizto", {}) or {}).get("project_uuid") or ""
        if not rev_pid:
            failed.append({"platform": "revizto", "field": "*",
                           "error": "project has no Revizto UUID in registry"})
        else:
            full_name = (row.get("name") or "").strip()
            parts     = full_name.split(" ", 1) if full_name else []
            first     = parts[0] if parts else ""
            last      = parts[1] if len(parts) > 1 else ""

            cur_rev = row.get("revizto") or {}
            new_company    = rev_edits.get("company",    cur_rev.get("company",    "")) or ""
            new_department = rev_edits.get("department", cur_rev.get("department", "")) or ""
            new_location   = rev_edits.get("location",   cur_rev.get("location",   "")) or ""
            new_lic_role   = rev_edits.get("license_role_id")  # int or None
            new_proj_role  = rev_edits.get("project_role_id")  # int or None

            if rev_is_invite:
                # INVITE mode: gate flags from the frontend choose which
                # Revizto invite calls fire. Default both to True if the
                # frontend forgot to send them — matches the "default
                # checked" UX. At least one must end up True; if both
                # are False, the admin explicitly opted out of doing
                # anything, which is an error worth surfacing.
                inv_lic   = rev_edits.get("_invite_to_license", True)
                inv_proj  = rev_edits.get("_invite_to_project", True)
                inv_lic   = bool(inv_lic)
                inv_proj  = bool(inv_proj)
                if not (inv_lic or inv_proj):
                    failed.append({"platform": "revizto", "field": "*",
                                   "error": "Revizto invite requires at least one of license/project gates to be checked"})
                else:
                    # Default license role to Collaborator (2) if the admin
                    # didn't pick one. Mirrors the manage-page invite default.
                    if inv_lic and new_lic_role is None:
                        new_lic_role = 2
                    try:
                        res = revizto_invite(
                            email          = email,
                            first_name     = first,
                            last_name      = last,
                            company        = new_company,
                            department     = new_department,
                            location       = new_location,
                            config         = config,
                            project_uuid   = rev_pid,
                            license_role_id= new_lic_role,
                            project_role_id= new_proj_role,
                            invite_to_license = inv_lic,
                            invite_to_project = inv_proj,
                            admin_email    = user,
                            preserve_roles = True,   # invite semantics: don't blow away role for already-on-license users
                        )
                    except Exception as e:
                        res = {"status": "error", "detail": f"{type(e).__name__}: {e}"}

                    if res.get("status") == "success":
                        # Build a fresh revizto sub-object from the values
                        # we just sent. The next sweep will canonicalize
                        # fields we don't have (like user_uuid, last_active);
                        # for now, the row reflects the admin's choices.
                        new_sub = {
                            "company":      new_company,
                            "department":   new_department,
                            "location":     new_location,
                            "license_role": (REVIZTO_LICENSE_ROLE_MAP.get(int(new_lic_role), "")
                                             if new_lic_role is not None else ""),
                            "role":         rev_edits.get("project_role_label", ""),
                            "last_active":  "",
                            "user_uuid":    "",
                            "on_license":   inv_lic,
                            "on_project":   inv_proj,
                        }
                        row["revizto"] = new_sub
                        # Tag every requested field as applied so the
                        # frontend renders the new state correctly.
                        for k in rev_edits.keys():
                            if k.startswith("_") or k.endswith("_label"):
                                continue
                            applied["revizto"].append(k)
                        applied["revizto"].append("_invited")
                        # Populate the new member's UUIDs so the row is
                        # immediately deactivate-ready in the same session
                        # (no Refresh needed). Best-effort — if the lookup
                        # fails, IDs stay empty and admin sees "refresh
                        # first" on next deactivate, same as before.
                        # Run on the worker thread so the helper's sync
                        # HTTP calls don't block the event loop.
                        await loop.run_in_executor(
                            None, _populate_invited_user_ids,
                            row, email, "revizto", project_key, proj, config, user
                        )
                    else:
                        detail = res.get("detail", "Revizto invite failed")
                        for k in rev_edits.keys():
                            if k.startswith("_") or k.endswith("_label"):
                                continue
                            failed.append({"platform": "revizto", "field": k, "error": detail})

            else:
                # EDIT mode (original behaviour, preserved unchanged).
                # Build a single revizto_invite call that bundles every
                # requested change. Project-side fires only when project-
                # adjacent fields were touched.
                try:
                    res = revizto_invite(
                        email          = email,
                        first_name     = first,
                        last_name      = last,
                        company        = new_company,
                        department     = new_department,
                        location       = new_location,
                        config         = config,
                        project_uuid   = rev_pid,
                        license_role_id= new_lic_role,
                        project_role_id= new_proj_role,
                        invite_to_license = True,
                        invite_to_project = bool(new_proj_role) or any(
                            k in rev_edits for k in ("company", "department", "location")
                        ),
                        admin_email    = user,
                        preserve_roles = (new_lic_role is None),  # only overwrite role if admin changed it
                    )
                except Exception as e:
                    res = {"status": "error", "detail": f"{type(e).__name__}: {e}"}

                if res.get("status") == "success":
                    if "company" in rev_edits:
                        cur_rev["company"] = new_company
                        applied["revizto"].append("company")
                    if "department" in rev_edits:
                        cur_rev["department"] = new_department
                        applied["revizto"].append("department")
                    if "location" in rev_edits:
                        cur_rev["location"] = new_location
                        applied["revizto"].append("location")
                    if new_lic_role is not None:
                        cur_rev["license_role"] = REVIZTO_LICENSE_ROLE_MAP.get(int(new_lic_role), cur_rev.get("license_role", ""))
                        applied["revizto"].append("license_role")
                    if new_proj_role is not None:
                        label = rev_edits.get("project_role_label") or cur_rev.get("role", "")
                        cur_rev["role"] = label
                        applied["revizto"].append("project_role")
                    row["revizto"] = cur_rev
                else:
                    detail = res.get("detail", "unknown error")
                    for k in rev_edits.keys():
                        if k.startswith("_") or k.endswith("_label"):
                            continue
                        failed.append({"platform": "revizto", "field": k, "error": detail})

    # ── Forma edits ────────────────────────────────────────────────
    # Three modes:
    #   - DEACTIVATE: fo_edits contains _toggle: false → _do_deactivate.
    #   - INVITE: POST /admin/v2/projects/{pid}/users:import via forma_invite()
    #   - EDIT: PATCH /admin/v1/projects/{pid}/users/{userId} via forma_update_user()
    fo_edits = edits.get("forma") or {}
    fo_toggle_off = fo_edits.get("_toggle") is False
    fo_toggle_on  = fo_edits.get("_toggle") is True
    fo_is_invite  = _is_inactive("forma") and bool(fo_edits) and not fo_toggle_off and not fo_toggle_on
    fo_snap_key   = _snapshot_key(email, "forma", project_key)
    if fo_edits and fo_toggle_off and _is_active("forma"):
        snap_data = dict(row["forma"])
        snap_data["_name"]    = row.get("name", "")
        snap_data["_email"]   = row.get("email", "")
        snap_data["_project_name"] = row.get("project_name", "")
        snapshots[fo_snap_key] = snap_data
        save_deactivation_snapshots(snapshots)

        ok, err = _do_deactivate(user, config, email, "forma", project_key, proj, row)
        if ok:
            row["forma"] = {**row["forma"], "_deactivated": True}
            applied["forma"].append("_deactivated")
        else:
            snapshots.pop(fo_snap_key, None)
            save_deactivation_snapshots(snapshots)
            failed.append({"platform": "forma", "field": "_toggle", "error": err})
    elif fo_edits and fo_toggle_on and _is_inactive("forma"):
        snap = snapshots.get(fo_snap_key)
        has_field_edits = any(
            k for k in fo_edits.keys()
            if not k.startswith("_") and not k.endswith("_label")
        )
        if not snap and not has_field_edits:
            failed.append({"platform": "forma", "field": "_toggle",
                           "error": "no Forma snapshot — use Manage Users to re-invite, or fill in fields and try again"})
        elif not snap:
            # Fall through to invite — see Revizto comment for rationale.
            fo_is_invite = True
        else:
            ok, err = _do_reactivate(user, config, email, "forma", project_key, proj, snap)
            if ok:
                row["forma"] = {k: v for k, v in snap.items() if not k.startswith("_")}
                snapshots.pop(fo_snap_key, None)
                save_deactivation_snapshots(snapshots)
                applied["forma"].append("_reactivated")
            else:
                failed.append({"platform": "forma", "field": "_toggle", "error": err})
    fo_skip_edit = fo_toggle_off or (fo_toggle_on and not fo_is_invite)
    if fo_edits and not fo_skip_edit and (_is_active("forma") or fo_is_invite):
        fo_pid = (proj.get("platforms", {}).get("forma", {}) or {}).get("project_id") or ""
        if not fo_pid:
            failed.append({"platform": "forma", "field": "*",
                           "error": "project has no Forma project_id in registry"})
        else:
            cur_fo  = row.get("forma") or {}
            new_company_id = fo_edits.get("company_id")            # may be ""
            new_role_ids   = fo_edits.get("role_ids")              # list or None
            new_products   = fo_edits.get("products")              # list or None

            if fo_is_invite:
                try:
                    res = forma_invite(
                        email      = email,
                        config     = config,
                        project_id = fo_pid,
                        company_id = new_company_id,
                        role_ids   = new_role_ids,
                        products   = new_products,
                    )
                except Exception as e:
                    res = {"status": "error", "detail": f"{type(e).__name__}: {e}"}
            else:
                # EDIT path: needs the existing Forma user_id, which the
                # sweep stores on row.forma.user_id. Without it we can't
                # PATCH; tell the admin to refresh.
                fo_user_id = (cur_fo or {}).get("user_id", "")
                if not fo_user_id:
                    res = {"status": "error",
                           "detail": "Forma user_id missing on row — refresh first"}
                else:
                    # Only pass fields the admin explicitly touched. PATCH
                    # leaves untouched fields alone server-side.
                    patch_kwargs = {}
                    if "company_id" in fo_edits:
                        patch_kwargs["company_id"] = new_company_id or None
                    if "role_ids" in fo_edits:
                        patch_kwargs["role_ids"] = new_role_ids or []
                    if "products" in fo_edits:
                        patch_kwargs["products"] = new_products or []
                    try:
                        res = forma_update_user(
                            user_id    = fo_user_id,
                            config     = config,
                            project_id = fo_pid,
                            **patch_kwargs,
                        )
                    except Exception as e:
                        res = {"status": "error", "detail": f"{type(e).__name__}: {e}"}

            if res.get("status") == "success":
                if fo_is_invite:
                    # Build a fresh forma sub-object. The next sweep fills in
                    # user_id, access_level, last_active.
                    cur_fo = {
                        "company_id":   new_company_id or "",
                        "company":      fo_edits.get("company_label") or "",
                        "role_ids":     new_role_ids or [],
                        "role":         fo_edits.get("role_label") or "",
                        "access_level": "",
                        "docs":         False,
                        "design":       False,
                        "model":        False,
                        "user_id":      "",
                        "last_active":  "",
                    }
                if "company_id" in fo_edits:
                    cur_fo["company_id"]   = new_company_id or ""
                    cur_fo["company"]      = fo_edits.get("company_label") or cur_fo.get("company", "")
                    applied["forma"].append("company")
                if "role_ids" in fo_edits:
                    cur_fo["role_ids"]     = new_role_ids or []
                    cur_fo["role"]         = fo_edits.get("role_label") or cur_fo.get("role", "")
                    applied["forma"].append("role")
                if "products" in fo_edits:
                    prod_keys = {p.get("key") for p in (new_products or []) if isinstance(p, dict)}
                    cur_fo["docs"]   = "docs"   in prod_keys
                    cur_fo["design"] = "design" in prod_keys
                    cur_fo["model"]  = "model"  in prod_keys
                    for k in ("docs", "design", "model"):
                        if k in fo_edits:
                            applied["forma"].append(k)
                row["forma"] = cur_fo
                if fo_is_invite:
                    applied["forma"].append("_invited")
                    # Populate user_id off the event loop — see Revizto comment.
                    await loop.run_in_executor(
                        None, _populate_invited_user_ids,
                        row, email, "forma", project_key, proj, config, user
                    )
            else:
                detail = res.get("detail", "unknown error")
                for k in fo_edits.keys():
                    if k.startswith("_") or k.endswith("_label"):
                        continue
                    failed.append({"platform": "forma", "field": k, "error": detail})

    # ── Morta edits ────────────────────────────────────────────────
    # Three paths:
    #   - DEACTIVATE: mo_edits contains _toggle: false → _do_deactivate.
    #   - EDIT: row already has `morta`; change tag via morta_set_user_tag.
    #   - INVITE (add-from-browse): row.morta is None — call morta_invite()
    #     to add the user to the hub (default projectRole=member) and
    #     chain a tag assignment if a tag was selected.
    mo_edits = edits.get("morta") or {}
    mo_toggle_off = mo_edits.get("_toggle") is False
    mo_toggle_on  = mo_edits.get("_toggle") is True
    mo_is_invite  = _is_inactive("morta") and bool(mo_edits) and not mo_toggle_off and not mo_toggle_on
    mo_snap_key   = _snapshot_key(email, "morta", project_key)
    if mo_edits and mo_toggle_off and _is_active("morta"):
        snap_data = dict(row["morta"])
        snap_data["_name"]    = row.get("name", "")
        snap_data["_email"]   = row.get("email", "")
        snap_data["_project_name"] = row.get("project_name", "")
        snapshots[mo_snap_key] = snap_data
        save_deactivation_snapshots(snapshots)

        ok, err = _do_deactivate(user, config, email, "morta", project_key, proj, row)
        if ok:
            row["morta"] = {**row["morta"], "_deactivated": True}
            applied["morta"].append("_deactivated")
        else:
            snapshots.pop(mo_snap_key, None)
            save_deactivation_snapshots(snapshots)
            failed.append({"platform": "morta", "field": "_toggle", "error": err})
    elif mo_edits and mo_toggle_on and _is_inactive("morta"):
        snap = snapshots.get(mo_snap_key)
        has_field_edits = any(
            k for k in mo_edits.keys()
            if not k.startswith("_") and not k.endswith("_label")
        )
        if not snap and not has_field_edits:
            failed.append({"platform": "morta", "field": "_toggle",
                           "error": "no Morta snapshot — use Manage Users to re-invite, or fill in fields and try again"})
        elif not snap:
            mo_is_invite = True
        else:
            ok, err = _do_reactivate(user, config, email, "morta", project_key, proj, snap)
            if ok:
                row["morta"] = {k: v for k, v in snap.items() if not k.startswith("_")}
                snapshots.pop(mo_snap_key, None)
                save_deactivation_snapshots(snapshots)
                applied["morta"].append("_reactivated")
            else:
                failed.append({"platform": "morta", "field": "_toggle", "error": err})
    mo_skip_edit = mo_toggle_off or (mo_toggle_on and not mo_is_invite)
    if mo_edits and not mo_skip_edit and (_is_active("morta") or mo_is_invite):
        mo_hub = (proj.get("platforms", {}).get("morta", {}) or {}).get("hub_id") or ""
        if not mo_hub:
            failed.append({"platform": "morta", "field": "*",
                           "error": "project has no Morta hub_id in registry"})
        elif mo_is_invite:
            # Invite path: morta_invite handles the hub-member add and
            # an optional tag chain. Tag is optional — empty tag_id is fine.
            new_tag_id = (mo_edits.get("tag_reference_id") or "").strip()
            try:
                res = morta_invite(email, config, hub_id=mo_hub,
                                   tag_reference_id=new_tag_id or None)
            except Exception as e:
                res = {"status": "error", "detail": f"{type(e).__name__}: {e}"}
            if res.get("status") == "success":
                row["morta"] = {
                    "role":        "member",
                    "tag":         mo_edits.get("tag_label") or "",
                    "public_id":   "",
                    "firebase_id": "",
                    "hub_id":      mo_hub,
                    "last_active": "",
                    "on_hub":      True,
                }
                applied["morta"].append("_invited")
                if "tag_reference_id" in mo_edits:
                    applied["morta"].append("tag")
                # Populate Morta IDs off the event loop — see Revizto comment.
                await loop.run_in_executor(
                    None, _populate_invited_user_ids,
                    row, email, "morta", project_key, proj, config, user
                )
            else:
                failed.append({"platform": "morta", "field": "*",
                               "error": res.get("detail", "Morta invite failed")})
        elif "tag_reference_id" in mo_edits:
            new_tag_id = (mo_edits.get("tag_reference_id") or "").strip()
            try:
                res = morta_set_user_tag(email, config, hub_id=mo_hub,
                                         new_tag_reference_id=new_tag_id or None)
            except Exception as e:
                res = {"status": "error", "detail": f"{type(e).__name__}: {e}"}

            if res.get("status") == "success":
                cur_mo = row.get("morta") or {}
                cur_mo["tag"] = mo_edits.get("tag_label") or ""
                row["morta"] = cur_mo
                applied["morta"].append("tag")
            else:
                failed.append({"platform": "morta", "field": "tag",
                               "error": res.get("detail", "unknown error")})

    # ── Persist + audit ────────────────────────────────────────────
    save_browse_cache(cache)
    audit_log_event("browse_edit", user,
        {"email": email, "project_key": project_key,
         "edits": edits, "applied": applied, "failed": failed},
        status="ok" if not failed else ("partial" if any(applied.values()) else "error"))

    return {
        "ok":      not failed,
        "applied": {k: v for k, v in applied.items() if v},
        "failed":  failed,
        "row":     row,
    }


@app.get("/scheduler", response_class=HTMLResponse)
async def scheduler_page(request: Request, user: str = Depends(require_user)):
    return templates.TemplateResponse(request=request, name="scheduler.html", context={
        "active_page": "scheduler", "current_user": user,
    })

# Block 6A: Agent token management page
@app.get("/agent-tokens", response_class=HTMLResponse)
async def agent_tokens_page(request: Request, user: str = Depends(require_user)):
    return templates.TemplateResponse(request=request, name="agent_tokens.html", context={
        "active_page": "agent-tokens", "current_user": user,
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
    user: str = Depends(require_user)
):
    """Return invite log entries scoped to the user's registry projects.

    project_key: if provided, filter to that specific project only.
    limit: max entries to return (most recent first), default 50.
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

    # Enrich Revizto project results: prefer the registry's display name
    # (e.g. "Sphere Abu Dhabi") if the project is registered; otherwise
    # keep the Revizto API title we already fetched (e.g. "Sphere Abu
    # Dhabi Design Phase"). This way projects outside the registry still
    # show a meaningful name instead of a UUID.
    if revizto_result and revizto_result.get("projects"):
        for p in revizto_result["projects"]:
            reg_name = revizto_name_map.get(p.get("uuid"))
            if reg_name:
                p["name"] = reg_name
            # else: keep p["name"] as-is (set by revizto_search_user)

    # Enrich Forma project results with names + resolved company/role labels.
    # The registry stores per-project forma.companies and forma.roles as
    # [{id, name}] lists. Build a per-project lookup so the user pane can
    # show the same human-readable values that the invite table uses.
    if forma_result and forma_result.get("projects"):
        # Map forma project_id -> registry record (for company/role lookups)
        forma_pid_to_regkey = {
            v["platforms"]["forma"]["project_id"]: k
            for k, v in admin_projects.items()
            if v.get("platforms", {}).get("forma", {}).get("project_id")
        }
        for p in forma_result["projects"]:
            pid = p.get("project_id", "")
            p["name"] = forma_name_map.get(pid, pid)
            reg_key = forma_pid_to_regkey.get(pid)
            reg_forma = (admin_projects.get(reg_key, {})
                                       .get("platforms", {})
                                       .get("forma", {})) if reg_key else {}
            companies = {c["id"]: c["name"] for c in reg_forma.get("companies", []) if c.get("id")}
            roles     = {r["id"]: r["name"] for r in reg_forma.get("roles", [])     if r.get("id")}
            # Resolve company name; fall back to raw id if not in registry
            cid = p.get("company_id", "")
            p["company"] = companies.get(cid, cid) if cid else ""
            # Resolve role names: prefer inline names from the API, else
            # look up role_ids in the registry.
            inline = p.get("role_names_inline") or []
            if inline:
                p["role"] = ", ".join(inline)
            else:
                resolved = [roles.get(rid, rid) for rid in (p.get("role_ids") or [])]
                p["role"] = ", ".join(r for r in resolved if r)

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
# ---------------------------------------------------------------------------
# Scheduler — API Endpoints
# ---------------------------------------------------------------------------
# All routes admin-only via require_user (every authenticated user is an
# admin in this app — there's no separate admin-vs-user distinction).
#
# Endpoints exposed:
#   GET    /api/scheduler/scripts         — script catalog (no state)
#   GET    /api/scheduler/shards          — list all shards
#   POST   /api/scheduler/shards          — create
#   PUT    /api/scheduler/shards/{id}     — update
#   DELETE /api/scheduler/shards/{id}     — delete (blocked if in compiler)
#   POST   /api/scheduler/shards/{id}/toggle — pause/resume
#   GET    /api/scheduler/compilers       — list
#   POST   /api/scheduler/compilers       — create
#   PUT    /api/scheduler/compilers/{id}  — update
#   DELETE /api/scheduler/compilers/{id}  — delete (frees its shards)
#   POST   /api/scheduler/compilers/{id}/toggle — pause/resume
#   GET    /api/scheduler/runs            — recent run records
#
# What's NOT here (next building block): /run endpoints that actually fire.

@app.get("/api/scheduler/scripts")
async def api_scheduler_scripts(user: str = Depends(require_user)):
    """Return the static script catalog. Used by the UI to populate
    Platform/Script dropdowns and validate context fields client-side."""
    return JSONResponse({
        "scripts": SCRIPT_CATALOG,
        "valid_formats":      sorted(VALID_OUTPUT_FORMATS),
        "valid_frequencies":  sorted(VALID_FREQUENCIES),
        "valid_destinations": sorted(VALID_DESTINATIONS),
    })

# ── Shards CRUD ────────────────────────────────────────────────────────────

@app.get("/api/scheduler/shards")
async def api_scheduler_shards_list(user: str = Depends(require_user)):
    """List all shards. Shared pool — every admin sees every shard."""
    return JSONResponse({"shards": load_shards()})

@app.post("/api/scheduler/shards")
async def api_scheduler_shard_create(request: Request, user: str = Depends(require_user)):
    payload = await request.json()
    ok, err = validate_shard(payload, allow_partial=False)
    if not ok:
        return JSONResponse({"error": err}, status_code=400)

    now = int(time.time() * 1000)
    shard = {
        "id":            _new_id("shard"),
        "name":          payload["name"].strip(),
        "platform":      SCRIPT_CATALOG[payload["script"]]["platform"],
        "script":        payload["script"],
        "context":       payload.get("context") or {},
        "output":        payload.get("output") or {"formats": ["json"], "prefix": "", "naming": ["append_date"]},
        "destination":   payload.get("destination") or {"type": "blob"},
        "active":        bool(payload.get("active", True)),
        "in_compiler":   None,    # always null at create — joins compiler later
        "consecutive_failures": 0,
        "auto_disabled": False,
        "created_by":    user,
        "created_at":    now,
        "updated_at":    now,
        "last_run_id":   None,
    }
    shards = load_shards()
    shards.append(shard)
    save_shards(shards)
    return JSONResponse({"shard": shard})

@app.put("/api/scheduler/shards/{shard_id}")
async def api_scheduler_shard_update(shard_id: str, request: Request,
                                      user: str = Depends(require_user)):
    payload = await request.json()
    ok, err = validate_shard(payload, allow_partial=True)
    if not ok:
        return JSONResponse({"error": err}, status_code=400)

    shards = load_shards()
    idx = next((i for i, s in enumerate(shards) if s["id"] == shard_id), -1)
    if idx < 0:
        return JSONResponse({"error": "Shard not found."}, status_code=404)

    shard = shards[idx]
    # Apply only the fields present in the payload, preserving everything else
    if "name" in payload:        shard["name"]        = payload["name"].strip()
    if "script" in payload:      shard["script"]      = payload["script"]
    if "platform" in payload:    shard["platform"]    = payload["platform"]
    if "context" in payload:     shard["context"]     = payload["context"] or {}
    if "output" in payload:      shard["output"]      = payload["output"] or {}
    if "destination" in payload: shard["destination"] = payload["destination"] or {}
    if "active" in payload:      shard["active"]      = bool(payload["active"])
    # If platform/script changed, re-derive platform from catalog to stay consistent
    if "script" in payload:
        shard["platform"] = SCRIPT_CATALOG[payload["script"]]["platform"]
    shard["updated_at"] = int(time.time() * 1000)
    shards[idx] = shard
    save_shards(shards)
    return JSONResponse({"shard": shard})

@app.delete("/api/scheduler/shards/{shard_id}")
async def api_scheduler_shard_delete(shard_id: str, user: str = Depends(require_user)):
    shards = load_shards()
    target = next((s for s in shards if s["id"] == shard_id), None)
    if target is None:
        return JSONResponse({"error": "Shard not found."}, status_code=404)
    # Block deletion if shard is referenced by a compiler. The user must
    # remove it from the compiler first — protects against accidental
    # deletion of a shard that's part of an active automation.
    if target.get("in_compiler"):
        return JSONResponse({
            "error": "This shard belongs to a compiler. Remove it from the compiler first.",
            "in_compiler": target["in_compiler"],
        }, status_code=409)
    shards = [s for s in shards if s["id"] != shard_id]
    save_shards(shards)
    return JSONResponse({"deleted": shard_id})

@app.post("/api/scheduler/shards/{shard_id}/toggle")
async def api_scheduler_shard_toggle(shard_id: str, user: str = Depends(require_user)):
    shards = load_shards()
    idx = next((i for i, s in enumerate(shards) if s["id"] == shard_id), -1)
    if idx < 0:
        return JSONResponse({"error": "Shard not found."}, status_code=404)
    shards[idx]["active"] = not shards[idx].get("active", True)
    shards[idx]["updated_at"] = int(time.time() * 1000)
    save_shards(shards)
    return JSONResponse({"shard": shards[idx]})

# ── Compilers CRUD ────────────────────────────────────────────────────────

@app.get("/api/scheduler/compilers")
async def api_scheduler_compilers_list(user: str = Depends(require_user)):
    return JSONResponse({"compilers": load_compilers()})

@app.post("/api/scheduler/compilers")
async def api_scheduler_compiler_create(request: Request,
                                         user: str = Depends(require_user)):
    payload = await request.json()
    ok, err = validate_compiler(payload, allow_partial=False)
    if not ok:
        return JSONResponse({"error": err}, status_code=400)

    # Validate referenced shards exist + aren't already in another compiler
    shard_ids = payload.get("shard_ids") or []
    if shard_ids:
        shards = load_shards()
        shard_index = {s["id"]: s for s in shards}
        for sid in shard_ids:
            if sid not in shard_index:
                return JSONResponse({"error": f"Shard {sid} not found."}, status_code=400)
            existing = shard_index[sid].get("in_compiler")
            if existing:
                return JSONResponse({
                    "error": f"Shard {sid} is already in compiler {existing}. "
                             "Remove from the other compiler first."
                }, status_code=409)

    now = int(time.time() * 1000)
    compiler = {
        "id":         _new_id("compiler"),
        "name":       payload["name"].strip(),
        "schedule":   payload["schedule"],
        "shard_ids":  shard_ids,
        "active":     bool(payload.get("active", True)),
        "created_by": user,
        "created_at": now,
        "updated_at": now,
        "last_run_id": None,
    }
    compilers = load_compilers()
    compilers.append(compiler)
    save_compilers(compilers)
    _sync_shard_compiler_links()
    # Block 5: register the new compiler with the cron engine so it fires on schedule.
    try:
        Scheduler_Engine.register_compiler(compiler)
    except Exception as e:
        print(f"[block5] register_compiler({compiler['id']}) failed: {e}", flush=True)
    return JSONResponse({"compiler": compiler})

@app.put("/api/scheduler/compilers/{compiler_id}")
async def api_scheduler_compiler_update(compiler_id: str, request: Request,
                                         user: str = Depends(require_user)):
    payload = await request.json()
    ok, err = validate_compiler(payload, allow_partial=True)
    if not ok:
        return JSONResponse({"error": err}, status_code=400)

    compilers = load_compilers()
    idx = next((i for i, c in enumerate(compilers) if c["id"] == compiler_id), -1)
    if idx < 0:
        return JSONResponse({"error": "Compiler not found."}, status_code=404)

    # If shard_ids changing, validate ownership transitions
    if "shard_ids" in payload:
        new_shard_ids = payload["shard_ids"] or []
        shards = load_shards()
        shard_index = {s["id"]: s for s in shards}
        for sid in new_shard_ids:
            if sid not in shard_index:
                return JSONResponse({"error": f"Shard {sid} not found."}, status_code=400)
            existing = shard_index[sid].get("in_compiler")
            if existing and existing != compiler_id:
                return JSONResponse({
                    "error": f"Shard {sid} is already in compiler {existing}. "
                             "Remove from the other compiler first."
                }, status_code=409)

    c = compilers[idx]
    if "name" in payload:       c["name"]       = payload["name"].strip()
    if "schedule" in payload:   c["schedule"]   = payload["schedule"]
    if "shard_ids" in payload:  c["shard_ids"]  = payload["shard_ids"] or []
    if "active" in payload:     c["active"]     = bool(payload["active"])
    c["updated_at"] = int(time.time() * 1000)
    compilers[idx] = c
    save_compilers(compilers)
    _sync_shard_compiler_links()
    # Block 5: re-register so schedule changes take effect immediately.
    try:
        Scheduler_Engine.register_compiler(c)
    except Exception as e:
        print(f"[block5] register_compiler({c['id']}) on update failed: {e}", flush=True)
    return JSONResponse({"compiler": c})

@app.delete("/api/scheduler/compilers/{compiler_id}")
async def api_scheduler_compiler_delete(compiler_id: str,
                                         user: str = Depends(require_user)):
    compilers = load_compilers()
    if not any(c["id"] == compiler_id for c in compilers):
        return JSONResponse({"error": "Compiler not found."}, status_code=404)
    compilers = [c for c in compilers if c["id"] != compiler_id]
    save_compilers(compilers)
    # Rerun link sync — orphaned shards have their in_compiler cleared
    _sync_shard_compiler_links()
    # Block 5: unregister the cron job
    try:
        Scheduler_Engine.unregister_compiler(compiler_id)
    except Exception as e:
        print(f"[block5] unregister_compiler({compiler_id}) failed: {e}", flush=True)
    return JSONResponse({"deleted": compiler_id})

@app.post("/api/scheduler/compilers/{compiler_id}/toggle")
async def api_scheduler_compiler_toggle(compiler_id: str,
                                         user: str = Depends(require_user)):
    compilers = load_compilers()
    idx = next((i for i, c in enumerate(compilers) if c["id"] == compiler_id), -1)
    if idx < 0:
        return JSONResponse({"error": "Compiler not found."}, status_code=404)
    compilers[idx]["active"] = not compilers[idx].get("active", True)
    compilers[idx]["updated_at"] = int(time.time() * 1000)
    save_compilers(compilers)
    # Block 5: sync the cron job state. register_compiler() handles both
    # "now active" (adds/replaces job) and "now inactive" (unregisters).
    try:
        Scheduler_Engine.register_compiler(compilers[idx])
    except Exception as e:
        print(f"[block5] register_compiler({compiler_id}) on toggle failed: {e}", flush=True)
    return JSONResponse({"compiler": compilers[idx]})

# ── Runs ──────────────────────────────────────────────────────────────────

@app.get("/api/scheduler/runs")
async def api_scheduler_runs_list(limit: int = 100,
                                   user: str = Depends(require_user)):
    """Return the most recent N run records (most recent first)."""
    runs = load_runs()
    # Newest first
    runs_sorted = sorted(runs, key=lambda r: r.get("started_at", 0), reverse=True)
    return JSONResponse({"runs": runs_sorted[:max(1, min(limit, 500))]})


# Block 5: scheduler engine status — lists registered cron jobs + next-fire times
@app.get("/api/scheduler/engine")
async def api_scheduler_engine_status(user: str = Depends(require_user)):
    """Diagnostic endpoint: shows cron engine status + registered jobs."""
    return JSONResponse({
        "engine": Scheduler_Engine.status(),
        "jobs":   Scheduler_Engine.list_jobs(),
    })


# ---------------------------------------------------------------------------
# Pre-warm cache at startup (must run after all routes are defined)
# ---------------------------------------------------------------------------

# ═══════════════════════════════════════════════════════════════════════════
# BLOCK 4 — SCHEDULER "RUN NOW" ENDPOINTS                                   ▼
# ═══════════════════════════════════════════════════════════════════════════
# ── resolve_script_path ──────────────────────────────────────────────────
# Translate (platform, script_name) into a concrete .py path under SCRIPT_DIRS.
# Convention: scripts live at SCRIPT_DIRS[platform] / {script_name}.py
# We DO NOT trust script_name from the request — the only legitimate values
# are keys in SCRIPT_CATALOG. Any other value raises.
def resolve_script_path(platform: str, script_name: str) -> Path:
    if script_name not in SCRIPT_CATALOG:
        raise ValueError(f"Unknown script: {script_name}")
    catalog_entry = SCRIPT_CATALOG[script_name]
    if catalog_entry["platform"] != platform:
        raise ValueError(
            f"Script {script_name} belongs to platform "
            f"{catalog_entry['platform']}, not {platform}"
        )
    base = SCRIPT_DIRS.get(platform)
    if not base:
        raise ValueError(f"No script directory configured for platform: {platform}")
    return Path(base) / f"{script_name}.py"


# ── append_or_replace_run ────────────────────────────────────────────────
# Run records get written twice — once at start (status=running) and once
# at end (status=success/failed/partial). We need to update the existing
# entry by id, not blindly append a duplicate.
def _append_or_replace_run(record: dict) -> None:
    runs = load_runs()
    rid  = record.get("id")
    found = False
    for i, r in enumerate(runs):
        if r.get("id") == rid:
            runs[i] = record
            found = True
            break
    if not found:
        runs.append(record)
    # Trim to RUN_LOG_MAX_ENTRIES (defined in Block 2)
    if len(runs) > RUN_LOG_MAX_ENTRIES:
        runs = runs[-RUN_LOG_MAX_ENTRIES:]
    save_runs(runs)


# ── update_one_shard ────────────────────────────────────────────────────
# Update specific fields on a shard by id, leaving everything else intact.
# Used by the runner to update consecutive_failures, auto_disabled, etc.
def _update_one_shard(shard_id: str, updates: dict) -> None:
    shards = load_shards()
    for s in shards:
        if s.get("id") == shard_id:
            s.update(updates)
            break
    save_shards(shards)


# ── load_shard_by_id ────────────────────────────────────────────────────
def _load_shard_by_id(shard_id: str) -> dict | None:
    for s in load_shards():
        if s.get("id") == shard_id:
            return s
    return None


# ── write_config_to_path ────────────────────────────────────────────────
# Mirror of write_script_config, but operates on a pre-built config dict
# instead of building one. (build_script_config is called separately by the
# runner so it can apply the shard-context override before write.)
def _write_config_to_path(cfg: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(cfg, f, indent=2)


# ═══════════════════════════════════════════════════════════════════════════
# POST /api/scheduler/shards/{id}/run — fire one shard now
# ═══════════════════════════════════════════════════════════════════════════
@app.post("/api/scheduler/shards/{shard_id}/run")
async def scheduler_run_shard(
    shard_id: str,
    background_tasks: BackgroundTasks,
    user: str = Depends(require_user),
):
    shard = _load_shard_by_id(shard_id)
    if not shard:
        raise HTTPException(status_code=404, detail="Shard not found")

    if not shard.get("active", True):
        raise HTTPException(status_code=409, detail="Shard is paused. Resume it first.")
    if shard.get("auto_disabled"):
        raise HTTPException(status_code=409, detail="Shard is auto-disabled. Resume it first.")

    # Pre-generate the run_id so we can return it immediately. The runner
    # will write the actual record from inside the background task.
    # NB: The runner generates its OWN run_id internally — we don't pass
    # this one in. Instead, we return a marker that the UI polls
    # /api/scheduler/runs to find the new entry.
    background_tasks.add_task(
        Scheduler_Runner.run_shard,
        shard=shard,
        triggering_user=user,
        build_config_for_user=build_script_config,
        write_config_to_path=_write_config_to_path,
        prepare_forma_3l_token=prepare_forma_3l_token,
        prepare_revizto_tokens=prepare_revizto_tokens,
        check_l4_dependency=_check_l4_dependency,
        update_shard=_update_one_shard,
        append_run=_append_or_replace_run,
        blob_upload_file=Blob_Client.upload_file,
        blob_generate_url=Blob_Client.generate_download_url,
        blob_container_for_platform=Blob_Client.container_for_platform,
        resolve_script_path=resolve_script_path,
        stamp_l3_run=_stamp_l3_run,
    )

    return JSONResponse({
        "status":   "queued",
        "message":  f"Shard '{shard.get('name', shard_id)}' is running. Watch Recent Runs for status.",
        "shard_id": shard_id,
    })


# ═══════════════════════════════════════════════════════════════════════════
# POST /api/scheduler/compilers/{id}/run — fire all shards in a compiler
# ═══════════════════════════════════════════════════════════════════════════
@app.post("/api/scheduler/compilers/{compiler_id}/run")
async def scheduler_run_compiler(
    compiler_id: str,
    background_tasks: BackgroundTasks,
    user: str = Depends(require_user),
):
    compilers = load_compilers()
    compiler  = next((c for c in compilers if c.get("id") == compiler_id), None)
    if not compiler:
        raise HTTPException(status_code=404, detail="Compiler not found")
    if not compiler.get("active", True):
        raise HTTPException(status_code=409, detail="Compiler is paused. Resume it first.")
    if not (compiler.get("shard_ids") or []):
        raise HTTPException(status_code=409, detail="Compiler has no shards.")

    background_tasks.add_task(
        Scheduler_Runner.run_compiler,
        compiler=compiler,
        triggering_user=user,
        load_shard_by_id=_load_shard_by_id,
        append_run=_append_or_replace_run,
        # Forwarded to run_shard for each child:
        build_config_for_user=build_script_config,
        write_config_to_path=_write_config_to_path,
        prepare_forma_3l_token=prepare_forma_3l_token,
        prepare_revizto_tokens=prepare_revizto_tokens,
        check_l4_dependency=_check_l4_dependency,
        update_shard=_update_one_shard,
        blob_upload_file=Blob_Client.upload_file,
        blob_generate_url=Blob_Client.generate_download_url,
        blob_container_for_platform=Blob_Client.container_for_platform,
        resolve_script_path=resolve_script_path,
        stamp_l3_run=_stamp_l3_run,
    )

    return JSONResponse({
        "status":      "queued",
        "message":     f"Compiler '{compiler.get('name', compiler_id)}' running {len(compiler['shard_ids'])} shards.",
        "compiler_id": compiler_id,
    })


# ═══════════════════════════════════════════════════════════════════════════
# GET /api/scheduler/runs/{id} — fetch one run by id (for UI polling)
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/api/scheduler/runs/{run_id}")
async def scheduler_get_run(run_id: str, user: str = Depends(require_user)):
    runs = load_runs()
    run  = next((r for r in runs if r.get("id") == run_id), None)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


# ═══════════════════════════════════════════════════════════════════════════
# DELETE /api/scheduler/runs/{id} — clear a run record + its output blobs
# ═══════════════════════════════════════════════════════════════════════════
# Behaviour rules locked in by the spec:
#   - Removes the run record from automation_runs.json
#   - Also deletes the output blobs from Azure Storage (so they don't sit
#     around for 15 days waiting for the lifecycle rule to clean them up)
#   - For compiler-kind runs: cascades — deletes all child shard run
#     records AND their output blobs
#   - Refuses to delete a run that's currently in 'running' status —
#     killing the record while a subprocess is mid-flight could orphan it.
#     Returns 409 with a helpful message in that case.
#   - Returns a summary of what was removed: how many records, how many
#     blobs (with per-blob success/failure counts).
@app.delete("/api/scheduler/runs/{run_id}")
async def scheduler_delete_run(run_id: str, user: str = Depends(require_user)):
    runs = load_runs()
    target = next((r for r in runs if r.get("id") == run_id), None)
    if target is None:
        raise HTTPException(status_code=404, detail="Run not found")

    # Refuse to delete an in-flight run
    if target.get("status") == "running":
        raise HTTPException(
            status_code=409,
            detail="This run is still in progress. Wait for it to finish before deleting.",
        )

    # Build the set of run IDs to remove. For compiler runs, also include
    # child shards so the deletion is consistent — otherwise the children
    # would become orphans (no top-level row, no parent visible, dead state).
    ids_to_remove = {run_id}
    if target.get("kind") == "compiler":
        for r in runs:
            if r.get("kind") == "shard" and r.get("parent_compiler_run_id") == run_id:
                # Don't cascade into a still-running child either
                if r.get("status") == "running":
                    raise HTTPException(
                        status_code=409,
                        detail=f"Child shard run {r.get('id')} is still in progress. "
                               "Wait for it to finish before deleting the compiler run.",
                    )
                ids_to_remove.add(r.get("id"))

    # Collect every output_blob across the records we're about to delete.
    # These are the things to remove from Azure Storage.
    blobs_to_delete = []
    for r in runs:
        if r.get("id") in ids_to_remove:
            for b in (r.get("output_blobs") or []):
                container = b.get("container")
                blob_path = b.get("blob_path")
                if container and blob_path:
                    blobs_to_delete.append((container, blob_path))

    # Delete the blobs first. If some fail (network glitch, already gone),
    # we still proceed with the record deletion — leaving orphaned blob
    # records in JSON would be worse than orphaned blobs in storage,
    # which the lifecycle rule will sweep up in 15 days anyway.
    blob_results = {"deleted": 0, "failed": 0, "errors": []}
    for container, blob_path in blobs_to_delete:
        try:
            ok = Blob_Client.delete_blob(container, blob_path)
            if ok:
                blob_results["deleted"] += 1
            else:
                # Blob didn't exist — count as success (idempotent)
                blob_results["deleted"] += 1
        except Exception as e:
            blob_results["failed"] += 1
            # Truncate error to avoid bloating the response
            blob_results["errors"].append(f"{blob_path}: {str(e)[:120]}")

    # Now remove the records themselves
    new_runs = [r for r in runs if r.get("id") not in ids_to_remove]
    save_runs(new_runs)

    return JSONResponse({
        "deleted_run_id":     run_id,
        "deleted_record_ids": sorted(ids_to_remove),
        "records_removed":    len(ids_to_remove),
        "blobs":              blob_results,
    })


# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# BLOCK 4 END                                                                ▲
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# BLOCK 5 — CRON FIRING ENGINE (boot + shutdown + factory)                  ▼
# ═══════════════════════════════════════════════════════════════════════════
#
# Boots APScheduler when the FastAPI process starts. Registers a job for
# each active compiler. Each job's fire callback delegates back to
# Scheduler_Runner.run_compiler() with the right per-compiler kwargs.
#
# The factory pattern (run_compiler_kwargs_factory) lets us build the
# kwargs fresh at fire time instead of capturing them at boot. That way
# any later updates to (for example) Blob_Client functions are picked up
# without restarting the scheduler.

def _build_run_compiler_kwargs(compiler_id: str) -> dict:
    """Factory called by Scheduler_Engine when a job fires. Returns the
    kwargs dict that Scheduler_Runner.run_compiler() needs at fire time.

    The triggering_user is determined inside Scheduler_Engine itself
    (it uses compiler.created_by), so we don't include it here."""
    return {
        # Persistence
        "load_shard_by_id":      _load_shard_by_id,
        "append_run":            _append_or_replace_run,
        # Forwarded to run_shard for each child shard
        "build_config_for_user":   build_script_config,
        "write_config_to_path":    _write_config_to_path,
        "prepare_forma_3l_token":  prepare_forma_3l_token,
        "prepare_revizto_tokens":  prepare_revizto_tokens,
        "check_l4_dependency":     _check_l4_dependency,
        "update_shard":            _update_one_shard,
        "blob_upload_file":        Blob_Client.upload_file,
        "blob_generate_url":       Blob_Client.generate_download_url,
        "blob_container_for_platform": Blob_Client.container_for_platform,
        "resolve_script_path":     resolve_script_path,
        "stamp_l3_run":            _stamp_l3_run,
    }


@app.on_event("startup")
async def _b5_boot_scheduler():
    """Boot the cron engine after the app is fully wired up.
    Non-fatal on failure — the cockpit still runs without scheduling."""
    try:
        ok = Scheduler_Engine.boot(
            load_compilers=load_compilers,
            load_shard_by_id=_load_shard_by_id,
            load_runs=load_runs,
            save_runs=save_runs,
            run_compiler_kwargs_factory=_build_run_compiler_kwargs,
        )
        if not ok:
            print("[block5] Scheduler engine did not boot — cron firing disabled", flush=True)
    except Exception as e:
        print(f"[block5] Boot error (non-fatal): {type(e).__name__}: {e}", flush=True)


@app.on_event("shutdown")
async def _b5_shutdown_scheduler():
    """Stop the scheduler cleanly on app exit."""
    try:
        Scheduler_Engine.shutdown()
    except Exception as e:
        print(f"[block5] Shutdown error (non-fatal): {type(e).__name__}: {e}", flush=True)

# ═══════════════════════════════════════════════════════════════════════════
# BLOCK 5 END                                                                ▲
# ═══════════════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════════
# BLOCK 6A — AGENT API TOKEN SYSTEM                                         ▼
# ═══════════════════════════════════════════════════════════════════════════
# Per-user API tokens for the cockpit-agent (a small Windows program that
# runs on an admin's laptop and downloads completed scheduler runs to a
# local folder per shard). Block 6A is the cockpit half of that system:
#
#   - token storage at agent_tokens.json in cockpit-data blob
#   - generate / list / revoke endpoints (cockpit UI uses these)
#   - bearer-token auth for agent-facing endpoints
#   - new endpoints the agent will call (pending runs / mark-delivered)
#   - schema additions: shard.destination.local_path; run.delivered_by[]
#
# DESIGN DECISIONS:
#   - Tokens are SHA-256 hashed at rest. Plaintext is shown to the user
#     ONCE at create-time and never again. Standard practice — if the
#     storage leaks, hashes alone are useless.
#   - Token format: ckpt_<32 hex chars>. The "ckpt_" prefix makes them
#     visually identifiable in logs and accidental git commits.
#   - Each token is scoped to a single user and an "install_date_iso"
#     timestamp. The agent only sees runs that COMPLETED after this date,
#     so reinstalling/regenerating doesn't redeliver historical runs.
#   - Last-used-at is updated on each successful auth. Helps detect
#     stale or potentially-leaked tokens.
#   - Cockpit UI endpoints use the existing session-cookie auth
#     (require_user). Agent endpoints accept BOTH session cookies and
#     bearer tokens (require_user_or_token) — so if you're testing in a
#     browser while logged in, the same URL works for you AND for the
#     agent. Convenient for diagnostics.

import secrets, hashlib

AGENT_TOKEN_PREFIX = "ckpt_"          # token literal prefix (visible)
AGENT_TOKEN_BYTES  = 16               # 32 hex chars after prefix


# ── load/save: token list ────────────────────────────────────────────────
# Stored at agent_tokens.json in cockpit-data, same pattern as users.json.
# Cache invalidates on every write so subsequent reads see fresh data.

def load_agent_tokens() -> list:
    cached = _cache_get("agent_tokens.json")
    if cached is not None:
        return cached
    if USE_BLOB:
        data = blob_read("agent_tokens.json")
        if data is not None:
            _cache_set("agent_tokens.json", data)
            return data
    _cache_set("agent_tokens.json", [])
    return []


def save_agent_tokens(tokens: list) -> None:
    if USE_BLOB:
        blob_write("agent_tokens.json", tokens)
    _cache_invalidate("agent_tokens.json")


# ── helpers ──────────────────────────────────────────────────────────────

def _hash_token(plaintext: str) -> str:
    """Hash a token for storage. SHA-256 with no salt is fine here because
    the input space (16 random bytes = 128 bits of entropy) makes brute
    force infeasible — there's nothing to attack."""
    return "sha256:" + hashlib.sha256(plaintext.encode("utf-8")).hexdigest()


def _generate_token_plaintext() -> str:
    """Return a fresh plaintext token. Caller is responsible for showing
    it to the user once and storing only the hash."""
    return AGENT_TOKEN_PREFIX + secrets.token_hex(AGENT_TOKEN_BYTES)


def _resolve_user_from_bearer(authorization: str | None) -> str | None:
    """Given an Authorization header value (e.g. 'Bearer ckpt_abc...'),
    return the email of the token's owner if it's valid + not revoked.
    Returns None for any failure (invalid header, no token, revoked, etc.).
    Updates last_used_at on success."""
    if not authorization:
        return None
    parts = authorization.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    plaintext = parts[1].strip()
    if not plaintext.startswith(AGENT_TOKEN_PREFIX):
        return None

    target_hash = _hash_token(plaintext)
    tokens = load_agent_tokens()
    matched = None
    for t in tokens:
        if t.get("revoked"):
            continue
        if t.get("token_hash") == target_hash:
            matched = t
            break
    if not matched:
        return None

    # Bump last_used_at. Best-effort — failure to record this should not
    # block the auth itself.
    try:
        matched["last_used_at"] = int(time.time() * 1000)
        save_agent_tokens(tokens)
    except Exception as e:
        print(f"[agent-tokens] Failed to update last_used_at: {e}", flush=True)

    return matched.get("user")


def require_user_or_token(
    authorization: str | None = Header(default=None),
    user_session: str | None = Depends(get_current_user),
) -> str:
    """Auth dependency that accepts EITHER session cookie or bearer token.
    Used for endpoints the agent will call. Returns the resolved user
    email or raises 401."""
    if user_session:
        return user_session
    user_token = _resolve_user_from_bearer(authorization)
    if user_token:
        return user_token
    raise HTTPException(status_code=401, detail="Authentication required.")


# ── COCKPIT UI ENDPOINTS — token management ──────────────────────────────
# These use require_user (session cookies) so a stolen agent token can't
# be used to view or revoke other tokens — defense in depth.

@app.get("/api/agent-tokens")
async def api_agent_tokens_list(user: str = Depends(require_user)):
    """List the current user's tokens (without hashes). Owner can see all
    tokens via owner-only diagnostic; everyone else sees only their own."""
    all_tokens = load_agent_tokens()
    visible = [t for t in all_tokens if t.get("user") == user]
    # Strip the hash before returning — never expose it
    cleaned = []
    for t in visible:
        cleaned.append({
            "id":               t.get("id"),
            "name":             t.get("name", ""),
            "prefix":           t.get("prefix", ""),
            "user":             t.get("user"),
            "created_at":       t.get("created_at"),
            "install_date_iso": t.get("install_date_iso"),
            "last_used_at":     t.get("last_used_at"),
            "revoked":          bool(t.get("revoked")),
            "revoked_at":       t.get("revoked_at"),
        })
    return {"tokens": cleaned}


@app.post("/api/agent-tokens")
async def api_agent_tokens_create(request: Request,
                                   user: str = Depends(require_user)):
    """Create a new token. Returns the plaintext value ONCE — caller must
    save it immediately because it's never shown again.

    Request body (JSON): {"name": "Display label, e.g. 'My laptop'"}"""
    payload = {}
    try:
        payload = await request.json()
    except Exception:
        pass
    name = (payload.get("name") or "").strip()
    if not name:
        return JSONResponse({"error": "Token name is required."}, status_code=400)
    if len(name) > 80:
        return JSONResponse({"error": "Token name must be 80 characters or fewer."}, status_code=400)

    plaintext = _generate_token_plaintext()
    now_ms    = int(time.time() * 1000)
    now_iso   = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    new_token = {
        "id":               _new_id("tok"),
        "user":             user,
        "name":             name,
        "token_hash":       _hash_token(plaintext),
        # First 8 chars of the hex part (NOT the prefix) — shown in the UI
        # so users can identify which token is which without seeing the full
        # value. e.g. plaintext "ckpt_abc12345..." → display prefix "abc12345"
        "prefix":           plaintext[len(AGENT_TOKEN_PREFIX):len(AGENT_TOKEN_PREFIX)+8],
        "created_at":       now_ms,
        "install_date_iso": now_iso,
        "last_used_at":     None,
        "revoked":          False,
        "revoked_at":       None,
    }
    tokens = load_agent_tokens()
    tokens.append(new_token)
    save_agent_tokens(tokens)

    # Return the plaintext ONCE in the response. The cockpit UI is
    # responsible for showing it to the user with copy-to-clipboard.
    return {
        "token":     plaintext,                     # ONLY time this appears
        "id":        new_token["id"],
        "name":      new_token["name"],
        "prefix":    new_token["prefix"],
        "created_at": new_token["created_at"],
        "install_date_iso": new_token["install_date_iso"],
    }


@app.delete("/api/agent-tokens/{token_id}")
async def api_agent_tokens_revoke(token_id: str,
                                   user: str = Depends(require_user)):
    """Revoke (soft-delete) a token. Idempotent — revoking an already-revoked
    token is a no-op success. We keep revoked tokens in storage so audit
    history stays intact (you can see which tokens were ever issued)."""
    tokens = load_agent_tokens()
    target = next((t for t in tokens if t.get("id") == token_id), None)
    if not target:
        return JSONResponse({"error": "Token not found."}, status_code=404)
    # Authorisation: only the owning user (or system owner) can revoke
    if target.get("user") != user and not is_owner(user):
        return JSONResponse({"error": "You can only revoke your own tokens."}, status_code=403)
    if target.get("revoked"):
        return {"revoked": token_id, "already_revoked": True}
    target["revoked"]    = True
    target["revoked_at"] = int(time.time() * 1000)
    save_agent_tokens(tokens)
    return {"revoked": token_id}


# ── AGENT-FACING ENDPOINTS — bearer token auth ───────────────────────────
# These accept either session cookies or bearer tokens (via the
# require_user_or_token dependency). The agent uses bearer tokens.

@app.get("/api/agent/health")
async def api_agent_health(user: str = Depends(require_user_or_token)):
    """Sanity-check endpoint for the agent. Returns the resolved user
    so the agent can confirm its token works and which account it
    represents. Used in setup + as a heartbeat."""
    return {
        "ok":     True,
        "user":   user,
        "server_time_iso": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


@app.get("/api/agent/runs/pending")
async def api_agent_runs_pending(user: str = Depends(require_user_or_token),
                                  authorization: str | None = Header(default=None)):
    """Return runs that the agent should download for the requesting user.

    Block 8 (claim-based delivery): a run is "pending" when nobody's agent
    has claimed it yet. Once any admin's agent successfully delivers, that
    run is hidden from EVERY admin's pending list — first-claim wins.

    A run is "pending delivery" iff:
      - status is 'success' or 'partial' (NOT 'running' or 'failed')
      - kind is 'shard' AND ran via a compiler (parent_compiler_run_id set)
      - the originating shard has a destination.local_path configured
      - delivered_by is empty (no one has claimed yet)
      - run completed AFTER the requesting agent's token install_date_iso
    """
    # Determine the install_date cutoff for this token (only relevant when
    # auth was via bearer token; cookie auth has no install_date concept,
    # so we pass through and use a far-past sentinel).
    install_cutoff_iso = "1970-01-01T00:00:00Z"
    if authorization:
        # Re-resolve the token to find its install_date. Cheap — load is cached.
        plaintext = (authorization or "").split(None, 1)[-1].strip()
        if plaintext.startswith(AGENT_TOKEN_PREFIX):
            target_hash = _hash_token(plaintext)
            for t in load_agent_tokens():
                if t.get("token_hash") == target_hash and not t.get("revoked"):
                    install_cutoff_iso = t.get("install_date_iso") or install_cutoff_iso
                    break

    # Build a fast lookup: shard_id -> destination.local_path
    shards = load_shards()
    shard_local_path = {}
    for s in shards:
        dest = s.get("destination") or {}
        lp   = dest.get("local_path")
        if lp:
            shard_local_path[s["id"]] = lp

    runs = load_runs()
    pending = []
    for r in runs:
        if r.get("kind") != "shard":
            continue
        if not r.get("parent_compiler_run_id"):
            continue
        if r.get("status") not in ("success", "partial"):
            continue
        # Block 8: claim-based filter — skip if anyone has claimed.
        if _run_claim_owner(r):
            continue
        ended_at = r.get("ended_at") or ""
        if ended_at and ended_at < install_cutoff_iso:
            continue
        sid = r.get("shard_id")
        local_path = shard_local_path.get(sid)
        if not local_path:
            continue  # no destination configured for this shard's agent path

        # Strip stdout_tail to keep the response light. The agent doesn't
        # need it — it only cares about output_blobs and the local_path.
        pending.append({
            "id":           r.get("id"),
            "shard_id":     sid,
            "shard_name":   r.get("shard_name", ""),
            "platform":     r.get("platform", ""),
            "script":       r.get("script", ""),
            "ended_at":     ended_at,
            "output_blobs": r.get("output_blobs") or [],
            "local_path":   local_path,
        })

    return {"pending": pending, "user": user, "install_cutoff": install_cutoff_iso}


# ── Block 8 helper: read claim owner from a run record ────────────────────
# Backward-compat: handles BOTH old list-shape `delivered_by: [user1, user2]`
# AND new string-shape `delivered_by: "user@x.com"`. Returns None if empty.
# This is the central place that interprets the claim — call it everywhere
# rather than reading delivered_by directly.
def _run_claim_owner(run: dict) -> str | None:
    raw = run.get("delivered_by")
    if not raw:
        return None
    if isinstance(raw, str):
        return raw or None
    if isinstance(raw, list):
        # Pre-Block-8 shape — first element wins. If list is empty → None.
        return raw[0] if raw else None
    # Unknown shape — treat as no claim (defensive)
    return None


@app.post("/api/agent/runs/{run_id}/mark-delivered")
async def api_agent_runs_mark_delivered(run_id: str,
                                          user: str = Depends(require_user_or_token)):
    """Block 8: claim-based delivery. The first agent to call this 'wins' —
    the run is marked claimed by that user. Subsequent calls are idempotent
    no-ops that return the existing owner.

    Returns:
      - {"ok": true, "run_id": ..., "delivered_by": "<user>"} on first claim
      - {"ok": true, "already_claimed": true, "delivered_by": "<other_user>"}
        if someone else got there first. Agent should treat this as
        success (not failure) and proceed.

    The race window between two agents calling at the same instant is
    handled by last-write-wins on the JSON file. In practice the only cost
    is a redundant blob download by the second agent — harmless because
    files write to the same shared path with identical content.
    """
    runs = load_runs()
    target = next((r for r in runs if r.get("id") == run_id), None)
    if not target:
        return JSONResponse({"error": "Run not found."}, status_code=404)

    existing_owner = _run_claim_owner(target)
    if existing_owner and existing_owner != user:
        # Already claimed by someone else — return ok=true with the flag
        # so the agent knows the file is already accounted for and doesn't
        # need to retry. Still 200, not an error.
        return {
            "ok":              True,
            "already_claimed": True,
            "run_id":          run_id,
            "delivered_by":    existing_owner,
        }

    if existing_owner == user:
        # Idempotent — same user calling twice is fine
        return {"ok": True, "run_id": run_id, "delivered_by": user}

    # Fresh claim — mark it
    target["delivered_by"] = user
    save_runs(runs)
    return {"ok": True, "run_id": run_id, "delivered_by": user}


# ── Block 8: one-shot migration of existing list-shape records ────────────
# Converts delivered_by: ["user1", "user2"] → delivered_by: "user1" in place.
# Idempotent — already-string records pass through. Safe to run on every
# startup. Logs how many records changed.
def _migrate_delivered_by_list_to_string():
    try:
        runs = load_runs()
        if not runs:
            return
        changed = 0
        for r in runs:
            raw = r.get("delivered_by")
            if isinstance(raw, list):
                if raw:
                    r["delivered_by"] = raw[0]   # first claimant wins
                else:
                    r["delivered_by"] = None     # empty list → null
                changed += 1
        if changed:
            save_runs(runs)
            print(f"[block8] Migrated {changed} run record(s) from delivered_by:list to delivered_by:string", flush=True)
    except Exception as e:
        # Migration failure is non-fatal — code handles both shapes anyway.
        print(f"[block8] delivered_by migration error (non-fatal): {e}", flush=True)


# Run the migration once at startup (idempotent — safe on every boot)
_migrate_delivered_by_list_to_string()


# ═══════════════════════════════════════════════════════════════════════════
# BLOCK 6A END                                                              ▲
# ═══════════════════════════════════════════════════════════════════════════


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