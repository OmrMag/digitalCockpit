"""
Revizto_Config.py — reads from digitalCockpit/config.json
"""
import json
import os

_THIS_DIR   = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR   = os.path.abspath(os.path.join(_THIS_DIR, "..", ".."))
CONFIG_FILE = os.path.join(_ROOT_DIR, "config.json")        # same path as Forma/Morta configs
EXPORTS_DIR = os.path.join(_ROOT_DIR, "exports")

# Ensure exports folder exists
os.makedirs(EXPORTS_DIR, exist_ok=True)


def _load():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    return {}


_full_cfg = _load()

# ── Region resolution ──────────────────────────────────────────────────────
# Prefer active_context region (set by sidebar), fall back to first configured region
_ctx    = _full_cfg.get("active_context", {})
REGION  = _ctx.get("revizto_region", "")

if not REGION:
    for _r in ["dubai", "ireland"]:
        if _full_cfg.get(f"revizto_{_r}", {}).get("license_uuid"):
            REGION = _r
            break

if not REGION:
    REGION = "dubai"   # last-resort default

# ── Per-region credentials ─────────────────────────────────────────────────
_cfg = _full_cfg.get(f"revizto_{REGION}", {})

BASE_URL             = f"https://api.{REGION}.revizto.com/v5"
LICENSE_UUID         = _cfg.get("license_uuid", "")
AUTH_METHOD          = _cfg.get("auth_method", "")
DEFAULT_LICENSE_ROLE = _cfg.get("default_license_role", 2)
ACCESS_CODE          = _cfg.get("access_code", "")
TOKENS_FILE          = os.path.join(_ROOT_DIR, f"revizto_tokens_{REGION}.json")

# ── Active project (set by sidebar context selector) ──────────────────────
PROJECT_UUID = _ctx.get("revizto_project_uuid", "")
PROJECT_NAME = _ctx.get("revizto_project_name", "")