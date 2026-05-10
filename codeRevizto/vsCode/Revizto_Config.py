"""
Revizto_Config.py — reads from digitalCockpit/config.json

When running under the web app as a subprocess, COCKPIT_CONFIG is set to the
path of a temp file containing the triggering admin's merged config. This ensures
each admin's script runs with their own credentials and the correct project context.

When running directly from VS Code, COCKPIT_CONFIG is not set and the script
falls back to the local config.json as before.
"""
import json
import os

_THIS_DIR   = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR   = os.path.abspath(os.path.join(_THIS_DIR, "..", ".."))

# COCKPIT_CONFIG: path to the admin's resolved config (set by web app before subprocess).
CONFIG_FILE = os.environ.get("COCKPIT_CONFIG") or os.path.join(_ROOT_DIR, "config.json")

# COCKPIT_EXPORTS_DIR: where the script should write output files.
EXPORTS_DIR = os.environ.get("COCKPIT_EXPORTS_DIR") or os.path.join(_ROOT_DIR, "exports")
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
        if _full_cfg.get(f"revizto_{_r}", {}).get("access_code"):
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
# TOKENS_FILE: local disk fallback only — when COCKPIT_USER is set the
# MasterScript uses blob storage instead.
TOKENS_FILE = os.path.join(EXPORTS_DIR, f"revizto_tokens_{REGION}.json")

# ── Active project (set by sidebar context selector) ──────────────────────
PROJECT_UUID = _ctx.get("revizto_project_uuid", "")
PROJECT_NAME = _ctx.get("revizto_project_name", "")

# ── Active scene + model (set by /extractions Fetch Models dropdown) ──────
# When the user picks a model from the cockpit's extractions page, the
# /api/context/revizto-model endpoint persists the selection into
# config["revizto"]["scene_id"] / ["model_id"] / ["model_name"].
# This mirrors how Forma persists model_item_id under config["forma"].
#
# Scripts that target a specific scene/model (e.g. Object Tree) check
# these and use them; if absent, they fall back to "first model" so the
# standalone-from-VS-Code workflow still works.
#
# Stored as strings on the wire (JSON-stringified from JS), but Revizto's
# scene/model IDs are large signed integers. Coerce here so consumers
# always get an int — or None if not set.
_revizto_block = _full_cfg.get("revizto", {})

def _coerce_int(val):
    if val in (None, "", 0):
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None

SCENE_ID   = _coerce_int(_revizto_block.get("scene_id"))
MODEL_ID   = _coerce_int(_revizto_block.get("model_id"))
MODEL_NAME = _revizto_block.get("model_name", "")