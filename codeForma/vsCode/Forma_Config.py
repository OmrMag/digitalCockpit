"""
Forma_Config.py — reads from digitalCockpit/config.json
"""
import json
import os

_THIS_DIR   = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR   = os.path.abspath(os.path.join(_THIS_DIR, "..", ".."))
CONFIG_FILE = os.path.join(_ROOT_DIR, "config.json")
EXPORTS_DIR = os.path.join(_ROOT_DIR, "exports")

# Ensure exports folder exists — also used by MasterScript for token file paths
os.makedirs(EXPORTS_DIR, exist_ok=True)


def _load():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    return {}


_full_cfg = _load()
_cfg      = _full_cfg.get("forma", {})
_ctx      = _full_cfg.get("active_context", {})

# ── Static credentials (set in Settings page) ──────────────────────────────
CLIENT_ID        = _cfg.get("client_id", "")
CLIENT_SECRET    = _cfg.get("client_secret", "")
CLIENT_ID_3L     = _cfg.get("client_id_3l", "")
CLIENT_SECRET_3L = _cfg.get("client_secret_3l", "")
ACCOUNT_ID       = _cfg.get("account_id", "")
ADMIN_USER_ID    = _cfg.get("admin_user_id", "")

# ── Active project (set by sidebar context selector) ──────────────────────
PROJECT_ID   = _ctx.get("forma_project_id", "") or _cfg.get("project_id", "")
PROJECT_NAME = _ctx.get("forma_project_name", "")
PLATFORM     = _ctx.get("forma_platform", "")

# ── Active model (set by Extractions page model selector) ─────────────────
MODEL_ITEM_ID = _cfg.get("model_item_id", "")
MODEL_NAME    = _cfg.get("model_name", "")