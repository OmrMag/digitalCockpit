"""
Morta_Config.py — reads from digitalCockpit/config.json

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
_cfg      = _full_cfg.get("morta", {})
_ctx      = _full_cfg.get("active_context", {})

API_KEY = _cfg.get("api_key", "")

# ── Active hub (set by sidebar context selector, fallback to morta.hub_id) ─
HUB_ID   = _ctx.get("morta_hub_id", "") or _cfg.get("hub_id", "")
HUB_NAME = _ctx.get("morta_hub_name", "")