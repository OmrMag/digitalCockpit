"""
Morta_Config.py — reads from digitalCockpit/config.json
"""
import json
import os

_THIS_DIR   = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR   = os.path.abspath(os.path.join(_THIS_DIR, "..", ".."))
CONFIG_FILE = os.path.join(_ROOT_DIR, "config.json")


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