"""
Profile_Status_Check.py
=======================
Lightweight readiness probe for an admin's profile page.

Probes the admin's Morta API key + Revizto access (across both regions)
without going through the import-time-cached Morta_Config / Revizto_Config
modules — those load a single hardcoded ../../config.json at import time
and can't represent a specific admin's per-user config.

Public API
----------
    check_admin_status(config: dict, email: str | None = None) -> dict

        config: the dict returned by app.load_config(user)
        email:  the admin's email — needed to locate per-user Revizto
                token files (revizto_tokens_{region}_{safe_email}.json).
                Pass None only for ad-hoc CLI testing against the legacy
                shared revizto_tokens_{region}.json file.

    Returns:
        {
            "kind":  "ok" | "action_needed" | "error",
            "detail": "...",                                 # human-readable summary
            "morta":   {"ok": bool, "detail": str},
            "revizto": {"ok": bool, "detail": str, "region": str | None},
        }

Status rule
-----------
    ok            ← Morta API ready AND at least one Revizto region works
    action_needed ← Morta missing OR both Revizto regions missing/broken
    error         ← unexpected exception during the check (never raises)

Why no Forma?
-------------
Forma uses shared 2-legged credentials with an owner-config fallback. Any
admin whose admin-of-record has Forma configured can use Forma — there's
no per-admin Forma readiness state to check. Morta + Revizto are the two
platforms that need the admin's own credentials to function.

Used by
-------
    app.py → GET /profile
"""

from __future__ import annotations
import json
from pathlib import Path

import requests


# ── Tunables ────────────────────────────────────────────────────────────────
TIMEOUT_S = 5
MORTA_BASE_URL = "https://api.morta.io"
REVIZTO_REGIONS = ("dubai", "ireland")

# Sibling files (revizto_tokens_*) are expected next to this module.
BASE_DIR = Path(__file__).resolve().parent


# ── Helpers ─────────────────────────────────────────────────────────────────
def _safe_email(email: str) -> str:
    """Match the same email-to-filename scheme app.py uses."""
    return email.lower().replace("@", "_at_").replace(".", "_")


def _load_revizto_tokens(region: str, email: str | None) -> dict:
    """
    Mirror app.py's load_revizto_tokens(): try the per-admin file first,
    fall back to the legacy shared revizto_tokens_{region}.json. Tokens
    stored in Azure blob storage are *not* read here — the status check
    is intentionally local-only so it can't trigger a blob fetch on the
    /profile page render.
    """
    if email:
        per_user = BASE_DIR / f"revizto_tokens_{region}_{_safe_email(email)}.json"
        if per_user.exists():
            try:
                with open(per_user) as f:
                    return json.load(f)
            except (OSError, ValueError):
                pass

    legacy = BASE_DIR / f"revizto_tokens_{region}.json"
    if legacy.exists():
        try:
            with open(legacy) as f:
                return json.load(f)
        except (OSError, ValueError):
            pass

    return {}


def _save_revizto_tokens(region: str, email: str | None, data: dict) -> None:
    """
    Persist refreshed/exchanged tokens. Writes per-admin when email given,
    legacy shared file otherwise. Failure is silent — token refresh is a
    side-benefit of the probe, not its purpose.
    """
    try:
        if email:
            path = BASE_DIR / f"revizto_tokens_{region}_{_safe_email(email)}.json"
        else:
            path = BASE_DIR / f"revizto_tokens_{region}.json"
        with open(path, "w") as f:
            json.dump(data, f, indent=4)
    except OSError:
        pass


# ── Morta probe ─────────────────────────────────────────────────────────────
def _check_morta(api_key: str, hub_id: str) -> dict:
    """
    Hit GET /v1/user/me with the given API key. Requires BOTH api_key AND
    hub_id to be configured — a key without a hub means the admin can't
    actually do anything in the app yet.
    """
    if not api_key:
        return {"ok": False, "detail": "Morta API key not configured"}
    if not hub_id:
        return {"ok": False, "detail": "Morta hub not configured"}

    try:
        r = requests.get(
            f"{MORTA_BASE_URL}/v1/user/me",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=TIMEOUT_S,
        )
    except requests.exceptions.Timeout:
        return {"ok": False, "detail": "Morta API timed out"}
    except requests.exceptions.RequestException as e:
        return {"ok": False, "detail": f"Morta unreachable: {type(e).__name__}"}

    if r.status_code == 200:
        data = (r.json() or {}).get("data") or {}
        if data.get("id"):
            return {"ok": True, "detail": "Morta API key valid, hub assigned"}
        return {"ok": False, "detail": "Morta returned 200 but no user id"}

    if r.status_code in (401, 403):
        return {"ok": False, "detail": f"Morta API key rejected ({r.status_code})"}
    return {"ok": False, "detail": f"Morta API returned HTTP {r.status_code}"}


# ── Revizto probe (per region) ──────────────────────────────────────────────
def _revizto_access_token(region: str, access_code: str | None,
                           email: str | None) -> str | None:
    """
    Resolve a Revizto access token for `region`. Order of preference:
      1. Refresh from existing tokens file (per-admin > legacy shared)
      2. Exchange `access_code` for a fresh token via /oauth2?code=...

    Mirrors app.revizto_get_token_for_region() but with the explicit args
    needed for the status check, and *only* using the local token file —
    no blob storage. Returns the access_token string or None.
    """
    base_url = f"https://api.{region}.revizto.com/v5"

    # Path A: refresh from existing tokens
    tokens = _load_revizto_tokens(region, email)
    refresh_token = tokens.get("refresh_token", "")
    if refresh_token:
        try:
            r = requests.post(
                f"{base_url}/oauth2",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                timeout=TIMEOUT_S,
            )
            if r.status_code == 200:
                body = r.json() or {}
                new_token = body.get("access_token")
                if new_token:
                    _save_revizto_tokens(region, email, {
                        "access_token":  new_token,
                        "refresh_token": body.get("refresh_token", refresh_token),
                    })
                    return new_token
        except requests.exceptions.RequestException:
            pass  # fall through to access_code path

    # Path B: exchange access_code for first-ever token
    if access_code:
        try:
            r = requests.get(
                f"{base_url}/oauth2",
                params={"code": access_code},
                timeout=TIMEOUT_S,
            )
            if r.status_code == 200:
                body = r.json() or {}
                new_token = body.get("access_token")
                if new_token:
                    _save_revizto_tokens(region, email, {
                        "access_token":  new_token,
                        "refresh_token": body.get("refresh_token", ""),
                    })
                    return new_token
        except requests.exceptions.RequestException:
            return None

    return None


def _check_revizto_region(region: str, region_cfg: dict, email: str | None) -> dict:
    """Verify ONE Revizto region. Returns ok=True only when a token can be
    obtained AND the licenses endpoint responds 200."""
    access_code = (region_cfg or {}).get("access_code", "")
    has_tokens = bool(_load_revizto_tokens(region, email))

    if not access_code and not has_tokens:
        return {"ok": False, "detail": f"{region}: no access_code or saved token"}

    token = _revizto_access_token(region, access_code, email)
    if not token:
        return {"ok": False, "detail": f"{region}: could not obtain access token"}

    base_url = f"https://api.{region}.revizto.com/v5"
    try:
        r = requests.get(
            f"{base_url}/user/licenses",
            headers={"Authorization": f"Bearer {token}"},
            timeout=TIMEOUT_S,
        )
    except requests.exceptions.RequestException as e:
        return {"ok": False, "detail": f"{region}: API unreachable ({type(e).__name__})"}

    if r.status_code != 200:
        return {"ok": False, "detail": f"{region}: licenses endpoint returned HTTP {r.status_code}"}

    return {"ok": True, "detail": f"{region}: token valid, licenses returned"}


def _check_revizto(config: dict, email: str | None) -> dict:
    """Try Dubai then Ireland — one working region is enough."""
    last_failure = None
    for region in REVIZTO_REGIONS:
        region_cfg = config.get(f"revizto_{region}", {}) or {}
        result = _check_revizto_region(region, region_cfg, email)
        result["region"] = region
        if result["ok"]:
            return result
        last_failure = result

    if last_failure:
        last_failure["detail"] = (
            f"No working Revizto region. {last_failure['detail']}"
        )
        return last_failure
    return {"ok": False, "detail": "Revizto not configured in any region", "region": None}


# ── Public entry point ──────────────────────────────────────────────────────
def check_admin_status(config: dict, email: str | None = None) -> dict:
    """
    Run both probes and roll up the results. Never raises — wraps any
    unexpected exception in a {"kind": "error"} response.

    config: the merged config dict from app.load_config(user)
    email:  the admin's email, used to locate per-user Revizto token files.
            Pass None only for CLI testing against the legacy shared file.
    """
    try:
        morta_cfg = (config or {}).get("morta", {}) or {}
        morta = _check_morta(
            api_key=morta_cfg.get("api_key", ""),
            hub_id=morta_cfg.get("hub_id", ""),
        )
        revizto = _check_revizto(config or {}, email)

        if morta["ok"] and revizto["ok"]:
            kind = "ok"
            detail = f"Morta + Revizto ({revizto.get('region', '?')}) ready"
        else:
            failed = []
            if not morta["ok"]:
                failed.append("Morta")
            if not revizto["ok"]:
                failed.append("Revizto")
            kind = "action_needed"
            detail = f"Action needed: {', '.join(failed)}"

        return {
            "kind": kind,
            "detail": detail,
            "morta": morta,
            "revizto": revizto,
        }

    except Exception as e:
        return {
            "kind": "error",
            "detail": f"Status check failed: {type(e).__name__}: {e}",
            "morta":   {"ok": False, "detail": "not checked"},
            "revizto": {"ok": False, "detail": "not checked", "region": None},
        }


# ── CLI for ad-hoc testing ──────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    cfg_path = Path(sys.argv[1]) if len(sys.argv) > 1 else BASE_DIR / "config.json"
    cli_email = sys.argv[2] if len(sys.argv) > 2 else None
    if not cfg_path.exists():
        print(f"Config file not found: {cfg_path}")
        sys.exit(1)
    with open(cfg_path) as f:
        cfg = json.load(f)
    result = check_admin_status(cfg, email=cli_email)
    print(json.dumps(result, indent=2))