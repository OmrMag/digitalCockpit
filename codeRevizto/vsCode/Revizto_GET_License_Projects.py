"""
Revizto_GET_License_Projects.py

Lists ALL Revizto projects across BOTH regions (Dubai + Ireland) in a single
run, regardless of which region is currently set as active in the cockpit's
config. Each project is tagged with the region whose API actually returned it
so downstream consumers can dispatch to the right host for follow-up calls.

Why both regions: a single Revizto account can hold projects in multiple
regional data residencies (Dubai for UAE, Ireland for EU/UK). The cockpit's
default config-driven helpers (`Revizto_Config.py`, `Revizto_MasterScript.py`)
target ONE region at a time — fine for most extractions, but inadequate for
"inventory my projects everywhere". This script bypasses those single-region
constants and reads tokens directly from per-region token files, building
per-region BASE_URLs as it goes.

Failure handling: regions where the access token is missing or expired get
skipped with a clear log line (no exception). The output is still useful even
when only one region's tokens are valid.

Output: deduped list of {id, uuid, title, region}, JSON + CSV in the standard
EXPORTS_DIR. Backward-compatible shape — same keys as before, just with
multiple region values represented.
"""
import json
import csv
import os
import requests

from Revizto_Config import EXPORTS_DIR  # only the dirs/paths we still need

# Regions we know about. Add more here if Revizto launches new data centers.
REGIONS = ["dubai", "ireland"]


def _root_dir() -> str:
    """Resolve the repo root the same way Revizto_Config does — climb two
    levels up from this file's directory. Required to locate the per-region
    token files (revizto_tokens_<region>.json) which live at the project
    root, not in the script folder."""
    this_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(this_dir, "..", ".."))


def _read_region_token(region: str) -> str:
    """Pull the access_token out of the region's token file. We deliberately
    skip the refresh-or-reauth flow that Revizto_MasterScript.get_headers
    runs — that flow is wired to ONE region at a time via TOKENS_FILE in
    Revizto_Config. Re-implementing the dual-region refresh here would
    duplicate logic; instead we trust whatever's currently saved and let
    the API tell us via 401 if it's stale."""
    path = os.path.join(_root_dir(), f"revizto_tokens_{region}.json")
    if not os.path.exists(path):
        return ""
    try:
        with open(path, "r") as f:
            data = json.load(f)
        return data.get("access_token", "") or ""
    except (json.JSONDecodeError, OSError):
        return ""


def _list_licenses_for_region(region: str, token: str) -> list[dict]:
    """GET /v5/user/licenses against this region's API host. Returns the
    licenses the token's user is a member of in this region. Empty list on
    any failure (logged), so the outer iteration can keep going to the next
    region."""
    url = f"https://api.{region}.revizto.com/v5/user/licenses"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept":        "application/json",
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
    except requests.RequestException as e:
        print(f"[Revizto:{region}] Network error fetching licenses: {e}")
        return []

    if r.status_code != 200:
        print(f"[Revizto:{region}] /user/licenses returned HTTP {r.status_code} — skipping")
        return []

    try:
        return [
            {"id": lic["id"], "uuid": lic["uuid"], "name": lic["name"]}
            for lic in r.json()["data"]["entities"]
        ]
    except (KeyError, TypeError) as e:
        print(f"[Revizto:{region}] Unexpected /user/licenses shape: {e}")
        return []


def _list_projects_for_license(region: str, token: str,
                                lic_uuid: str, lic_name: str) -> list[dict]:
    """GET /v5/project/list/{lic_uuid}/paged for ONE license in ONE region.
    Returns the project list (without region tagging — caller adds that).
    Empty list on failure, with a per-license log line so the user can see
    which licenses contributed and which didn't."""
    url = f"https://api.{region}.revizto.com/v5/project/list/{lic_uuid}/paged"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept":        "application/json",
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
    except requests.RequestException as e:
        print(f"[Revizto:{region}] License '{lic_name}' — network error: {e}")
        return []

    if r.status_code != 200:
        print(f"[Revizto:{region}] License '{lic_name}' — HTTP {r.status_code}")
        return []

    try:
        return r.json()["data"]["data"]
    except (KeyError, TypeError):
        return []


def get_projects() -> list[dict]:
    """Iterate both regions and return a deduped list of all projects the
    current user can see, each tagged with its actual region."""
    all_projects: list[dict] = []
    seen_uuids:   set[str]   = set()

    for region in REGIONS:
        token = _read_region_token(region)
        if not token:
            print(f"[Revizto:{region}] No token saved — skipping this region")
            continue

        licenses = _list_licenses_for_region(region, token)
        if not licenses:
            # Either the token's expired (logged above) or this region has
            # no licenses for this user. Either way, nothing else to do here.
            continue

        for lic in licenses:
            lic_uuid = lic.get("uuid", "")
            lic_name = lic.get("name", lic_uuid)
            if not lic_uuid:
                continue

            projects = _list_projects_for_license(region, token, lic_uuid, lic_name)
            print(f"[Revizto:{region}] License '{lic_name}' — {len(projects)} project(s)")

            for p in projects:
                uuid = p.get("uuid", "")
                # Dedupe by UUID. A project should not appear in two
                # regions, but we guard anyway.
                if not uuid or uuid in seen_uuids:
                    continue
                seen_uuids.add(uuid)
                all_projects.append({
                    "id":     p.get("id", ""),
                    "uuid":   uuid,
                    "title":  p.get("title", ""),
                    "region": region,   # actual region this project lives in
                })

    return all_projects


# ── Output helpers (unchanged from original) ──────────────────────────────

def save_json(projects: list[dict], filename: str = "revizto_license_projects.json"):
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w") as f:
        json.dump(projects, f, indent=2)
    print(f"Saved {len(projects)} projects to {path}")


def save_csv(projects: list[dict], filename: str = "revizto_license_projects.csv"):
    if not projects:
        print("No projects to save.")
        return
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "uuid", "title", "region"])
        writer.writeheader()
        writer.writerows(projects)
    print(f"Saved {len(projects)} projects to {path}")


if __name__ == "__main__":
    projects = get_projects()
    if projects:
        # Group log line per region so user can sanity-check distribution
        by_region: dict[str, int] = {}
        for p in projects:
            by_region[p["region"]] = by_region.get(p["region"], 0) + 1
        summary = ", ".join(f"{r}: {n}" for r, n in sorted(by_region.items()))
        print(f"[Revizto] Total: {len(projects)} project(s) across regions ({summary})")
        print(json.dumps(projects, indent=4))
        save_json(projects)
        save_csv(projects)
    else:
        print("[Revizto] No projects found in any region.")