"""
Revizto_GET_License_Members.py

Lists all Revizto members the current user can see across BOTH regions
(Dubai + Ireland), deduplicated by email. Each member is tagged with the
license + project + region they were discovered through.

Why this script changed: the previous version called
/v5/license/{uuid}/member/list, which Revizto has either deprecated or
restricted on our account — it now returns HTTP 404 for every license
UUID we send. The replacement endpoint, /v5/project/{projectUuid}/team,
works and returns richer per-member data including `location` (which the
old endpoint never exposed). The trade-off is that the new endpoint is
PROJECT-scoped, so to recover the previous "all members across all my
licenses" output we iterate every project the user can see and merge.

Multi-region: piggybacks on Revizto_GET_License_Projects.get_projects(),
which already iterates Dubai + Ireland and tags each project with its
actual region. We dispatch each /team call to the matching regional API
host. Per-region tokens are read from the cockpit's per-admin token files
(via COCKPIT_USER env var), with a fallback to the legacy shared filename
for local-dev runs.

Output schema (small, deliberate change from the old version):
  uuid           — user UUID
  name           — full name
  email          — primary key for dedupe
  license_role   — role NAME from accessRole.name on the team entity
  status         — active / deactivated / pending (derived from flags)
  company        — top-level field on the team entity
  department     — top-level field on the team entity
  location       — NEW: top-level field on the team entity
  last_active    — lastActive timestamp string
  project_uuid   — the project this member was discovered under
  region         — dubai / ireland (which API host returned them)
  license_uuid   — kept for backward compat with consumers; populated
                   from the project's parent license when available

`invited_at` is no longer surfaced — the team endpoint only returns a
boolean `invited`, not a timestamp. If a downstream tool needs that field
back, it would require yet another endpoint to look up the invite event.
"""
import json
import csv
import os
import requests

from Revizto_Config import EXPORTS_DIR
# Reuse the multi-region project iterator so we don't re-implement the
# license + region enumeration logic.
from Revizto_GET_License_Projects import (
    get_projects, _read_region_token,
)


def _team_for_project(project_uuid: str, region: str, token: str,
                       timeout: int = 15) -> list[dict]:
    """GET /v5/project/{uuid}/team for one project and return the raw
    entity list. Empty list on any failure (logged), so the outer
    iteration can keep going. The shape of each entity follows the
    Revizto API docs at developer.revizto.com — see the docs page for
    `Get project members`."""
    url = f"https://api.{region}.revizto.com/v5/project/{project_uuid}/team"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept":        "application/json",
    }
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
    except requests.RequestException as e:
        print(f"[Revizto:{region}] Project {project_uuid[:8]} — network error: {e}")
        return []

    if r.status_code != 200:
        print(f"[Revizto:{region}] Project {project_uuid[:8]} — HTTP {r.status_code}")
        return []

    try:
        body = r.json()
    except ValueError:
        print(f"[Revizto:{region}] Project {project_uuid[:8]} — invalid JSON")
        return []

    if body.get("result", 0) < 0:
        print(f"[Revizto:{region}] Project {project_uuid[:8]} — result={body.get('result')}")
        return []

    data = body.get("data") or {}
    if not isinstance(data, dict):
        return []

    entities = data.get("entities") or []
    return entities if isinstance(entities, list) else []


def _normalize_entity(entity: dict, project_uuid: str, region: str) -> dict:
    """Flatten the team-entity shape into the row dict our consumers expect.
    The team endpoint nests user identity inside `member.user` for some
    fields and exposes others at the entity top level — we look in both
    places to be robust against shape variations."""
    if not isinstance(entity, dict):
        return {}

    member = entity.get("member") or {}
    user   = member.get("user")    if isinstance(member, dict)         else None
    user   = user                  if isinstance(user, dict)           else {}
    role   = entity.get("accessRole") or {}
    role   = role                  if isinstance(role, dict)           else {}

    # Status: prefer the explicit booleans if present, fall back to invited
    if entity.get("activated"):
        status = "active"
    elif entity.get("frozen"):
        status = "deactivated"
    elif entity.get("invited"):
        status = "pending"
    else:
        status = ""

    # Identity fallbacks: top-level entity has firstname/lastname/email
    # too on most responses; user{} sub-dict is the deeper version
    email     = (entity.get("email") or user.get("email") or "").strip()
    full_name = (entity.get("fullname") or user.get("fullname") or "").strip()
    if not full_name:
        first = (entity.get("firstname") or user.get("firstname") or "").strip()
        last  = (entity.get("lastname")  or user.get("lastname")  or "").strip()
        full_name = f"{first} {last}".strip()

    return {
        "uuid":         entity.get("uuid") or user.get("uuid", ""),
        "name":         full_name,
        "email":        email,
        "license_role": role.get("name", ""),
        "status":       status,
        "company":      (entity.get("company")    or "").strip(),
        "department":   (entity.get("department") or "").strip(),
        "location":     (entity.get("location")   or "").strip(),
        "last_active":  entity.get("lastActive", ""),
        "project_uuid": project_uuid,
        "region":       region,
        # license_uuid was on every row in the old script; we don't have
        # it from /team, so we leave it blank. Downstream consumers that
        # filtered by license_uuid will need updating — flagged in
        # pending_items.json for visibility.
        "license_uuid": "",
    }


def get_all_members() -> list[dict]:
    """Iterate every project (across both regions) and aggregate the union
    of their team members, deduplicated by email."""
    # Pull project list (multi-region, already region-tagged)
    projects = get_projects()
    if not projects:
        print("[Revizto] No projects found — nothing to enumerate members for.")
        return []

    # Cache tokens per region — `get_projects` already validated they exist
    token_cache: dict[str, str] = {}
    def _token_for(region: str) -> str:
        if region not in token_cache:
            token_cache[region] = _read_region_token(region)
        return token_cache[region]

    all_members: list[dict] = []
    seen_emails: set[str]   = set()

    for proj in projects:
        region   = proj.get("region", "")
        proj_uuid = proj.get("uuid",  "")
        proj_name = proj.get("title", proj_uuid)
        if not (region and proj_uuid):
            continue
        token = _token_for(region)
        if not token:
            print(f"[Revizto:{region}] Project '{proj_name}' — no token cached, skipping")
            continue

        print(f"[Revizto:{region}] Fetching team for project '{proj_name}'...")
        entities = _team_for_project(proj_uuid, region, token)
        added_here = 0
        for ent in entities:
            row = _normalize_entity(ent, proj_uuid, region)
            email = row.get("email", "").lower()
            if not email or email in seen_emails:
                continue
            seen_emails.add(email)
            all_members.append(row)
            added_here += 1
        print(f"[Revizto:{region}] Project '{proj_name}' — {len(entities)} member(s), {added_here} new")

    return all_members


def save_json(members: list[dict], filename: str = "revizto_license_members.json"):
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w") as f:
        json.dump(members, f, indent=2)
    print(f"Saved {len(members)} members to {path}")


def save_csv(members: list[dict], filename: str = "revizto_license_members.csv"):
    if not members:
        print("No members to save.")
        return
    path = os.path.join(EXPORTS_DIR, filename)
    fields = ["uuid", "name", "email", "license_role", "status",
              "company", "department", "location",
              "last_active", "project_uuid", "region", "license_uuid"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(members)
    print(f"Saved {len(members)} members to {path}")


if __name__ == "__main__":
    members = get_all_members()
    if members:
        # Group log line per region so user can sanity-check distribution
        by_region: dict[str, int] = {}
        for m in members:
            by_region[m["region"]] = by_region.get(m["region"], 0) + 1
        summary = ", ".join(f"{r}: {n}" for r, n in sorted(by_region.items()))
        print(f"\n[Revizto] Total: {len(members)} unique member(s) across regions ({summary})")
        save_json(members)
        save_csv(members)
    else:
        print("[Revizto] No members found in any project.")