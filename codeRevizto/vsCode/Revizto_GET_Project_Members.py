import json
import csv
import os
import requests
from Revizto_Config import BASE_URL, PROJECT_UUID, EXPORTS_DIR
from Revizto_MasterScript import get_headers


def get_project_members(project_uuid: str) -> list[dict]:
    """Get all members in a project.

    Endpoint per Revizto v5 API docs: GET /v5/project/{projectUuid}/team
    Response shape:
        {
          "result": 0,
          "data": {
            "count": <int>,
            "entities": [<member objects>]
          },
          "message": "Success."
        }

    Each entity has top-level: email, fullname, status, suspendReason,
    activated, frozen, role, language, id, uuid, firstname, lastname,
    company, department, location, lastActive, accessRole {id, uuid,
    name, system}, member { id, role, uuid, user { ... } }, etc.
    """
    response = requests.get(
        f"{BASE_URL}/project/{project_uuid}/team",
        headers=get_headers(),
        timeout=15,
    )
    print(f"[Revizto] Project team status={response.status_code}")
    if response.status_code != 200:
        print(f"[Revizto] Error: {response.text[:300]}")
        return []

    body = response.json()
    if body.get("result") != 0:
        print(f"[Revizto] API result code: {body.get('result')} - {body.get('message', '')}")
        return []

    entities = (body.get("data") or {}).get("entities") or []
    members = []
    for ent in entities:
        # Top-level entity fields are the most reliable; fall back to
        # nested user object for fields not present at the top level.
        user_obj = (ent.get("member") or {}).get("user") or {}
        access_role = ent.get("accessRole") or {}
        members.append({
            "uuid":         ent.get("uuid") or user_obj.get("uuid", ""),
            "name":         ent.get("fullname") or
                            f"{ent.get('firstname','')} {ent.get('lastname','')}".strip(),
            "email":        ent.get("email") or user_obj.get("email", ""),
            "project_role": access_role.get("name", ""),
            "role_uuid":    access_role.get("uuid", ""),
            "status":       "active" if ent.get("activated") else "pending",
            "company":      ent.get("company", ""),
            "department":   ent.get("department", ""),
            "project_uuid": project_uuid,
        })
    return members


def save_json(members: list[dict], filename: str = "revizto_project_members.json"):
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w") as f:
        json.dump(members, f, indent=2)
    print(f"Saved {len(members)} members to {path}")


def save_csv(members: list[dict], filename: str = "revizto_project_members.csv"):
    if not members:
        print("No members to save.")
        return
    path = os.path.join(EXPORTS_DIR, filename)
    fields = ["uuid", "name", "email", "project_role", "role_uuid",
              "status", "company", "department", "project_uuid"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(members)
    print(f"Saved {len(members)} members to {path}")


if __name__ == "__main__":
    if not PROJECT_UUID:
        print("[Revizto] No active project set. Select a Revizto project in the sidebar first.")
        exit(1)

    print(f"[Revizto] Fetching members for project: {PROJECT_UUID}")
    members = get_project_members(PROJECT_UUID)
    if members:
        save_json(members)
        save_csv(members)
        print(f"\nTotal members: {len(members)}")
    else:
        print("[Revizto] No members found.")