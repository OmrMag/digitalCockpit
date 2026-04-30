import json
import csv
import os
import requests
from Revizto_Config import BASE_URL, PROJECT_UUID, EXPORTS_DIR
from Revizto_MasterScript import get_headers


def get_project_members(project_uuid: str) -> list[dict]:
    """Get all members in a project."""
    response = requests.get(
        f"{BASE_URL}/project/{project_uuid}/role/member/list",
        headers=get_headers(),
        timeout=15,
    )
    print(f"[Revizto] Project members status={response.status_code}")
    if response.status_code != 200:
        print(f"[Revizto] Error: {response.text[:300]}")
        return []
    data = response.json().get("data", {})
    if not isinstance(data, dict):
        return []

    members = []
    for member_id, member in data.items():
        user = member.get("user", {})
        members.append({
            "uuid":         user.get("uuid", ""),
            "name":         user.get("fullname", ""),
            "email":        user.get("email", ""),
            "project_role": member.get("roleName", ""),
            "role_uuid":    member.get("roleUuid", ""),
            "status":       "active" if member.get("activated") else "pending",
            "company":      member.get("company", ""),
            "department":   member.get("department", ""),
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