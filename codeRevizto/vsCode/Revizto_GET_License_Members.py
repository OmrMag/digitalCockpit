import json
import csv
import os
import requests
from Revizto_Config import BASE_URL, EXPORTS_DIR
from Revizto_MasterScript import get_headers
from Revizto_GET_User_Licenses import get_user_licenses


def get_license_members(license_uuid: str) -> list[dict]:
    """Get all members in a license."""
    response = requests.get(
        f"{BASE_URL}/license/{license_uuid}/member/list",
        headers=get_headers(),
        timeout=15,
    )
    print(f"[Revizto] License members status={response.status_code}")
    if response.status_code != 200:
        print(f"[Revizto] Error: {response.text[:300]}")
        return []
    data = response.json().get("data", {})
    if not isinstance(data, dict):
        return []

    role_map = {0: "Owner", 1: "License Admin", 2: "Collaborator", 3: "Content Creator", 4: "Guest"}
    members  = []
    for member_id, member in data.items():
        user = member.get("user", {})
        members.append({
            "uuid":         user.get("uuid", ""),
            "name":         user.get("fullname", ""),
            "email":        user.get("email", ""),
            "license_role": role_map.get(member.get("role"), str(member.get("role", ""))),
            "status":       "active" if member.get("activated") else "deactivated" if member.get("deactivated") else "pending",
            "company":      member.get("company", ""),
            "department":   member.get("department", ""),
            "invited_at":   member.get("invitedAt", ""),
            "last_active":  user.get("lastActive", ""),
            "license_uuid": license_uuid,
        })
    return members


def get_all_members() -> list[dict]:
    """Get members across all user licenses, deduplicated by email."""
    licenses = get_user_licenses()
    all_members = []
    seen = set()
    for lic in licenses:
        lic_uuid = lic.get("uuid", "")
        lic_name = lic.get("name", lic_uuid)
        print(f"[Revizto] Fetching members for license '{lic_name}'...")
        members = get_license_members(lic_uuid)
        for m in members:
            email = m.get("email", "").lower()
            if email and email not in seen:
                seen.add(email)
                all_members.append(m)
        print(f"[Revizto] License '{lic_name}' — {len(members)} member(s)")
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
    fields = ["uuid", "name", "email", "license_role", "status", "company",
              "department", "invited_at", "last_active", "license_uuid"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(members)
    print(f"Saved {len(members)} members to {path}")


if __name__ == "__main__":
    members = get_all_members()
    if members:
        save_json(members)
        save_csv(members)
        print(f"\nTotal members: {len(members)}")
    else:
        print("[Revizto] No members found.")