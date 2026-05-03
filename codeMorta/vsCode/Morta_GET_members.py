import json
import csv
import os
import requests
from Morta_Config import API_KEY, HUB_ID, EXPORTS_DIR

BASE_URL = "https://api.morta.io"
HEADERS  = {"Authorization": f"Bearer {API_KEY}"}


def get_hub_members() -> list:
    """Fetch all members from the active Morta hub."""
    response = requests.get(
        f"{BASE_URL}/v1/hub/{HUB_ID}/members",
        headers=HEADERS,
        timeout=15,
    )
    print(f"[Morta] Members status={response.status_code}")
    if response.status_code != 200:
        print(f"[Morta] Error: {response.text[:300]}")
        return []
    return response.json().get("data", [])


def to_email_dict(members: list) -> dict:
    """Convert member list to email-keyed dict."""
    result = {}
    for m in members:
        u = m.get("user", {})
        email = u.get("email", "").lower().strip()
        if email:
            tags = [
                {"name": t.get("value", ""), "tag_reference_id": t.get("publicId", "")}
                for t in u.get("tags", [])
                if isinstance(t, dict)
            ]
            result[email] = {
                "name":         u.get("name", ""),
                "email":        email,
                "public_id":    u.get("publicId", ""),
                "project_role": m.get("projectRole", ""),
                "role_method":  m.get("roleAssignedMethod", ""),
                "tags":         tags,
                "joined":       m.get("createdAt", ""),
            }
    return result


def flatten_members(members: list) -> list:
    """Flatten member records for CSV export."""
    rows = []
    for m in members:
        u    = m.get("user", {})
        tags = [t.get("value", "") for t in u.get("tags", []) if isinstance(t, dict)]
        rows.append({
            "name":        u.get("name", ""),
            "email":       u.get("email", ""),
            "public_id":   u.get("publicId", ""),
            "role":        m.get("projectRole", ""),
            "role_method": m.get("roleAssignedMethod", ""),
            "tags":        ", ".join(tags),
            "joined":      m.get("createdAt", ""),
        })
    return rows


def find_user_by_email(email: str):
    """Fetch fresh members and look up by email."""
    members = get_hub_members()
    if not members:
        return None
    return to_email_dict(members).get(email.strip().lower())


def save_json(members: list, filename: str = "morta_members.json"):
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w") as f:
        json.dump(members, f, indent=2)
    print(f"[Morta] Saved {len(members)} members to {path}")


def save_csv(members: list, filename: str = "morta_members.csv"):
    rows = flatten_members(members)
    if not rows:
        print("[Morta] No members to save.")
        return
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"[Morta] Saved {len(rows)} members to {path}")


if __name__ == "__main__":
    if not HUB_ID:
        print("[Morta] No active hub set. Select a Morta hub in the sidebar first.")
        exit(1)

    members = get_hub_members()
    if members:
        save_json(members)
        save_csv(members)
        print(f"[Morta] Done. {len(members)} members extracted.")
    else:
        print("[Morta] No members returned from Morta API.")