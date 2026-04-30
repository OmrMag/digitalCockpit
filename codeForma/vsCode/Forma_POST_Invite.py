import requests
import json
import os

from Forma_Config import ACCOUNT_ID, PROJECT_ID, ADMIN_USER_ID, EXPORTS_DIR
from Forma_MasterScript import get_token
from Forma_GET_Account_Projects import get_projects

BASE_URL = "https://developer.api.autodesk.com"

DEFAULT_PRODUCTS = [
    {"key": "docs", "access": "member"}
]


def invite_to_project(project_id: str, email: str, user_id_header: str = ADMIN_USER_ID,
                      products: list = None) -> dict:
    """Invite a single user to a Forma project using v2 bulk endpoint."""
    token = get_token()
    url = f"{BASE_URL}/construction/admin/v2/projects/{project_id}/users:import"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "User-Id": user_id_header,
    }
    payload = {
        "users": [{"email": email, "products": products or DEFAULT_PRODUCTS}],
        "suppressAdministrativeEmails": False,
    }
    response = requests.post(url, headers=headers, json=payload, timeout=30)
    print(f"[Forma] Status: {response.status_code}")
    if response.status_code not in (200, 202):
        print(f"[Forma] Error: {response.text}")
        return {}
    return response.json()


def invite_bulk_to_project(project_id: str, users: list[dict], user_id_header: str = ADMIN_USER_ID,
                           products: list = None) -> dict:
    """Invite multiple users to a Forma project. Max 200 per request."""
    token = get_token()
    url = f"{BASE_URL}/construction/admin/v2/projects/{project_id}/users:import"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "User-Id": user_id_header,
    }
    user_entries = []
    for u in users:
        entry = {"email": u["email"], "products": products or DEFAULT_PRODUCTS}
        if u.get("firstName"):
            entry["firstName"] = u["firstName"]
        if u.get("lastName"):
            entry["lastName"] = u["lastName"]
        user_entries.append(entry)

    payload = {"users": user_entries, "suppressAdministrativeEmails": False}
    response = requests.post(url, headers=headers, json=payload, timeout=60)
    print(f"[Forma] Status: {response.status_code}")
    if response.status_code not in (200, 202):
        print(f"[Forma] Error: {response.text}")
        return {}
    return response.json()


if __name__ == "__main__":
    if not ADMIN_USER_ID:
        print("[Forma] No admin user ID configured. Set it in Settings.")
        exit(1)

    if not PROJECT_ID:
        print("[Forma] No active project set. Select a Forma/ACC project in the sidebar first.")
        exit(1)

    print(f"Admin ID:   {ADMIN_USER_ID}")
    print(f"Project ID: {PROJECT_ID}\n")

    email = input("Enter user email to invite: ").strip()
    if not email:
        print("No email entered.")
        exit(1)

    result = invite_to_project(PROJECT_ID, email, ADMIN_USER_ID)
    if result:
        print(f"\nSuccess: {json.dumps(result, indent=2)}")