import requests
import json

from Forma_Config import ACCOUNT_ID, PROJECT_ID, ADMIN_USER_ID
from Forma_MasterScript import get_token
from Forma_GET_Account_Projects import get_projects

BASE_URL = "https://developer.api.autodesk.com"

# Default product access for new users
DEFAULT_PRODUCTS = [
    {"key": "docs", "access": "member"}
]


def get_admin_user_id():
    """Get the admin's Autodesk user ID for the User-Id header (2-legged impersonation)."""
    token = get_token()
    url = f"{BASE_URL}/hq/v1/accounts/{ACCOUNT_ID}/users"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"limit": 100}

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        for u in response.json():
            if u.get("role") == "account_admin":
                return u.get("id")
    print("[Forma] Could not find admin user ID")
    return None


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
        "users": [
            {
                "email": email,
                "products": products or DEFAULT_PRODUCTS,
            }
        ],
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
        entry = {
            "email": u["email"],
            "products": products or DEFAULT_PRODUCTS,
        }
        if u.get("firstName"):
            entry["firstName"] = u["firstName"]
        if u.get("lastName"):
            entry["lastName"] = u["lastName"]
        user_entries.append(entry)

    payload = {
        "users": user_entries,
        "suppressAdministrativeEmails": False,
    }

    response = requests.post(url, headers=headers, json=payload, timeout=60)
    print(f"[Forma] Status: {response.status_code}")
    if response.status_code not in (200, 202):
        print(f"[Forma] Error: {response.text}")
        return {}
    return response.json()


if __name__ == "__main__":
    admin_id = ADMIN_USER_ID
    print(f"Admin ID: {admin_id}")

    # Select project
    projects = get_projects(status="active", platform="acc")
    if not projects:
        print("No projects found.")
        exit()

    print("\nAvailable projects:")
    for i, p in enumerate(projects, 1):
        print(f"  {i}. {p['name']}")

    choice = int(input("\nSelect project number: ").strip())
    project_id = projects[choice - 1]["id"]

    email = input("Enter user email to invite: ").strip()

    result = invite_to_project(project_id, email, admin_id)
    if result:
        print(f"\nSuccess: {json.dumps(result, indent=2)}")