import json
import requests
from Revizto_Config import BASE_URL, DEFAULT_LICENSE_ROLE, PROJECT_UUID
from Revizto_MasterScript import get_headers
from Revizto_GET_User_Licenses import get_user_licenses
from Revizto_GET_Project_Roles import get_project_roles


def get_license_and_auth() -> tuple[str, str]:
    """Resolve primary license UUID and auth method from API."""
    licenses = get_user_licenses()
    if not licenses:
        return "", ""
    lic_uuid = licenses[0]["uuid"]

    # Fetch auth methods for this license
    r = requests.get(
        f"{BASE_URL}/license/{lic_uuid}/auth-methods",
        headers=get_headers(),
        timeout=15,
    )
    auth_method = ""
    if r.status_code == 200:
        try:
            methods = r.json()["data"]["entities"]
            if methods:
                auth_method = methods[0].get("uuid", "")
        except (KeyError, TypeError):
            pass
    return lic_uuid, auth_method


def invite_to_license(emails: list[str], first_names: list[str] = None,
                      last_names: list[str] = None, companies: list[str] = None,
                      departments: list[str] = None, role: int = DEFAULT_LICENSE_ROLE) -> dict:
    """Invite users to the Revizto license."""
    license_uuid, auth_method = get_license_and_auth()
    if not license_uuid:
        print("[Revizto] Could not resolve license UUID.")
        return {}

    data = []
    for i, email in enumerate(emails):
        data.append({
            "email":      email,
            "role":       role,
            "firstName":  first_names[i]  if first_names  and i < len(first_names)  else "",
            "lastName":   last_names[i]   if last_names   and i < len(last_names)   else "",
            "tags":       [],
            "authMethod": auth_method,
            "company":    companies[i]    if companies    and i < len(companies)    else "",
            "department": departments[i]  if departments  and i < len(departments)  else "",
        })

    payload = {
        "data":          data,
        "preserveRoles": True,
        "makeGuests":    False,
        "authMethod":    auth_method,
        "deactivate":    False,
    }
    response = requests.post(
        f"{BASE_URL}/license/{license_uuid}/invite/bulk",
        headers=get_headers(),
        json=payload,
        timeout=30,
    )
    print(f"[Revizto License] Status: {response.status_code}")
    if response.status_code != 200:
        print(f"[Revizto License] Error: {response.text}")
        return {}
    return response.json()


def invite_to_project(project_uuid: str, emails: list[str], role_id: int = None,
                      company: str = "", department: str = "") -> dict:
    """Invite users to a specific Revizto project."""
    payload = {"invitations": emails, "company": company, "department": department}
    if role_id:
        payload["roleId"] = role_id
    response = requests.post(
        f"{BASE_URL}/project/{project_uuid}/role/invite",
        headers=get_headers(),
        json=payload,
        timeout=30,
    )
    print(f"[Revizto Project] Status: {response.status_code}")
    if response.status_code != 200:
        print(f"[Revizto Project] Error: {response.text}")
        return {}
    return response.json()


def invite_user(email: str, first_name: str = "", last_name: str = "",
                company: str = "", department: str = "",
                project_uuid: str = None, project_role_id: int = None,
                license_role: int = DEFAULT_LICENSE_ROLE) -> dict:
    """Full invite: license + optionally project."""
    results = {}
    print(f"\n--- Inviting {email} to license ---")
    results["license"] = invite_to_license(
        emails=[email], first_names=[first_name], last_names=[last_name],
        companies=[company], departments=[department], role=license_role,
    )
    if project_uuid:
        print(f"\n--- Inviting {email} to project ---")
        results["project"] = invite_to_project(
            project_uuid=project_uuid, emails=[email],
            company=company, department=department, role_id=project_role_id,
        )
    return results


if __name__ == "__main__":
    email      = input("Enter user email: ").strip()
    first_name = input("First name: ").strip()
    last_name  = input("Last name: ").strip()
    company    = input("Company: ").strip()
    department = input("Department: ").strip()

    # Use active project from config, optionally pick a role
    project_uuid    = PROJECT_UUID or None
    project_role_id = None

    if project_uuid:
        print(f"\nUsing active project: {project_uuid}")
        roles = get_project_roles()
        if roles:
            print("\nAvailable project roles:")
            for i, r in enumerate(roles, 1):
                print(f"  {i}. {r['name']}")
            role_choice = input("\nSelect role number (or Enter for default): ").strip()
            if role_choice:
                project_role_id = roles[int(role_choice) - 1]["id"]

    result = invite_user(
        email=email, first_name=first_name, last_name=last_name,
        company=company, department=department,
        project_uuid=project_uuid, project_role_id=project_role_id,
    )
    print(f"\nResult: {json.dumps(result, indent=2)}")