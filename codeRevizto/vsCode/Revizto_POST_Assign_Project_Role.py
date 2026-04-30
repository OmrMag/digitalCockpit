import json
import requests
from Revizto_Config import BASE_URL, PROJECT_UUID
from Revizto_MasterScript import get_headers
from Revizto_GET_Project_Roles import get_project_roles


def assign_project_role(project_uuid: str, user_uuid: str, role_uuid: str) -> dict:
    """Assign a project role to a user."""
    response = requests.post(
        f"{BASE_URL}/project/{project_uuid}/role/{role_uuid}/member/{user_uuid}",
        headers=get_headers(),
        timeout=15,
    )
    print(f"[Revizto] Assign role status={response.status_code}")
    if response.status_code != 200:
        print(f"[Revizto] Error: {response.text[:300]}")
        return {}
    return response.json()


if __name__ == "__main__":
    if not PROJECT_UUID:
        print("[Revizto] No active project set. Select a Revizto project in the sidebar first.")
        exit(1)

    user_uuid = input("Enter user UUID: ").strip()
    if not user_uuid:
        print("No user UUID entered.")
        exit(1)

    roles = get_project_roles()
    if not roles:
        print("[Revizto] No roles found.")
        exit(1)

    print("\nAvailable project roles:")
    for i, r in enumerate(roles, 1):
        print(f"  {i}. {r['name']} (uuid: {r['uuid']})")

    choice    = int(input("\nSelect role number: ").strip())
    role_uuid = roles[choice - 1]["uuid"]

    result = assign_project_role(PROJECT_UUID, user_uuid, role_uuid)
    if result:
        print(f"\nSuccess: {json.dumps(result, indent=2)}")