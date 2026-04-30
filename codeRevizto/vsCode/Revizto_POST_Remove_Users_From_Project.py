import json
import requests
from Revizto_Config import BASE_URL, PROJECT_UUID
from Revizto_MasterScript import get_headers
from Revizto_GET_Project_Members import get_project_members


def remove_user_from_project(project_uuid: str, user_uuid: str) -> dict:
    """Remove a user from a project."""
    response = requests.delete(
        f"{BASE_URL}/project/{project_uuid}/role/member/{user_uuid}",
        headers=get_headers(),
        timeout=15,
    )
    print(f"[Revizto] Remove user status={response.status_code}")
    if response.status_code != 200:
        print(f"[Revizto] Error: {response.text[:300]}")
        return {}
    return response.json()


def remove_users_from_project(project_uuid: str, user_uuids: list[str]) -> list[dict]:
    """Remove multiple users from a project."""
    results = []
    for uuid in user_uuids:
        result = remove_user_from_project(project_uuid, uuid)
        results.append({"uuid": uuid, "result": result})
    return results


if __name__ == "__main__":
    if not PROJECT_UUID:
        print("[Revizto] No active project set. Select a Revizto project in the sidebar first.")
        exit(1)

    print(f"[Revizto] Fetching members for project: {PROJECT_UUID}")
    members = get_project_members(PROJECT_UUID)
    if not members:
        print("[Revizto] No members found.")
        exit(1)

    print(f"\nProject members ({len(members)}):")
    for i, m in enumerate(members, 1):
        print(f"  {i}. {m['name']} ({m['email']}) — {m['project_role']}")

    choice = input("\nSelect member number to remove (or comma-separated list): ").strip()
    indices = [int(x.strip()) - 1 for x in choice.split(",") if x.strip().isdigit()]
    selected = [members[i] for i in indices if 0 <= i < len(members)]

    if not selected:
        print("No valid selection.")
        exit(1)

    print(f"\nAbout to remove {len(selected)} user(s):")
    for m in selected:
        print(f"  - {m['name']} ({m['email']})")

    confirm = input("\nConfirm? (y/n): ").strip().lower()
    if confirm != "y":
        print("Cancelled.")
        exit(0)

    results = remove_users_from_project(PROJECT_UUID, [m["uuid"] for m in selected])
    print(f"\nDone: {json.dumps(results, indent=2)}")