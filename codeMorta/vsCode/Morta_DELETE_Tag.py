import json
import requests
from Morta_Config import API_KEY
from Morta_GET_User import find_user_by_email

BASE_URL = "https://api.morta.io"
HEADERS  = {"Authorization": f"Bearer {API_KEY}", "Accept": "application/json"}


def remove_tag(user_id: str, tag_id: str) -> dict:
    """DELETE a tag from a user."""
    response = requests.delete(
        f"{BASE_URL}/v1/user/{user_id}/tags/{tag_id}",
        headers=HEADERS,
        timeout=10,
    )
    print(f"[Morta] Status: {response.status_code}")
    if response.status_code != 200:
        print(f"[Morta] Error: {response.text}")
        return {}
    return response.json()


def remove_tag_by_email(email: str, tag_public_id: str) -> dict:
    """Remove a tag from a user by their email and the tag's publicId."""
    user = find_user_by_email(email)
    if not user:
        print(f"[Morta] No user found with email: {email}")
        return {}
    user_id = user.get("public_id", "").strip()
    if not user_id:
        print(f"[Morta] No public_id found for {email}")
        return {}
    print(f"Removing tag from {user.get('name')} ({email})")
    return remove_tag(user_id, tag_public_id)


if __name__ == "__main__":
    email = input("Enter user email: ").strip()
    user  = find_user_by_email(email)
    if not user:
        print(f"No user found with email: {email}")
        exit()

    print(f"\nUser: {user.get('name')}")
    tags = user.get("tags", [])
    if not tags:
        print("This user has no permission tags.")
        exit()

    print(f"\nCurrent tags:")
    for i, tag in enumerate(tags, 1):
        print(f"  {i}. {tag.get('value')} (ID: {tag.get('publicId')})")

    choice       = int(input("\nSelect tag number to remove: ").strip())
    selected_tag = tags[choice - 1]
    confirm      = input(f"Remove '{selected_tag.get('value')}'? (y/n): ").strip().lower()
    if confirm != "y":
        print("Cancelled.")
        exit()

    result = remove_tag_by_email(email, selected_tag["publicId"])
    if result:
        print(f"\nSuccess: {json.dumps(result, indent=2)}")