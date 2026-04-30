import json
import os
import requests
from Morta_Config import API_KEY, _ROOT_DIR
from Morta_GET_User import find_user_by_email

BASE_URL    = "https://api.morta.io"
HEADERS     = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type":  "application/json",
    "Accept":        "application/json",
}
EXPORTS_DIR = os.path.join(_ROOT_DIR, "exports")
TAGS_JSON   = os.path.join(EXPORTS_DIR, "morta_tags.json")


def load_tags(json_path: str = TAGS_JSON) -> dict:
    """Load tags from exports JSON. Returns name-keyed dict."""
    if not os.path.exists(json_path):
        print(f"[Morta] Tags file not found: {json_path}")
        print("[Morta] Run Morta_GET_Tags.py first to generate it.")
        return {}
    with open(json_path) as f:
        data = json.load(f)
    # Support both list format (new) and name-keyed dict (legacy)
    if isinstance(data, list):
        return {t["name"]: t for t in data if t.get("name")}
    return data


def assign_tag(user_id: str, tag_reference_id: str) -> dict:
    """POST a tag to a user."""
    response = requests.post(
        f"{BASE_URL}/v1/user/{user_id}/tags",
        headers=HEADERS,
        json={"tagReferenceId": tag_reference_id},
        timeout=10,
    )
    print(f"[Morta] Status: {response.status_code}")
    if response.status_code != 200:
        print(f"[Morta] Error: {response.text}")
        return {}
    return response.json()


def assign_tag_by_email(email: str, tag_name: str) -> dict:
    """Assign a tag to a user by their email and tag name."""
    user = find_user_by_email(email)
    if not user:
        print(f"[Morta] No user found with email: {email}")
        return {}
    user_id = user.get("public_id", "").strip()
    if not user_id:
        print(f"[Morta] No public_id found for {email}")
        return {}
    tags = load_tags()
    tag  = tags.get(tag_name)
    if not tag:
        print(f"[Morta] Tag not found: {tag_name}")
        print(f"[Morta] Available tags: {', '.join(tags.keys())}")
        return {}
    tag_ref_id = tag["tag_reference_id"]
    print(f"Assigning '{tag_name}' to {user.get('name')} ({email})")
    print(f"  User ID:  {user_id}")
    print(f"  Tag Ref:  {tag_ref_id}")
    return assign_tag(user_id, tag_ref_id)


if __name__ == "__main__":
    email = input("Enter user email: ").strip()
    user  = find_user_by_email(email)
    if not user:
        print(f"No user found with email: {email}")
        exit()

    print(f"\nUser:        {user.get('name')}")
    print(f"Current tag: {user.get('tags', 'None')}")

    tags = load_tags()
    if not tags:
        exit()

    print(f"\nAvailable tags:")
    for i, name in enumerate(tags.keys(), 1):
        print(f"  {i}. {name}")

    choice   = int(input("\nSelect tag number: ").strip())
    tag_name = list(tags.keys())[choice - 1]
    result   = assign_tag_by_email(email, tag_name)
    if result:
        print(f"\nSuccess: {json.dumps(result, indent=2)}")