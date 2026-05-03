import json
import os
from Morta_Config import EXPORTS_DIR
from Morta_GET_members import get_hub_members, to_email_dict

MEMBERS_JSON = os.path.join(EXPORTS_DIR, "morta_members.json")


def find_user_by_email(email: str):
    """Fetch fresh members from API, save to JSON, and look up by email."""
    members = get_hub_members()
    if not members:
        return None

    email_dict = to_email_dict(members)

    os.makedirs(EXPORTS_DIR, exist_ok=True)
    with open(MEMBERS_JSON, "w") as f:
        json.dump(email_dict, f, indent=2)

    return email_dict.get(email.strip().lower())


if __name__ == "__main__":
    email = input("Enter user email: ").strip()
    user  = find_user_by_email(email)
    if user:
        print(f"\nName:  {user.get('name')}")
        print(f"Email: {user.get('email')}")
        print(f"Role:  {user.get('project_role')}")
        print(f"Tags:  {user.get('tags')}")
        print(f"ID:    {user.get('public_id')}")
    else:
        print(f"No user found with email: {email}")