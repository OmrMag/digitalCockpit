import json
import os
import sys
from Morta_Config import EXPORTS_DIR
from Morta_GET_members import get_hub_members, to_email_dict

MEMBERS_JSON = os.path.join(EXPORTS_DIR, "morta_members.json")
USER_JSON    = os.path.join(EXPORTS_DIR, "morta_user.json")


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
    # When running as a cockpit subprocess, default to the admin's own
    # email so the extraction works without interactive stdin (which is
    # closed in subprocess context — input() raises EOFError there).
    # Falls back to input() for standalone terminal usage.
    email = os.environ.get("COCKPIT_USER", "").strip().lower()
    if not email:
        try:
            email = input("Enter user email: ").strip().lower()
        except EOFError:
            print("[Morta] No email provided (no COCKPIT_USER env var, no stdin available).")
            sys.exit(1)

    if not email:
        print("[Morta] No email provided.")
        sys.exit(1)

    print(f"[Morta] Looking up '{email}' in active hub...")
    user = find_user_by_email(email)

    os.makedirs(EXPORTS_DIR, exist_ok=True)

    if not user:
        # Still emit an output file so the cockpit's bake/preview flow has
        # something to render (avoids "no files produced" confusion when the
        # extraction succeeded but found no match).
        with open(USER_JSON, "w") as f:
            json.dump({"email": email, "found": False}, f, indent=2)
        print(f"[Morta] No user found with email '{email}' in the active hub.")
        print(f"[Morta] Saved empty record to {USER_JSON}")
        sys.exit(0)

    with open(USER_JSON, "w") as f:
        json.dump(user, f, indent=2)

    print(f"\nName:  {user.get('name')}")
    print(f"Email: {user.get('email')}")
    print(f"Role:  {user.get('project_role')}")
    print(f"Tags:  {user.get('tags')}")
    print(f"ID:    {user.get('public_id')}")
    print(f"\n[Morta] Saved user record