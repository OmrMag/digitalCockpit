import requests
import json
import csv

from Forma_Config import ACCOUNT_ID
from Forma_MasterScript import get_token


def get_all_users():
    token = get_token()
    url = f"https://developer.api.autodesk.com/hq/v1/accounts/{ACCOUNT_ID}/users"
    headers = {"Authorization": f"Bearer {token}"}

    all_users = []
    offset = 0
    limit = 100

    while True:
        params = {"limit": limit, "offset": offset}
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            batch = response.json()
            if not batch:
                break
            all_users.extend(batch)
            offset += len(batch)
            print(f"  Fetched {len(all_users)} users so far...")
            if len(batch) < limit:
                break
        else:
            print(f"[Forma] Error {response.status_code}: {response.text}")
            break

    return all_users


if __name__ == "__main__":
    users = get_all_users()
    print(f"\nTotal users: {len(users)}")

    fields = ["id", "name", "email", "role", "status", "company_name", "last_sign_in"]
    col_widths = [38, 30, 40, 15, 10, 30, 26]

    header = "  ".join(f"{fields[i]:<{col_widths[i]}}" for i in range(len(fields)))
    print("\n" + header)
    print("-" * len(header))
    for u in users:
        row = "  ".join(f"{str(u.get(fields[i], '') or ''):<{col_widths[i]}}" for i in range(len(fields)))
        print(row)

    with open("forma_hub_users.json", "w") as f:
        json.dump(users, f, indent=4)
    print("\nSaved forma_hub_users.json")

    with open("forma_hub_users.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(users)
    print("Saved forma_hub_users.csv")