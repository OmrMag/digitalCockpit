from Forma_MasterScript import get_token
import requests, json

token = get_token()
all_users = []
offset = 0

while True:
    response = requests.get(
        f"https://developer.api.autodesk.com/hq/v1/accounts/ef9891c5-6a55-4f0a-8f9f-cf9d8f9e7e44/users",
        headers={"Authorization": f"Bearer {token}"},
        params={"limit": 100, "offset": offset}
    )
    batch = response.json()
    if not batch:
        break
    for u in batch:
        if u.get("email") == "oawad@alec.ae":
            print(f"ID: {u['id']}")
            print(f"Name: {u['name']}")
            print(f"Role: {u['role']}")
            exit()
    offset += len(batch)
    if len(batch) < 100:
        break

print("Not found")