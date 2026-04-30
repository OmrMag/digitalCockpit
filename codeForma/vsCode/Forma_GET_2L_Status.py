import requests
from Forma_Config import CLIENT_ID, CLIENT_SECRET

response = requests.post(
    "https://developer.api.autodesk.com/authentication/v2/token",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    data={
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "account:read data:read",
    }
)

print(response.status_code)
print(response.json())