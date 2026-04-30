import requests, json

API_KEY = "YOUR_KEY"
HUB_ID = "YOUR_HUB_ID"

response = requests.get(
    f"https://api.morta.io/v1/hub/406ed81a-5e01-47df-b8e4-f0ddbf474d8a",
    headers={"Authorization": f"Bearer EqfHYwJXOaVcrJLTKniGnJPm6seNkk3E5VTlKF9YYto"},
    timeout=10
)
print(response.status_code)
print(json.dumps(response.json(), indent=2))