from Revizto_MasterScript import get_headers

headers = get_headers()
print("Token:", headers["Authorization"][:50], "...")

import requests
r = requests.get(
    "https://api.ireland.revizto.com/v5/project/e8beae62-1016-4928-898a-d9052f8a5b95/object-properties/get-tree",
    headers=headers,
    params={
        "sceneReviztoId": 8751744224124975359,
        "modelReviztoId": 8769050866186381624,
        "limit": 100,
        "angularUnit": "deg",
        "linearUnit": "mm",
    },
    timeout=30
)
print(r.status_code)
print(r.text[:500])