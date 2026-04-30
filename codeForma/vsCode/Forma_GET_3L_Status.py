import requests
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from Forma_Config import CLIENT_ID_3L, CLIENT_SECRET_3L

CALLBACK_URL = "http://localhost:8080/"
SCOPES       = "data:read account:read"

auth_code = None


class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code
        params    = parse_qs(urlparse(self.path).query)
        auth_code = params.get("code", [None])[0]
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Authorization successful! You can close this window.")

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    auth_url = (
        f"https://developer.api.autodesk.com/authentication/v2/authorize"
        f"?response_type=code"
        f"&client_id={CLIENT_ID_3L}"
        f"&redirect_uri={CALLBACK_URL}"
        f"&scope={SCOPES.replace(' ', '%20')}"
    )
    print("Opening browser for login...")
    webbrowser.open(auth_url)

    print("Waiting for authorization...")
    server = HTTPServer(("localhost", 8080), CallbackHandler)
    server.handle_request()

    if not auth_code:
        print("No auth code received.")
        exit()

    print("Auth code received!")

    response = requests.post(
        "https://developer.api.autodesk.com/authentication/v2/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type":    "authorization_code",
            "code":          auth_code,
            "client_id":     CLIENT_ID_3L,
            "client_secret": CLIENT_SECRET_3L,
            "redirect_uri":  CALLBACK_URL,
        }
    )

    if response.status_code == 200:
        token = response.json().get("access_token")
        print(f"\n3-legged token successfully obtained!")
        print(f"Token: {token[:30]}...")
    else:
        print(f"\nFailed: {response.status_code} {response.json()}")