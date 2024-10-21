import requests
import base64
from dotenv import load_dotenv
import os

load_dotenv()

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

auth_string = f"{CLIENT_ID}:{CLIENT_SECRET}"
b64_auth_string = base64.b64encode(auth_string.encode()).decode()

auth_url = "https://accounts.spotify.com/api/token"
headers = {
    "Authorization": f"Basic {b64_auth_string}"
}

data = {
    "grant_type": "client_credentials"
}

response = requests.post(auth_url, headers=headers, data=data)

if response.status_code == 200:
    token = response.json().get("access_token")
    print(f"Token de acesso: {token}")
else:
    print(f"Erro na autenticação. Status Code: {response.status_code}")
    print(response.json())
