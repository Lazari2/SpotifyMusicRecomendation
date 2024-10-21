import requests
import base64
from dotenv import load_dotenv
import os
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')

def update_env_file(key, value):
    with open(env_path, 'r') as file:
        lines = file.readlines()

    with open(env_path, 'w') as file:
        for line in lines:
            if line.startswith(key):
                file.write(f"{key}={value}\n")
            else:
                file.write(line)

load_dotenv(env_path)

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

    update_env_file("SPOTIFY_API_KEY", f"'{token}'")
    
else:
    print(f"Erro na autenticação. Status Code: {response.status_code}")
    print(response.json())
