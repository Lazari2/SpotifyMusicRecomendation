import os
import requests
from dotenv import load_dotenv

load_dotenv()

SPOTIFY_API_KEY = os.getenv("SPOTIFY_API_KEY")

def get_single_audio_feature(track_id):
    url = f"https://api.spotify.com/v1/audio-features/{track_id}"
    headers = {
        "Authorization": f"Bearer {SPOTIFY_API_KEY}"
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        print("Requisição bem-sucedida!")
        print(response.json())
    else:
        print(f"Erro ao buscar audio-features. Status Code: {response.status_code}")
        if response.status_code == 429:
            print("Limite de taxa excedido.")

if __name__ == "__main__":
    track_id = "4MF0xXhjJxUVNtCVar0nHv"
    get_single_audio_feature(track_id)


    #CONTINUA DANDO PROBLEMA DE LIMITE DE REQUISIÇÃO (429) (SPOTIFY? ; CONTA?)