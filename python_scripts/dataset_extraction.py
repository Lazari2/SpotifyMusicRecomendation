import requests
import os
from dotenv import load_dotenv
import json
from azure.storage.filedatalake import DataLakeServiceClient
import random

load_dotenv()

SPOTIFY_API_KEY = os.getenv("SPOTIFY_API_KEY")  
ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME") 
ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY") 
DATA_LAKE_FILE_SYSTEM = os.getenv("DATA_LAKE_FILE_SYSTEM") 
DATA_LAKE_OUTPUT_PATH_TRACK_IDS = os.getenv("DATA_LAKE_OUTPUT_PATH_TRACK_IDS")

def search_tracks(query, limit=50, offset=0):
    url = "https://api.spotify.com/v1/search"
    headers = {
        "Authorization": f"Bearer {SPOTIFY_API_KEY}"
    }
    params = {
        "q": query,
        "type": "track",
        "limit": limit,
        "offset": offset  
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()

        track_info = [
            {
                'track_id': track['id'],
                'track_name': track['name'],
                'artist_name': track['artists'][0]['name']
            }
            for track in data['tracks']['items']
        ]
        return track_info
    else:
        print(f"Erro ao buscar faixas com o termo '{query}'. Status Code: {response.status_code}")
        return None

def read_existing_track_info():
    try:
        service_client = DataLakeServiceClient(
            account_url=f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net",
            credential=ADLS_ACCOUNT_KEY
        )
        
        file_system_client = service_client.get_file_system_client(file_system=DATA_LAKE_FILE_SYSTEM)

        file_client = file_system_client.get_file_client(DATA_LAKE_OUTPUT_PATH_TRACK_IDS)

        if not file_client.exists():
            print(f"Arquivo {DATA_LAKE_OUTPUT_PATH_TRACK_IDS} não encontrado no ADLS. Criando um novo.")
            return []

        download = file_client.download_file()
        track_info_str = download.readall().decode('utf-8')

        existing_track_info = json.loads(track_info_str)
        return existing_track_info

    except Exception as e:
        print(f"Erro ao ler dados das faixas do ADLS: {str(e)}")
        return []

def upload_to_adls(file_content, file_name):
    try:
        service_client = DataLakeServiceClient(
            account_url=f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net",
            credential=ADLS_ACCOUNT_KEY
        )

        file_system_client = service_client.get_file_system_client(file_system=DATA_LAKE_FILE_SYSTEM)

        file_client = file_system_client.get_file_client(file_name)

        file_client.upload_data(file_content, overwrite=True)
        print(f"Arquivo {file_name} enviado para o Azure Data Lake com sucesso!")

    except Exception as e:
        print(f"Erro ao enviar dados para o ADLS: {str(e)}")

def store_track_info(track_info):
    existing_track_info = read_existing_track_info()

    existing_track_info.extend(track_info)

    unique_track_info = {track['track_id']: track for track in existing_track_info}.values()

    track_info_json = json.dumps(list(unique_track_info), indent=2)

    upload_to_adls(track_info_json, DATA_LAKE_OUTPUT_PATH_TRACK_IDS)

def get_random_search_query():
    queries = ["rock", "pop", "jazz", "hip hop", "classical", "blues", "edm", "metal", "indie", "funk", "disco"]

#Generos extraidos do endpoint de genres da api do spotify, mas muitos nao funcionam, testar os que funcionam dpois....

    # "genres": ["acoustic", "afrobeat", "alt-rock", "alternative", "ambient", "anime", "black-metal", "bluegrass", 
    #            "blues", "bossanova", "brazil", "breakbeat", "british", "cantopop", "chicago-house", "children", 
    #            "chill", "classical", "club", "comedy", "country", "dance", "dancehall", "death-metal", "deep-house", 
    #            "detroit-techno", "disco", "disney", "drum-and-bass", "dub", "dubstep", "edm", "electro", "electronic", 
    #            "emo", "folk", "forro", "french", "funk", "garage", "german", "gospel", "goth", "grindcore", "groove", 
    #            "grunge", "guitar", "happy", "hard-rock", "hardcore", "hardstyle", "heavy-metal", "hip-hop", "holidays", 
    #            "honky-tonk", "house", "idm", "indian", "indie", "indie-pop", "industrial", "iranian", "j-dance", "j-idol", 
    #            "j-pop", "j-rock", "jazz", "k-pop", "kids", "latin", "latino", "malay", "mandopop", "metal", "metal-misc", 
    #            "metalcore", "minimal-techno", "movies", "mpb", "new-age", "new-release", "opera", "pagode", "party", 
    #            "philippines-opm", "piano", "pop", "pop-film", "post-dubstep", "power-pop", "progressive-house", "psych-rock", 
    #            "punk", "punk-rock", "r-n-b", "rainy-day", "reggae", "reggaeton", "road-trip", "rock", "rock-n-roll", 
    #            "rockabilly", "romance", "sad", "salsa", "samba", "sertanejo", "show-tunes", "singer-songwriter", "ska", 
    #            "sleep", "songwriter", "soul", "soundtracks", "spanish", "study", "summer", "swedish", "synth-pop", "tango", 
    #            "techno", "trance", "trip-hop", "turkish", "work-out", "world-music"]
    
    return random.choice(queries)

if __name__ == "__main__":

    search_query = get_random_search_query()
    
    offset = random.randint(0, 10000)

    track_info = search_tracks(search_query, limit=50, offset=offset)

    if track_info:
        store_track_info(track_info)

        print(json.dumps(track_info, indent=2))
    else:
        print("Nenhuma faixa encontrada para o termo de busca.")