import requests
import os
from dotenv import load_dotenv
import json
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()

SPOTIFY_API_KEY= os.getenv("SPOTIFY_API_KEY")    
ADLS_ACCOUNT_NAME= os.getenv("ADLS_ACCOUNT_NAME") 
ADLS_ACCOUNT_KEY= os.getenv("ADLS_ACCOUNT_KEY") 
DATA_LAKE_FILE_SYSTEM= os.getenv("DATA_LAKE_FILE_SYSTEM") 
DATA_LAKE_OUTPUT_PATH_TRACK_IDS = os.getenv("DATA_LAKE_OUTPUT_PATH_TRACK_IDS") 

def get_new_release_ids():
    url = "https://api.spotify.com/v1/browse/new-releases"
    headers = {
        "Authorization": f"Bearer {SPOTIFY_API_KEY}"
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        album_ids = [album['id'] for album in data['albums']['items']]
        return album_ids
    else:
        print(f"Erro ao buscar novos lançamentos. Status Code: {response.status_code}")
        return None

def get_tracks_from_album(album_id):
    url = f"https://api.spotify.com/v1/albums/{album_id}"
    headers = {
        "Authorization": f"Bearer {SPOTIFY_API_KEY}"
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        track_ids = [track['id'] for track in data['tracks']['items']]
        return track_ids
    else:
        print(f"Erro ao buscar faixas do álbum {album_id}. Status Code: {response.status_code}")
        return None

def get_new_release_track_ids():
    album_ids = get_new_release_ids()
    if not album_ids:
        return []

    all_track_ids = []
    for album_id in album_ids:
        track_ids = get_tracks_from_album(album_id)
        if track_ids:
            all_track_ids.extend(track_ids)
    
    return all_track_ids

def read_existing_track_ids():
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
        track_ids_str = download.readall().decode('utf-8')

        existing_track_ids = json.loads(track_ids_str)
        return existing_track_ids

    except Exception as e:
        print(f"Erro ao ler track_ids do ADLS: {str(e)}")
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

def store_track_ids(track_ids):

    existing_track_ids = read_existing_track_ids()

    existing_track_ids.extend(track_ids)

    # Remove duplicados 
    unique_track_ids = list(dict.fromkeys(existing_track_ids))

    track_ids_json = json.dumps(unique_track_ids, indent=2)

    upload_to_adls(track_ids_json, DATA_LAKE_OUTPUT_PATH_TRACK_IDS)

if __name__ == "__main__":
    track_ids = get_new_release_track_ids()
    if track_ids:
        store_track_ids(track_ids)
    else:
        print("Nenhum novo ID encontrado.")

