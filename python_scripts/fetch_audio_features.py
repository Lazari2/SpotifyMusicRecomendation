import os
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv
import requests
import json
import time 

load_dotenv()

SPOTIFY_API_KEY = os.getenv("SPOTIFY_API_KEY")
ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")
DATA_LAKE_FILE_SYSTEM = os.getenv("DATA_LAKE_FILE_SYSTEM")
DATA_LAKE_OUTPUT_PATH_TRACK_IDS = os.getenv("DATA_LAKE_OUTPUT_PATH_TRACK_IDS") 
DATA_LAKE_OUTPUT_PATH= os.getenv("DATA_LAKE_OUTPUT_PATH")

def get_audio_features(track_id):
    url = f"https://api.spotify.com/v1/audio-features/{track_id}"
    headers = {
        "Authorization": f"Bearer {SPOTIFY_API_KEY}"
    }
    
    max_retries = 5  
    retries = 0

    while retries < max_retries:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))  
            print(f"Rate limit excedido. Aguardando {retry_after} segundos...")
            time.sleep(retry_after)
            retries += 1
        else:
            print(f"Erro ao buscar audio-features para {track_id}. Status Code: {response.status_code}")
            return None

    print(f"Máximo de tentativas excedido para {track_id}.")
    return None

def read_existing_parquet():
    try:
        service_client = DataLakeServiceClient(
            account_url=f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net",
            credential=ADLS_ACCOUNT_KEY
        )

        file_system_client = service_client.get_file_system_client(file_system=DATA_LAKE_FILE_SYSTEM)
        file_client = file_system_client.get_file_client(DATA_LAKE_OUTPUT_PATH)

        download = file_client.download_file()
        with open('temp_parquet.parquet', 'wb') as local_file:
            local_file.write(download.readall())
        
        df_existing = pd.read_parquet('temp_parquet.parquet')
        print("Arquivo Parquet existente carregado.")
        return df_existing

    except Exception as e:
        print(f"Nenhum arquivo Parquet existente encontrado. Criando novo arquivo.")
        return pd.DataFrame()

def upload_to_adls(file_content, file_name):
    try:
        service_client = DataLakeServiceClient(
            account_url=f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net",
            credential=ADLS_ACCOUNT_KEY
        )

        file_system_client = service_client.get_file_system_client(file_system=DATA_LAKE_FILE_SYSTEM)
        file_client = file_system_client.get_file_client(file_name)

        file_client.upload_data(file_content, overwrite=True)
        print(f"Dados salvos no Azure Data Lake em {file_name} com sucesso!")

    except Exception as e:
        print(f"Erro ao salvar dados no ADLS: {str(e)}")

def read_track_info_json():
    try:
        service_client = DataLakeServiceClient(
            account_url=f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net",
            credential=ADLS_ACCOUNT_KEY
        )

        file_system_client = service_client.get_file_system_client(file_system=DATA_LAKE_FILE_SYSTEM)
        file_client = file_system_client.get_file_client(DATA_LAKE_OUTPUT_PATH_TRACK_IDS)

        download = file_client.download_file()
        track_info_str = download.readall().decode('utf-8')
        track_info = json.loads(track_info_str)
        
        print(f"Arquivo JSON com informações de músicas carregado.")
        return track_info

    except Exception as e:
        print(f"Erro ao ler o arquivo JSON {DATA_LAKE_OUTPUT_PATH_TRACK_IDS}: {str(e)}")
        return []

def append_audio_features_to_parquet(track_info):
    df_existing = read_existing_parquet()

    new_data = []
    for track in track_info:
        track_id = track['track_id']
        features = get_audio_features(track_id)
        if features:
            features['track_id'] = track['track_id']
            features['track_name'] = track['track_name']
            features['artist_name'] = track['artist_name']
            new_data.append(features)

    if new_data:
        df_new = pd.DataFrame(new_data)

        columns_to_drop = ['type', 'id', 'uri', 'track_href', 'analysis_url']
        df_new = df_new.drop(columns=columns_to_drop, errors='ignore')

        if not df_existing.empty:
            df_final = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_final = df_new

        temp_file = 'temp_audio_features.parquet'
        df_final.to_parquet(temp_file, index=False, engine='pyarrow')

        with open(temp_file, 'rb') as file_data:
            upload_to_adls(file_data.read(), DATA_LAKE_OUTPUT_PATH)
    else:
        print("Nenhum dado novo para adicionar.")

if __name__ == "__main__":
    track_info = read_track_info_json()

    if track_info:
        append_audio_features_to_parquet(track_info)
    else:
        print("Nenhum ID de track encontrado.")
