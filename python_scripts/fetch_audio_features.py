import os
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv
import requests
from dataset_extraction import read_existing_track_ids

load_dotenv()

SPOTIFY_API_KEY = os.getenv("SPOTIFY_API_KEY")
ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")
DATA_LAKE_FILE_SYSTEM = os.getenv("DATA_LAKE_FILE_SYSTEM")
DATA_LAKE_OUTPUT_PATH = os.getenv("DATA_LAKE_OUTPUT_PATH")


def get_audio_features(track_id):
    url = f"https://api.spotify.com/v1/audio-features/{track_id}"
    headers = {
        "Authorization": f"Bearer {SPOTIFY_API_KEY}"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao buscar audio-features para {track_id}. Status Code: {response.status_code}")
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


def append_audio_features_to_parquet(track_ids):
    df_existing = read_existing_parquet()

    new_data = []
    for track_id in track_ids:
        features = get_audio_features(track_id)
        if features:
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
    track_ids = read_existing_track_ids() 
    if track_ids:
        append_audio_features_to_parquet(track_ids)
    else:
        print("Nenhum ID de track encontrado.")
