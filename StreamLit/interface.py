import streamlit as st
import joblib
import pandas as pd
from sklearn.metrics.pairwise import euclidean_distances
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv
import os
from io import BytesIO
import pywhatkit as kit

load_dotenv()

ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME") 
ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY") 
DATA_LAKE_FILE_SYSTEM = os.getenv("DATA_LAKE_FILE_SYSTEM")
DATA_LAKE_OUTPUT_PATH = os.getenv("DATA_LAKE_OUTPUT_PATH")

def load_data_from_adls():
    service_client = DataLakeServiceClient(
        account_url=f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net",
        credential=ADLS_ACCOUNT_KEY
    )
    
    file_system_client = service_client.get_file_system_client(file_system=DATA_LAKE_FILE_SYSTEM)
    file_client = file_system_client.get_file_client(DATA_LAKE_OUTPUT_PATH)

    download = file_client.download_file()
    downloaded_bytes = download.readall()

    data = pd.read_parquet(BytesIO(downloaded_bytes))
    return data

data = load_data_from_adls()

with open('C:/SpotifyRecomendation/MachineLearning/model/kmeans_model2.pkl', 'rb') as model_file:
    kmeans_model = joblib.load(model_file)

with open('C:/SpotifyRecomendation/MachineLearning/model/scaler.pkl', 'rb') as scaler_file:
    scaler = joblib.load(scaler_file)

def recomendar_musicas(musica_selecionada, data, num_recomendacoes=10):
    data_scaled = scaler.transform(data[['danceability', 'energy', 'key', 'loudness', 'mode',
                                         'speechiness', 'acousticness', 'instrumentalness',
                                         'liveness', 'valence', 'tempo', 'duration_ms', 'time_signature']])

    musica_index = data[data['track_name'] == musica_selecionada].index[0]

    cluster = kmeans_model.predict(data_scaled[musica_index].reshape(1, -1))

    musicas_cluster = data[data['cluster'] == cluster[0]]

    musicas_recomendadas = musicas_cluster.sample(n=num_recomendacoes) if len(musicas_cluster) > num_recomendacoes else musicas_cluster

    return musicas_recomendadas

def abrir_no_youtube(track_name, artist_name):
    query = f"{track_name} - {artist_name}"
    kit.playonyt(query)

st.title("Recomendador de Músicas")

musicas_artistas = data.apply(lambda row: f"{row['track_name']} - {row['artist_name']}", axis=1)

musica_selecionada = st.selectbox('Selecione uma música', musicas_artistas)

if st.button('Recomendar músicas semelhantes'):

    track_name = musica_selecionada.split(' - ')[0]  
    recomendacoes = recomendar_musicas(track_name, data)  

    recomendacoes = recomendacoes[['track_name', 'artist_name']].rename(columns={
        'track_name': 'Song Name', 
        'artist_name': 'Artist'
    })
    
    st.write("Recomendações de músicas:")

    recomendacoes = recomendacoes.reset_index(drop=True)
    recomendacoes.index = recomendacoes.index + 1
    
    st.dataframe(recomendacoes)

    primeira_musica = recomendacoes.iloc[0]
    abrir_no_youtube(primeira_musica['Song Name'], primeira_musica['Artist'])