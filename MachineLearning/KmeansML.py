from sklearn.preprocessing import StandardScaler
from dotenv import load_dotenv
from sklearn.metrics.pairwise import euclidean_distances
from sklearn.cluster import KMeans
from azure.storage.filedatalake import DataLakeServiceClient
import os
from io import BytesIO
import pandas as pd

load_dotenv()

DATA_LAKE_OUTPUT_PATH = os.getenv("DATA_LAKE_OUTPUT_PATH")
ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME") 
ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY") 
DATA_LAKE_FILE_SYSTEM = os.getenv("DATA_LAKE_FILE_SYSTEM")


service_client = DataLakeServiceClient(
    account_url=f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net",
    credential=ADLS_ACCOUNT_KEY
)

file_system_client = service_client.get_file_system_client(file_system=DATA_LAKE_FILE_SYSTEM)
file_client = file_system_client.get_file_client(DATA_LAKE_OUTPUT_PATH)

download = file_client.download_file()
downloaded_bytes = download.readall()  

data = pd.read_parquet(BytesIO(downloaded_bytes))

scaler = StandardScaler()
data_scaled = scaler.fit_transform(data[['danceability', 'energy', 'key', 'loudness', 'mode',
                                         'speechiness', 'acousticness', 'instrumentalness',
                                         'liveness', 'valence', 'tempo', 'duration_ms', 'time_signature']])

kmeans = KMeans(n_clusters=10, random_state=42) 
kmeans.fit(data_scaled)

data['cluster'] = kmeans.labels_

def recomendar_musicas(data, id_musica, num_recomendacoes=5):
    musica_base = data_scaled[id_musica].reshape(1, -1)  
    distancias = euclidean_distances(musica_base, data_scaled).flatten()
    indices_recomendadas = distancias.argsort()[1:num_recomendacoes+1]
    
    return data.iloc[indices_recomendadas]

#usando a música de ID 0
recomendacoes = recomendar_musicas(data, 0, num_recomendacoes=10)
print(recomendacoes)