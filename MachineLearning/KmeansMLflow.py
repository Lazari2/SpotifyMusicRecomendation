from sklearn.preprocessing import StandardScaler
from dotenv import load_dotenv
from sklearn.cluster import KMeans
from azure.storage.filedatalake import DataLakeServiceClient
import os
from io import BytesIO
import pandas as pd
import mlflow
import mlflow.sklearn

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

# Configura o MLflow
mlflow.set_tracking_uri("file:///C:/SpotifyRecomendation/MachineLearning/model")
mlflow.set_experiment("Spotify_Music_Recomendation")

with mlflow.start_run():

    scaler = StandardScaler()
    data_scaled = scaler.fit_transform(data[['danceability', 'energy', 'key', 'loudness', 'mode',
                                             'speechiness', 'acousticness', 'instrumentalness',
                                             'liveness', 'valence', 'tempo', 'duration_ms', 'time_signature']])
    kmeans = KMeans(n_clusters=10, random_state=42)
    kmeans.fit(data_scaled)

    mlflow.log_param("n_clusters", 10)
    mlflow.log_param("random_state", 42)
    mlflow.log_metric("inertia", kmeans.inertia_)

    mlflow.sklearn.log_model(kmeans, "kmeans_model")
    mlflow.sklearn.log_model(scaler, "scaler")

data['cluster'] = kmeans.labels_

temp_file = 'clustered_data.parquet'
data.to_parquet(temp_file, index=False, engine='pyarrow')

with open(temp_file, 'rb') as file_data:
    file_client.upload_data(file_data.read(), overwrite=True)

print("Modelo KMeans registrado no MLflow e dados com clusters salvos no Azure Data Lake.")
