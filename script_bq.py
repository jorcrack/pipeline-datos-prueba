import os
import requests
import pandas as pd
from google.cloud import bigquery

# Configuramos la variable de entorno para la autenticación usando tu JSON local
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

class APIDownloader:
    """Clase encargada de conectarse a la API externa y descargar los datos en crudo."""
    def __init__(self, url: str):
        self.url = url

    def fetch_data(self) -> pd.DataFrame:
        print(f"Conectando a la API: {self.url}...")
        response = requests.get(self.url)
        
        if response.status_code == 200:
            # Obtenemos los registros y limitamos a los primeros 100 exigidos
            data = response.json()[:100]
            df = pd.DataFrame(data)
            print(f"Descarga exitosa. Se han obtenido {len(df)} registros.")
            return df
        else:
            raise Exception(f"Error al conectarse a la API. Código de estado: {response.status_code}")

class BigQueryUploader:
    """Clase encargada de gestionar la conexión y subida de datos a Google BigQuery."""
    def __init__(self, dataset_id: str, table_id: str, location: str = "europe-west1"):
        self.client = bigquery.Client()
        self.table_ref = f"{self.client.project}.{dataset_id}.{table_id}"
        self.location = location

    def upload_dataframe(self, df: pd.DataFrame):
        print(f"Iniciando la carga en BigQuery en la tabla {self.table_ref}...")
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )
        
        job = self.client.load_table_from_dataframe(
            df, self.table_ref, job_config=job_config, location=self.location
        )
        job.result()  
        print(f"Éxito absoluto: Datos cargados en BigQuery en la región {self.location}.")

if __name__ == "__main__":
    # Usamos una API pública y estable de prueba
    API_URL = "https://jsonplaceholder.typicode.com/posts"
    
    DATASET_NAME = "SANDBOX_crypto_api"
    TABLE_NAME = "raw_api_data"

    try:
        # 1. Instanciar el descargador y extraer la información de la API
        downloader = APIDownloader(API_URL)
        df_datos = downloader.fetch_data()

        # 2. Instanciar el cargador y subir la información a Google
        uploader = BigQueryUploader(DATASET_NAME, TABLE_NAME, location="europe-west1")
        uploader.upload_dataframe(df_datos)
        
    except Exception as e:
        print(f"Ocurrió un error en el pipeline: {e}")