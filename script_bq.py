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
    API_URL = "https://jsonplaceholder.typicode.com/posts"
    
    # 1. Descarga de datos
    downloader = APIDownloader(API_URL)
    df_datos = downloader.fetch_data()
    
    # 2. Carga en el Sandbox original (Paso 1 y 2 del enunciado)
    uploader_sandbox = BigQueryUploader("SANDBOX_crypto_api", "raw_api_data", location="europe-west1")
    uploader_sandbox.upload_dataframe(df_datos)
    
    # 3. Transformación e Ingesta en la tabla final de INTEGRATION (Apartado 3)
    # Aplicamos la transformación (títulos en mayúsculas) directamente en el dataframe
    df_datos['title'] = df_datos['title'].str.upper()
    
    # Subimos los datos a la tabla final. Usamos WRITE_TRUNCATE para garantizar la IDEMPOTENCIA
    # (si el script se ejecuta 2 veces, se sobrescribe limpiamente sin duplicar filas)
    uploader_integration = BigQueryUploader("INTEGRATION", "integration_prueba_tecnica", location="europe-west1")
    uploader_integration.upload_dataframe(df_datos)