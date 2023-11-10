# Script de Descarga y Carga de Datos de Taxis Amarillos y Verdes a Google Cloud Storage

# Este script se encarga de descargar archivos Parquet de datos de taxis amarillos y verdes desde una URL
# y cargarlos en un depósito de Google Cloud Storage (GCS) con una estructura de directorios específica.

# Importar las bibliotecas necesarias.
import requests
from google.cloud import storage

# Función para descargar un archivo desde una URL.
def download_file(url):
    """
    Descarga un archivo desde una URL y devuelve su contenido.

    Args:
        url (str): La URL desde la cual se descargará el archivo.

    Returns:
        bytes: El contenido del archivo descargado en formato bytes.

    Raises:
        requests.exceptions.RequestException: En caso de un error al realizar la solicitud.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        print(f"Error al descargar el archivo: {url} - {e}")
        return None

# Función para subir un archivo a Google Cloud Storage (GCS).
def upload_to_gcs(bucket, object_name, file_content):
    """
    Sube un archivo a un cubo de Google Cloud Storage (GCS).

    Args:
        bucket (str): El nombre del cubo de GCS.
        object_name (str): El nombre del objeto dentro del cubo.
        file_content (bytes): El contenido del archivo a cargar en formato bytes.

    Raises:
        Exception: En caso de un error al subir el archivo a GCS.
    """
    try:
        client = storage.Client()
        bucket = client.bucket(bucket)
        blob = bucket.blob(object_name)
        blob.upload_from_string(file_content)
        print(f"Archivo subido a GCS: {object_name}")
    except Exception as e:
        print(f"Error al subir el archivo a GCS: {object_name} - {e}")

# Función para listar objetos en un cubo de GCS con un prefijo específico.
def list_gcs_objects(bucket, prefix):
    """
    Lista los objetos en un cubo de Google Cloud Storage (GCS) con un prefijo específico.

    Args:
        bucket (str): El nombre del cubo de GCS.
        prefix (str): El prefijo para filtrar objetos dentro del cubo.

    Returns:
        list: Una lista de nombres de objetos que coinciden con el prefijo.
    """
    client = storage.Client()
    bucket = client.bucket(bucket)
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs]

# Función principal que realiza el flujo principal del script.
def main_flow():
    """
    Realiza el flujo principal del script, descargando y cargando archivos Parquet de taxis amarillos y verdes
    en un depósito de Google Cloud Storage (GCS) con una estructura de directorios específica.
    """
    # URL base común para los archivos Parquet de Yellow y Green Taxis.
    base_url_yellow = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
    base_url_green = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_"

    # Nombre del cubo de Google Cloud Storage.
    bucket_name = "pf_data_lake_proyecto-final-henry-403600"
    
    # Años y meses que deseas descargar.
    years = ["2021","2022","2023"]
    months = [str(i).zfill(2) for i in range(1, 13)]

    for year in years:
        for month in months:
            yellow_parquet_url = f"{base_url_yellow}{year}-{month}.parquet"
            yellow_object_name = f"yellow/{year}/{year}-{month}.parquet"
            
            yellow_file_content = download_file(yellow_parquet_url)
            if yellow_file_content is not None:
                upload_to_gcs(bucket_name, yellow_object_name, yellow_file_content)
                print(f"Subiendo archivo amarillo: {yellow_object_name}")
            else:
                print(f"El mes {year}-{month} no está disponible para amarillo. Terminando.")

            green_parquet_url = f"{base_url_green}{year}-{month}.parquet"
            green_object_name = f"green/{year}/{year}-{month}.parquet"
            
            green_file_content = download_file(green_parquet_url)
            if green_file_content is not None:
                upload_to_gcs(bucket_name, green_object_name, green_file_content)
                print(f"Subiendo archivo verde: {green_object_name}")
            else:
                print(f"El mes {year}-{month} no está disponible para verde. Terminando.")



if __name__ == "__main__":
    main_flow()