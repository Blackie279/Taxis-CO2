from datetime import datetime, timedelta
import requests
from google.cloud import storage


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


def list_gcs_objects_with_date(bucket, prefix):
    """
    Lista los objetos en un cubo de Google Cloud Storage (GCS) con un prefijo específico y devuelve la fecha más reciente.

    Args:
        bucket (str): El nombre del cubo de GCS.
        prefix (str): El prefijo para filtrar objetos dentro del cubo.

    Returns:
        tuple: Una tupla con la lista de nombres de objetos y la fecha más reciente (en formato 'YYYY-MM').
    """
    client = storage.Client()
    bucket = client.bucket(bucket)
    blobs = bucket.list_blobs(prefix=prefix)
    
    # Obtener la lista de nombres de objetos y extraer las fechas de los nombres
    object_names = [blob.name for blob in blobs]
    object_dates = [name.split("/")[1] for name in object_names]
    
    # Devolver la fecha más reciente
    latest_date = max(object_dates) if object_dates else None
    
    return object_names, latest_date

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
    
    # Obtener las listas de nombres de objetos y fechas más recientes para yellow y green
    yellow_objects, latest_yellow_date = list_gcs_objects_with_date(bucket_name, "yellow/")
    green_objects, latest_green_date = list_gcs_objects_with_date(bucket_name, "green/")
    
    # Calcular la fecha más reciente de ambos conjuntos de datos
    latest_yellow_date = datetime.strptime(latest_yellow_date or "202101", "%Y%m")
    latest_green_date = datetime.strptime(latest_green_date or "202101", "%Y%m")
    latest_date = max(latest_yellow_date, latest_green_date)

    # Calcular la fecha para el siguiente mes al último dataset cargado
    next_month_date = latest_date + timedelta(days=32)
    next_month = next_month_date.strftime("%Y-%m")

    # Construir el nombre de objetos para el siguiente mes
    yellow_object_name_next_month = f"yellow/{next_month.replace('-', '/')}.parquet"
    green_object_name_next_month = f"green/{next_month[:4]}/{next_month}.parquet"

    # Descargar y cargar el siguiente mes si no existe en el depósito de GCS
    if yellow_object_name_next_month not in yellow_objects or green_object_name_next_month not in green_objects:
        yellow_parquet_url = f"{base_url_yellow}{next_month}.parquet"
        green_parquet_url = f"{base_url_green}{next_month}.parquet"

        yellow_file_content = download_file(yellow_parquet_url)
        green_file_content = download_file(green_parquet_url)

        if yellow_file_content is not None:
            upload_to_gcs(bucket_name, yellow_object_name_next_month, yellow_file_content)
            print(f"Subiendo archivo amarillo: {yellow_object_name_next_month}")
        else:
            print(f"El mes {next_month} no está disponible para amarillo. Terminando.")

        if green_file_content is not None:
            upload_to_gcs(bucket_name, green_object_name_next_month, green_file_content)
            print(f"Subiendo archivo verde: {green_object_name_next_month}")
        else:
            print(f"El mes {next_month} no está disponible para verde. Terminando.")
    else:
        print(f"Los archivos para {next_month} ya existen en el depósito de GCS. No es necesario cargarlos.")

if __name__ == "__main__":
    main_flow()
