from google.cloud import storage
import gcsfs
import pandas as pd
from pandas_gbq import to_gbq
from google.cloud import bigquery
import random
import warnings
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time

# Número máximo de intentos para obtener datos
MAX_INTENTOS = 3

def get_fecha_mas_reciente():
    """
    Obtiene la fecha más reciente de la tabla en BigQuery.

    Returns:
    - fecha_mas_reciente (str): La fecha más reciente en formato "YYYY-MM-DD".
    """
    # Conectarse a BigQuery
    client = bigquery.Client()

    # Nombre del proyecto y dataset donde se encuentra la tabla
    project_id = "proyecto-final-henry-403600"
    dataset_id = "taxi_data_all"
    table_name = "tu_tabla"  # Reemplaza "tu_tabla" con el nombre de tu tabla

    # Consulta para obtener la fecha más reciente en la tabla
    query = f"SELECT MAX(fecha_abordaje) AS fecha_mas_reciente FROM `{project_id}.{dataset_id}.{table_name}`"

    # Ejecutar la consulta
    query_job = client.query(query)
    results = query_job.result()

    # Obtener la fecha más reciente
    fecha_mas_reciente = next(results).fecha_mas_reciente.strftime("%Y-%m")

    return fecha_mas_reciente


def obtener_siguiente_fecha(fecha_actual):
    """
    Obtiene la siguiente fecha a partir de la fecha actual.

    Parámetros:
    - fecha_actual (str): Fecha en formato "YYYY-MM".
    - next_month (bool, opcional): Si es True, avanza al siguiente mes. Si es False, avanza al siguiente año.

    Retorna:
    - str: La siguiente fecha en formato "YYYY-MM".
    """
    # Convertir la fecha actual a un objeto datetime
    fecha_actual_obj = datetime.strptime(fecha_actual, "%Y-%m")

    # Calcular el mes siguiente
    if fecha_actual_obj.month == 12:
        # Si el mes actual es diciembre, ajustar al siguiente año
        siguiente_fecha_obj = fecha_actual_obj + relativedelta(months=1, year=fecha_actual_obj.year + 1)
    else:
        siguiente_fecha_obj = fecha_actual_obj + relativedelta(months=1)

    # Formatear la fecha del mes siguiente en formato 'YYYY-MM'
    siguiente_fecha = siguiente_fecha_obj.strftime("%Y-%m")

    return siguiente_fecha


def cargar_datos_gcs(bucket_name, tipo_flota, year, month):
    """
    Carga datos desde Google Cloud Storage y devuelve un DataFrame de pandas.

    Parameters:
    - bucket_name (str): Nombre del cubo (bucket) en Google Cloud Storage.
    - tipo_flota (str): Tipo de flota (por ejemplo, "green" o "yellow").
    - year (str): Año de los datos.
    - month (str): Mes de los datos.

    Returns:
    - df (pd.DataFrame): DataFrame que contiene los datos cargados.
    """
    gcs = gcsfs.GCSFileSystem(project="proyecto-final-henry-403600")  # Replace "your-project-id" with your Google Cloud project ID
    ruta_archivo = f"{tipo_flota}/{year}/{year}-{month}.parquet"

    with gcs.open(f"{bucket_name}/{ruta_archivo}") as f:
        df = pd.read_parquet(f)

    return df


def cargar_archivo_complementario(bucket_name, filename):
    """
    Carga un archivo complementario desde Google Cloud Storage y devuelve un DataFrame de pandas.

    Parameters:
    - bucket_name (str): Nombre del cubo (bucket) en Google Cloud Storage.
    - filename (str): Nombre del archivo complementario.

    Returns:
    - df (pd.DataFrame): DataFrame que contiene los datos cargados desde el archivo complementario.
    """
    gcs = gcsfs.GCSFileSystem(project="proyecto-final-henry-403600")  # Reemplaza "tu-proyecto" por tu ID de proyecto de Google Cloud
    ruta_archivo = f"{bucket_name}/complementarios/{filename}"

    with gcs.open(ruta_archivo) as f:
        df = pd.read_csv(f)

    return df


def seleccionar_columnas_comunes(df, tipo_flota):
    """
    Selecciona y renombra las columnas comunes entre los conjuntos de datos de diferentes flotas.

    Parameters:
    - df (pd.DataFrame): DataFrame con los datos cargados.
    - tipo_flota (str): Tipo de flota (por ejemplo, "green" o "yellow").

    Returns:
    - df (pd.DataFrame): DataFrame con las columnas seleccionadas y renombradas.
    """
    # Columnas en común entre ambos conjuntos de datos
    columnas_comunes = {'total_amount', 'VendorID', 'improvement_surcharge', 'tolls_amount',
                        'PULocationID', 'DOLocationID', 'passenger_count', 'RatecodeID', 'mta_tax',
                        'trip_distance', 'payment_type', 'tip_amount', 'congestion_surcharge', 'store_and_fwd_flag',
                        'fare_amount', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'extra'}

    # Renombrar columnas si es necesario
    if tipo_flota == "yellow":
        df = df.rename(columns={"tpep_pickup_datetime": "lpep_pickup_datetime", "tpep_dropoff_datetime": "lpep_dropoff_datetime"})

    # Seleccionar solo las columnas en común
    columnas_seleccionadas = list(columnas_comunes.intersection(df.columns))

    df = df[columnas_seleccionadas]

    return df


def castear(df):
    """
    Realiza la conversión de las columnas de fecha y hora a formato de cadena (string).

    Parameters:
    - df (pd.DataFrame): DataFrame con los datos.

    Returns:
    - df (pd.DataFrame): DataFrame con las columnas de fecha y hora convertidas a formato de cadena.
    """
    df["lpep_pickup_datetime"] = df["lpep_pickup_datetime"].astype(str)
    df["lpep_dropoff_datetime"] = df["lpep_dropoff_datetime"].astype(str)


def limpiar_columnas(df):
    """
    Elimina columnas no deseadas del DataFrame.

    Parameters:
    - df (pd.DataFrame): DataFrame con los datos.

    Returns:
    - df (pd.DataFrame): DataFrame con las columnas no deseadas eliminadas.
    """
    columnas_a_borrar = ["RatecodeID", "passenger_count", "store_and_fwd_flag",  "mta_tax",
                         "tip_amount", "tolls_amount", "improvement_surcharge", "congestion_surcharge"]
    df.drop(columnas_a_borrar, axis=1, inplace=True)
    return df


def separar_fecha_hora(df):
    """
    Separa las columnas de fecha y hora de las columnas de abordaje y descenso.

    Parameters:
    - df (pd.DataFrame): DataFrame con los datos.

    Returns:
    - df (pd.DataFrame): DataFrame con columnas de fecha y hora separadas.
    """
    df[["fecha_abordaje", "hora_abordaje"]] = df["lpep_pickup_datetime"].str.extract(r"(\d{4}-\d{2}-\d{2})\s(\d{2}:\d{2}:\d{2})")
    df[["fecha_descenso", "hora_descenso"]] = df["lpep_dropoff_datetime"].str.extract(r"(\d{4}-\d{2}-\d{2})\s(\d{2}:\d{2}:\d{2})")
    return df


def cambiar_nombres_zonas(df, ruta_zonas):
    """
    Cambia los códigos de las zonas por los nombres de los municipios.

    Parameters:
    - df (pd.DataFrame): DataFrame con los datos.
    - ruta_zonas (pd.DataFrame): DataFrame con información de zonas y nombres de municipios.

    Returns:
    - df (pd.DataFrame): DataFrame con códigos de zonas reemplazados por nombres de municipios.
    """
    diccionario_zonas = ruta_zonas.set_index('LocationID')['Borough'].to_dict()
    
    #Cambio del código por el nombre del municipio
    df["lugar_abordaje"] = df["PULocationID"].map(diccionario_zonas)
    df["lugar_descenso"] = df["DOLocationID"].map(diccionario_zonas)
    
    df.drop(["lpep_pickup_datetime", "lpep_dropoff_datetime", "PULocationID", "DOLocationID"], axis=1, inplace=True)
    
    return df


def renombrar_columnas(df):
    """
    Renombra las columnas del DataFrame.

    Parameters:
    - df (pd.DataFrame): DataFrame con los datos.

    Returns:
    - df (pd.DataFrame): DataFrame con columnas renombradas.
    """
    df.rename(columns={"VendorID": "IDproveedor_servicio",
                      "trip_distance": "distancia_viaje",
                      "payment_type": "tipo_pago",
                      "fare_amount": "tarifa",
                      "total_amount": "total"}, inplace=True)
    return df


def add_columna_tipo_flota(df, tipo_flota):
    """
    Agrega una columna 'tipo_flota' al DataFrame.

    Parameters:
    - df (pd.DataFrame): DataFrame con los datos.
    - tipo_flota (str): Tipo de flota (por ejemplo, "green" o "yellow").

    Returns:
    - df (pd.DataFrame): DataFrame con la columna 'tipo_flota' agregada.
    """
    df["tipo_flota"] = tipo_flota  # Agrega la columna tipo_flota
    return df


def add_calendario_id(df):
    """
    Agrega una columna 'calendario_id' al DataFrame.

    Parameters:
    - df (pd.DataFrame): DataFrame con los datos.

    Returns:
    - df (pd.DataFrame): DataFrame con la columna 'calendario_id' agregada.
    """
    df['calendario_id'] = df['fecha_abordaje'].str.replace('-', '')
    return df


def add_vehicle_id(df):
    """
    Agrega una columna 'vehicle_id' al DataFrame.

    Parameters:
    - df (pd.DataFrame): DataFrame con los datos.

    Returns:
    - df (pd.DataFrame): DataFrame con la columna 'vehicle_id' agregada.
    """
    df['vehicle_id'] = df['tipo_flota'].apply(lambda x: random.choice([5, 7, 11, 15, 16]) if x == 'green' else random.choice([1, 3, 5, 6, 8, 9, 10, 14, 17, 18, 19, 20, 21, 22, 23, 25, 26, 13]) if x == 'yellow' else None)
    df['vehicle_id'] = df['vehicle_id'].astype(str)
    return df


def get_max_viaje_id():
    """
    Obtiene el valor máximo de la columna 'viaje_id' en BigQuery.

    Returns:
    - max_viaje_id (int): El valor máximo de la columna 'viaje_id'.
    """
    # Conectarse a BigQuery
    client = bigquery.Client()

    # Nombre del proyecto y dataset donde se encuentra la tabla
    project_id = "proyecto-final-henry-403600"
    dataset_id = "taxi_data_all"
    table_name = "viajes_taxi"  # Nombre de la tabla

    # Consulta para obtener el valor máximo de la columna 'viaje_id'
    query = f"SELECT MAX(viaje_id) AS max_viaje_id FROM `{project_id}.{dataset_id}.{table_name}`"

    # Ejecutar la consulta
    query_job = client.query(query)
    results = query_job.result()

    # Obtener el valor máximo
    max_viaje_id = next(results).max_viaje_id

    return max_viaje_id


def add_viaje_id(df):
    """
    Agrega una columna 'viaje_id' al DataFrame.

    Parameters:
    - df (pd.DataFrame): DataFrame con los datos.

    Returns:
    - df (pd.DataFrame): DataFrame con la columna 'viaje_id' agregada.
    """
    # Obtener el valor máximo actual de 'viaje_id' en BigQuery
    max_viaje_id = get_max_viaje_id()

    if max_viaje_id is None:
        max_viaje_id = 0

    # Convert max_viaje_id to a string
    max_viaje_id_str = str(max_viaje_id)

    # Asignar valores únicos incrementales comenzando desde el valor máximo + 1
    df['viaje_id'] = (df.index + int(max_viaje_id_str) + 1).astype(str)

    return df


def load_to_bigquery(df, project_id, dataset_id, table_name):
    """
    Carga datos en BigQuery.

    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a cargar en BigQuery.
    - project_id (str): ID del proyecto de Google Cloud.
    - dataset_id (str): ID del conjunto de datos en BigQuery.
    - table_name (str): Nombre de la tabla en BigQuery.
    """
    # Cargar el DataFrame en BigQuery
    to_gbq(df, f"{dataset_id}.{table_name}", project_id=project_id, if_exists="append")



def main():
    bucket_name = "pf_data_lake_proyecto-final-henry-403600"
    warnings.filterwarnings("ignore")
    intentos = 0
    
    while intentos < MAX_INTENTOS:
        # Obtener la fecha más reciente de BigQuery (mes y año)
        fecha_mas_reciente = get_fecha_mas_reciente()
        year, month = fecha_mas_reciente.year, fecha_mas_reciente.month

        for folder in ["green", "yellow"]:
            df = cargar_datos_gcs(bucket_name, folder, str(year), f"{month:02}")

            # Comprobar si la obtención del conjunto de datos fue exitosa
            if df is None:
                print(f"No se pudo obtener el conjunto de datos de {year}-{month:02} en la carpeta {folder}.")
                intentos += 1
                if intentos >= MAX_INTENTOS:
                    print("Se superó el número máximo de intentos. Saliendo del script.")
                    return  # Salir del script si se supera el número máximo de intentos
                print("Reintentando en unos minutos...")
                time.sleep(300)  # Esperar 5 minutos antes de reintentar
                break  # Saltar este mes y pasar al siguiente

            print(f"\nProcesando datos de {year}-{month:02} en la carpeta {folder}...")

            df_columnas_comunes = seleccionar_columnas_comunes(df, folder)
            df_casteado = castear(df_columnas_comunes)
            df_columnas_seleccionadas = limpiar_columnas(df_casteado)
            df_fecha_hora_separada = separar_fecha_hora(df_columnas_seleccionadas)

            df_complementario = cargar_archivo_complementario(bucket_name, "taxi+_zone_lookup.csv")
            df_zonas = cambiar_nombres_zonas(df_fecha_hora_separada, df_complementario)  # Asegúrate de definir ruta_zonas

            df_columnas_renombradas = renombrar_columnas(df_zonas)
            df_columna_tipo = add_columna_tipo_flota(df_columnas_renombradas, folder)
            df_columna_calendario = add_calendario_id(df_columna_tipo)
            df_columna_vehicle = add_vehicle_id(df_columna_calendario)
            df_final = add_viaje_id(df_columna_vehicle)

            print("Transformación de datos completada.")

            print(f"\nIniciando la carga de datos de {year}-{month:02} en BigQuery...")
            load_to_bigquery(df_final, "proyecto-final-henry-403600", "proyecto-final-henry-403600.taxi_data_all", "viajes_taxi")
            print(f"Carga de datos de {year}-{month:02} en BigQuery completada.")

        # Avanzar al mes siguiente (o siguiente año si es diciembre)
        fecha_mas_reciente = obtener_siguiente_fecha(fecha_mas_reciente, next_month=True)

if __name__ == "__main__":
    main()