import os
import time
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------
# Cliente BQ
# ---------------------------------
def get_bq_client() -> bigquery.Client:
    project_id = os.getenv("PROJECT_ID")
    location = os.getenv("BQ_LOCATION") or None
    return bigquery.Client(project=project_id, location=location)

def Get_BQ_service():
    # Si usas credenciales de servicio locales:
    credentials_path = "./credenciales/data-warehouse-311917-73a0792225c7.json"
    if os.path.exists(credentials_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    # Evitar gRPC si te da problemas en local
    os.environ.setdefault("GOOGLE_CLOUD_DISABLE_GRPC", "True")

    project_id = os.getenv("PROJECT_ID")
    return bigquery.Client(project=project_id)

# ---------------------------------
# Carga genérica (DataFrame → BQ)
# ---------------------------------
def Insertar_Datos_BQ(
    logger,
    client: bigquery.Client,
    schema: list,
    nombre_tabla: str,
    df_panda,
    tipo: str,
    metodo: str = "WRITE_TRUNCATE",
):
    """
    Carga un DataFrame en:
      - dataset TEMP con sufijo *_temp si tipo == 'temp'
      - dataset FINAL si tipo == 'final'

    nombre_tabla: sólo el nombre de la tabla (sin dataset).
    """
    if tipo == "temp":
        dataset_id = os.getenv("DATASET_TEMP")
        table_id = f"{nombre_tabla}_temp"
        logger.info(
            f"Se cargaron en el DataSet: {os.getenv('DATASET_TEMP')} a la tabla {table_id}."
        )
    elif tipo == "final":
        dataset_id = os.getenv("DATASET_FINAL")
        table_id = nombre_tabla
    else:
        raise ValueError("Tipo de dataset no válido. Usa 'temp' o 'final'.")

    filas_cargadas = Cargar_CSV_a_BigQuery(
        logger=logger,
        client=client,
        dataset_id=dataset_id,
        table_id=table_id,
        schema=schema,
        df_panda=df_panda,
        tipo=tipo,
        metodo=metodo,
    )
    return filas_cargadas


def Cargar_CSV_a_BigQuery(
    logger,
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    schema: list,
    df_panda,
    tipo: str,
    metodo: str,
):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    write_disp = (
        bigquery.WriteDisposition.WRITE_TRUNCATE
        if metodo == "WRITE_TRUNCATE"
        else bigquery.WriteDisposition.WRITE_APPEND
    )

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_disp,
        autodetect=False,
    )

    job = client.load_table_from_dataframe(df_panda, table_ref, job_config=job_config)
    job.result()  # Esperar a que termine

    # Expiración para tablas de staging
    if tipo == "temp":
        time.sleep(1)
        table = client.get_table(table_ref)
        expiration_time = datetime.now(timezone.utc) + timedelta(minutes=15)
        table.expires = expiration_time
        client.update_table(table, ["expires"])

    logger.info(
        f"Se cargaron {len(df_panda)} registros en {dataset_id}.{table_id}"
    )
    return len(df_panda)


def Insertar_Datos_BQ_primeravez(
    logger,
    client: bigquery.Client,
    schema: list,
    nombre_tabla: str,
    df_panda,
    metodo: str = "WRITE_TRUNCATE",
):
    """
    Útil para una carga inicial directa a dataset final.
    """
    dataset_id = os.getenv("DATASET_FINAL")
    return Cargar_CSV_a_BigQuery(
        logger=logger,
        client=client,
        dataset_id=dataset_id,
        table_id=nombre_tabla,
        schema=schema,
        df_panda=df_panda,
        tipo="final",
        metodo=metodo,
    )