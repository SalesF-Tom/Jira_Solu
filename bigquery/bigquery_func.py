import os
import time
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

def Get_BQ_service():
    credentials_path = './credenciales/data-warehouse-311917-73a0792225c7.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    os.environ['GOOGLE_CLOUD_DISABLE_GRPC'] = 'True'
    project_id = os.getenv("PROJECT_ID")
    
    clientBQ = bigquery.Client(project=project_id)

    return clientBQ


def Insertar_Datos_BQ(logger, client, schema, nombre_tabla, df_panda, tipo, metodo="WRITE_TRUNCATE"):
    if tipo == "temp":
        dataset_id = os.getenv("DATASET_TEMP")
        prefijo = "_temp"
        nombre_tablaTemp = nombre_tabla + prefijo
        logger.info(f"Se cargaron en el DataSet : {(os.getenv("DATASET_TEMP"))} a la tabla {nombre_tablaTemp}.")

    elif tipo == "final":
        dataset_id = os.getenv("DATASET_FINAL")
        prefijo = ""
        nombre_tablaTemp = nombre_tabla + prefijo
    else:
        print("Tipo de dataset no válido.")
        return

    filas_cargadas = Cargar_CSV_a_BigQuery(logger, client, dataset_id, nombre_tablaTemp , schema, df_panda, tipo,  metodo=metodo)


def Cargar_CSV_a_BigQuery(logger, client, dataset_id, table_id, schema, df_panda, tipo, metodo):
     # Forzar pandas a leer las columnas problemáticas como string
    dtype = {
        'date_': 'datetime'
    }
          
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    # Configuración del job para BigQuery
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE if metodo == "WRITE_TRUNCATE" else bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False
    )
    # Cargar el DataFrame en BigQuery
    job = client.load_table_from_dataframe(df_panda, table_ref, job_config=job_config)
    job.result()

    # EXPIRES PARA EL DATASET INTERMEDIO
    if tipo == "temp":
        time.sleep(2)
        table = client.get_table(table_ref)
        expiration_time = datetime.now(timezone.utc) + timedelta(minutes=15)
        table.expires = expiration_time
        client.update_table(table, ["expires"])
    
    logger.info(f"Se cargaron {len(df_panda)} registros en el DataSet {dataset_id} en la tabla {table_id}.")

    return len(df_panda)



def Insertar_Datos_BQ_primeravez(client, schema, nombre_tabla, df_panda, tipo, metodo="WRITE_TRUNCATE"):
       dataset_id = os.getenv("DATASET_FINAL")
       Cargar_CSV_a_BigQuery(client, dataset_id, nombre_tabla, schema, df_panda, tipo,  metodo=metodo)
