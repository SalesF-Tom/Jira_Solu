import os
from datetime import datetime
import pytz
import time
import schedule
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

# Importar cliente de BigQuery
from bigquery.bigquery_func import Get_BQ_service

# Importar esquemas y funciones de merge
from schema.schemas import Esquema
from bigquery.querys import (
    Merge_Data_Projects_BQ,
    Merge_Data_Sprints_BQ,
    Merge_Data_Tickets_BQ,
)

# Importar capa ETL modular
from etl.extractor import get_raw_projects, get_raw_sprints, get_raw_tickets
from etl.transformer import clean_projects, clean_sprints, clean_tickets
from etl.loader import cargar_entidad

# Cargar variables de entorno
load_dotenv()

# Autenticación Jira
auth = os.getenv("AUTHORIZATION")
headers = {
    "Accept": "application/json",
    "Authorization": auth
}

# Configuración de credenciales de Google
CREDENTIALS_PATH = "./credenciales/data-warehouse-311917-73a0792225c7.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
TIMEZONE = pytz.timezone("America/Argentina/Buenos_Aires")

# Cliente de BigQuery
client = Get_BQ_service()

# Mapeo de entidades ETL
ENTIDADES = [
    {
        "nombre": "Projects",
        "extract": get_raw_projects,
        "transform": clean_projects,
        "schema": Esquema.schema_projects,
        "tabla_final": "tbl_jira_projects",
        "merge_func": Merge_Data_Projects_BQ,
    },
    {
        "nombre": "Sprints",
        "extract": get_raw_sprints,
        "transform": clean_sprints,
        "schema": Esquema.schema_sprints,
        "tabla_final": "tbl_jira_sprints",
        "merge_func": Merge_Data_Sprints_BQ,
    },
    {
        "nombre": "Tickets",
        "extract": get_raw_tickets,
        "transform": clean_tickets,
        "schema": Esquema.schema_tickets,
        "tabla_final": "tbl_jira_tickets",
        "merge_func": Merge_Data_Tickets_BQ,
    },
]

def ejecutar_entidad(entidad, filtro):
    try:
        print(f"\033[33mProcesando {entidad['nombre']}...\033[0m")
        df_raw = entidad["extract"](headers, filtro)
        df_clean = entidad["transform"](df_raw)
        cargar_entidad(client, entidad, df_clean)
    except Exception as e:
        print(f"\033[31mError procesando {entidad['nombre']}: {e}\033[0m")

def main(tipo="diario"):
    inicio = time.time()
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(ejecutar_entidad, entidad, tipo) for entidad in ENTIDADES]
        for future in futures:
            future.result()
    fin = time.time()
    print(f"\nDuración total: {fin - inicio:.2f} segundos.\n")

def ejecutar_tareas(historico=False):
    if historico:
        print("Ejecución histórica")
        main("historico")
    else:
        hoy = datetime.now(TIMEZONE)
        if hoy.weekday() == 0:
            print("Ejecución semanal")
            main("semanal")
        elif hoy.day == 1:
            print("Ejecución mensual")
            main("mensual")
        else:
            print("Ejecución diaria")
            main("diario")

if __name__ == "__main__":
    try:
        ejecutar_tareas(historico=False)  # Cambiá a False para ejecución diaria
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("Scheduler detenido manualmente.")
