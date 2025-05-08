import os
from datetime import datetime
import pytz
import time
import schedule
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

# Cliente de BigQuery
from bigquery.bigquery_func import Get_BQ_service

# Esquemas y merges
from schema.schemas import Esquema
from bigquery.querys import (
    Merge_Data_Projects_BQ,
    Merge_Data_Sprints_BQ,
    Merge_Data_Tickets_BQ,
)

# ETL modular
from etl.extractor import get_raw_projects, get_raw_sprints, get_raw_tickets
from etl.transformer import clean_projects, clean_sprints, clean_tickets
from etl.loader import cargar_entidad

# Logger y notificaciones
from utils.logger import configurar_logger
from utils.discord_notify import enviar_resumen_discord

# Configuración de entorno
load_dotenv()

auth = os.getenv("AUTHORIZATION")
headers = {
    "Accept": "application/json",
    "Authorization": "Basic " + auth
}

CREDENTIALS_PATH = "./credenciales/data-warehouse-311917-73a0792225c7.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
TIMEZONE = pytz.timezone("America/Argentina/Buenos_Aires")

client = Get_BQ_service()
logger = configurar_logger()
resumen = []

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
        logger.info(f"Procesando {entidad['nombre']}...")
        df_raw = entidad["extract"](headers, filtro)
        df_clean = entidad["transform"](df_raw)
        mbytes_fac = cargar_entidad(logger, client, entidad, df_clean)
        filas = len(df_clean)
        resumen.append(f"✅ {entidad['nombre']}: {filas} filas procesadas | {mbytes_fac:.2f} MB Billed")
        logger.info(f"{entidad['nombre']}: {filas} filas cargadas.")
    except Exception as e:
        mensaje = f"❌ Error procesando {entidad['nombre']}: {e}"
        logger.error(mensaje)
        resumen.append(mensaje)


def main(tipo="diario"):
    print(f"---== Ejecutando Extract {tipo} ==---")
    inicio = time.time()

    # ----------------------- LO SACO PARA TESTEAR TIEMPOS -----------------------
    with ThreadPoolExecutor(max_workers=3) as executor:
        print(f"---== Ejecutando Hilo ==---")
        futures = [executor.submit(ejecutar_entidad, entidad, tipo) for entidad in ENTIDADES]
        for future in futures:
            future.result()
    # ----------------------- ----------------------- -----------------------

    # print(f"---== Ejecutando entidades secuencialmente ==---")
    # for entidad in ENTIDADES:
    #     ejecutar_entidad(entidad, tipo)
    
    fin = time.time()
    duracion = f"⏱️ Duración total: {fin - inicio:.2f} segundos"
    resumen.append(duracion)
    logger.info(duracion)
    enviar_resumen_discord("**Resumen ETL Jira**\n" + "\n".join(resumen))


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
    # main()
    ejecutar_tareas(historico=False)
    # try:
    #     ejecutar_tareas(historico=False)
    #     while True:
    #         schedule.run_pending()
    #         time.sleep(60)
    # except KeyboardInterrupt:
    #     print("Scheduler detenido manualmente.")
