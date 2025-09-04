# main.py
import os
import time
import pytz
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# BigQuery
from google.cloud import bigquery
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

# =========================
# Setup
# =========================
load_dotenv()

PROJECT_ID    = os.getenv("PROJECT_ID", "data-warehouse-311917")
DATASET_FINAL = os.getenv("DATASET_FINAL", "Jira")
DATASET_TEMP  = os.getenv("DATASET_TEMP", "zt_Clear_Lab")

# Credenciales GCP
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "./credenciales/data-warehouse-311917-73a0792225c7.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

# Jira headers (Basic Auth)
JIRA_EMAIL     = os.getenv("JIRA_EMAIL")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")

if not JIRA_EMAIL or not JIRA_API_TOKEN:
    raise RuntimeError("Faltan variables de entorno JIRA_EMAIL / JIRA_API_TOKEN")

import requests
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": requests.auth._basic_auth_str(JIRA_EMAIL, JIRA_API_TOKEN),
}

TIMEZONE = pytz.timezone("America/Argentina/Buenos_Aires")

# Clientes / logger
client  = Get_BQ_service()  # si preferís: bigquery.Client(project=PROJECT_ID)
logger  = configurar_logger()

# Resumen thread-safe
resumen = []
resumen_lock = Lock()

def _add_resumen(line: str):
    with resumen_lock:
        resumen.append(line)

# =========================
# Config de Entidades
# =========================
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
        "schema": Esquema.schema_tickets,  # incluye epic_key/epic_name
        "tabla_final": "tbl_jira_tickets",
        "merge_func": Merge_Data_Tickets_BQ,
    },
]

# =========================
# Runner por entidad
# =========================
def ejecutar_entidad(entidad: dict, filtro: str):
    t0 = time.time()
    nombre = entidad["nombre"]

    try:
        logger.info(f"[{nombre}] Extract → Transform → Load (filtro={filtro})")

        # Extract
        df_raw = entidad["extract"](HEADERS, filtro)

        # Transform
        df_clean = entidad["transform"](df_raw)

        # Load (staging → merge)
        mb_fact = cargar_entidad(logger, client, entidad, df_clean)

        filas = len(df_clean)
        dur   = time.time() - t0
        _add_resumen(f"✅ {nombre}: {filas} filas | {mb_fact:.2f} MB | {dur:.1f}s")
        logger.info(f"[{nombre}] OK: {filas} filas, {mb_fact:.2f} MB, {dur:.1f}s")

    except Exception as e:
        msg = f"❌ {nombre} falló: {e}"
        logger.exception(msg)
        _add_resumen(msg)

# =========================
# Main orchestration
# =========================
def main(tipo: str = "diario", paralelo: bool = True):
    logger.info(f"=== ETL Jira ({tipo}) ===")
    t0 = time.time()

    if paralelo:
        with ThreadPoolExecutor(max_workers=3) as ex:
            futures = [ex.submit(ejecutar_entidad, ent, tipo) for ent in ENTIDADES]
            for _ in as_completed(futures):
                pass
    else:
        for ent in ENTIDADES:
            ejecutar_entidad(ent, tipo)

    total = time.time() - t0
    _add_resumen(f"⏱️ Total: {total:.1f}s")
    logger.info(f"ETL terminado en {total:.1f}s")

    # Notificación Discord
    try:
        enviar_resumen_discord("**Resumen ETL Jira**\n" + "\n".join(resumen))
    except Exception as e:
        logger.warning(f"No se pudo notificar a Discord: {e}")

# =========================
# Scheduling simple
# =========================
def ejecutar_tareas(historico: bool = False):
    if historico:
        logger.info("Ejecución histórica")
        main("historico", paralelo=True)
    else:
        hoy = datetime.now(TIMEZONE)
        if hoy.weekday() == 0:
            logger.info("Ejecución semanal")
            main("semanal", paralelo=True)
        elif hoy.day == 1:
            logger.info("Ejecución mensual")
            main("mensual", paralelo=True)
        else:
            logger.info("Ejecución diaria")
            main("diario", paralelo=True)

if __name__ == "__main__":
    ejecutar_tareas(historico=False)