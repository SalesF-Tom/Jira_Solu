# main.py
import os
import time
import base64
import pytz
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

# BigQuery
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


# =======================
# Configuración
# =======================
load_dotenv()

TIMEZONE = pytz.timezone("America/Argentina/Buenos_Aires")

# Auth Jira: email + token → base64
JIRA_EMAIL = os.getenv("JIRA_EMAIL")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")

if not JIRA_EMAIL or not JIRA_API_TOKEN:
    raise RuntimeError("❌ Faltan variables JIRA_EMAIL / JIRA_API_TOKEN en .env")

_auth_str = f"{JIRA_EMAIL}:{JIRA_API_TOKEN}"
_auth_b64 = base64.b64encode(_auth_str.encode()).decode()

headers = {
    "Accept": "application/json",
    "Authorization": f"Basic {_auth_b64}",
}

# Cliente BigQuery
client = Get_BQ_service()

# Logger
logger = configurar_logger()
resumen = []


# =======================
# Config de entidades
# =======================
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


# =======================
# Helpers de ejecución
# =======================
def ejecutar_entidad(entidad, filtro: str):
    """
    Ejecuta Extract → Transform → Load para una entidad.
    """
    t0 = time.time()
    nombre = entidad["nombre"]
    try:
        logger.info(f"[{nombre}] Extract → Transform → Load (filtro={filtro})")
        df_raw = entidad["extract"](headers, filtro)
        df_clean = entidad["transform"](df_raw)

        mb_fact = cargar_entidad(logger, client, entidad, df_clean)
        filas = len(df_clean)
        dt = time.time() - t0

        resumen.append(f"✅ {nombre}: {filas} filas | {mb_fact:.2f} MB | {dt:.1f}s")
        logger.info(f"[{nombre}] OK: {filas} filas, {mb_fact:.2f} MB, {dt:.1f}s")

    except Exception as e:
        msg = f"❌ {nombre} falló: {e}"
        logger.error(msg, exc_info=True)
        resumen.append(msg)


def main(tipo: str = "diario", paralelo: bool = True):
    """
    Ejecuta la ETL para el tipo indicado: diario / semanal / mensual / historico
    """
    logger.info(f"=== ETL Jira ({tipo}) ===")
    t0 = time.time()

    if paralelo:
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(ejecutar_entidad, entidad, tipo) for entidad in ENTIDADES]
            for f in futures:
                f.result()
    else:
        for entidad in ENTIDADES:
            ejecutar_entidad(entidad, tipo)

    dt = time.time() - t0
    resumen.append(f"⏱️ Duración total: {dt:.1f}s")
    logger.info(f"ETL terminado en {dt:.1f}s")

    # Notificación (opcional, si tenés DISCORD_WEBHOOK_URL en .env)
    try:
        enviar_resumen_discord("**Resumen ETL Jira**\n" + "\n".join(resumen))
    except Exception as _:
        # Evita romper por fallas de webhook
        pass


def ejecutar_tareas(historico: bool = False):
    """
    Orquesta la ejecución según el día del calendario:
      - historico=True → backfill completo
      - caso contrario: lunes=semanal, día 1=mensual, resto=diario
    """
    if historico:
        logger.info("Ejecución histórica")
        main("historico", paralelo=False)  # opcional: sin paralelismo para cuidar rate limits
        return

    hoy = datetime.now(TIMEZONE)
    logger.info("Ejecución diaria" if (hoy.weekday() not in (0,) and hoy.day != 1) else
                "Ejecución semanal" if hoy.weekday() == 0 else
                "Ejecución mensual")

    if hoy.weekday() == 0:
        main("semanal")
    elif hoy.day == 1:
        main("mensual")
    else:
        main("diario")


# =======================
# Entry point
# =======================
if __name__ == "__main__":
    # Normal: decide diario/semanal/mensual
    ejecutar_tareas(historico=False)

    # Para backfill manual, corré:
    #   python -c "import main; main.ejecutar_tareas(historico=True)"