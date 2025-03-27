import os
from datetime import datetime
import pytz
import schedule
from dotenv import load_dotenv
import time
from bigquery.bigquery_func import Get_BQ_service, Insertar_Datos_BQ
from schema.schemas import Esquema
from funciones.projects import get_projects
from funciones.tickets import  get_tickets
from funciones.sprints import get_sprints
from bigquery.querys import (
    Merge_Data_Projects_BQ,
    Merge_Data_Sprints_BQ,
    Merge_Data_Tickets_BQ,
)

load_dotenv()

# Recuperar las credenciales desde las variables de entorno
auth = os.getenv("AUTHORIZATION")  # Auth de Jira

# Crear el diccionario headers
headers = {
    "Accept": "application/json",
    "Authorization": auth
}

# Configuración de credenciales
CREDENTIALS_PATH = "./credenciales/data-warehouse-311917-73a0792225c7.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
TIMEZONE = pytz.timezone("America/Argentina/Buenos_Aires")

# Cliente de BigQuery
client = Get_BQ_service()

# Mapeo de funciones y esquemas
ENTIDADES = [
    {
        "nombre": "Projects",
        "func_get": get_projects,
        "schema": Esquema.schema_projects,
        "tabla_final": "tbl_jira_projects",
        "merge_func": Merge_Data_Projects_BQ,
    },
    {
        "nombre": "Sprints",
        "func_get": get_sprints,
        "schema": Esquema.schema_sprints,
        "tabla_final": "tbl_jira_sprints",
        "merge_func": Merge_Data_Sprints_BQ,
    },
    {
        "nombre": "Tickets",
        "func_get": get_tickets,
        "schema": Esquema.schema_tickets,
        "tabla_final": "tbl_jira_tickets",
        "merge_func": Merge_Data_Tickets_BQ,
    },
]


def ejecutar_entidad(entidad, filtro):
    """Ejecuta el flujo de carga y merge para una entidad."""
    try:
        print(f"\033[33m {entidad['nombre']}: \033[0m")
        data = entidad["func_get"](headers, filtro)
        if not data.empty:
            Insertar_Datos_BQ(client, entidad["schema"], entidad["tabla_final"], data, "temp", "WRITE_TRUNCATE")
            entidad["merge_func"](
                client,
                f"data-warehouse-311917.Jira.{entidad['tabla_final']}",
                f"data-warehouse-311917.zt_productive_temp.{entidad['tabla_final']}_temp",
            )
        else:
            print(f"No hay datos para {entidad['nombre']}.")
    except Exception as e:
        print(f"\033[31m Error procesando {entidad['nombre']}: {e} \033[0m")


def main(tipo="diario"):
    """Ejecución principal."""
    inicio_ejecucion = time.time()
    for entidad in ENTIDADES:
        ejecutar_entidad(entidad, filtro=tipo)
    fin_ejecucion = time.time()
    print(f'Duración de ejecución: {fin_ejecucion-inicio_ejecucion}')
    print("Esperando la hora programada...")


def ejecutar_tareas(historico=False):
    """Planificador de tareas."""
    if historico : 
        print('Ejecución histórica')
        main('historico')
    else:
        hoy = datetime.now(TIMEZONE)
        if hoy.weekday() == 0:  # Lunes
            print("Ejecución semanal")
            main("semanal")
        elif hoy.day == 1:  # Primer día del mes
            print("Ejecución mensual")
            main("mensual")
        else:
            print("Ejecución diaria")
            main("diario")



if __name__ == "__main__":
    try:
        # schedule.every().day.at("02:00").do(ejecutar_tareas)
        # print("Esperando la hora programada...")
        # ejecutar_tareas()
        ejecutar_tareas(True) #histórico 

        while True:
            schedule.run_pending()
            time.sleep(60)

    except KeyboardInterrupt:
        print("Scheduler detenido manualmente.")





