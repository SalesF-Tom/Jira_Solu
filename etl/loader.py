# etl/loader.py
import os
from typing import Dict, Any, Optional

from google.cloud import bigquery
from bigquery.bigquery_func import Insertar_Datos_BQ
from bigquery.querys import (
    Merge_Data_Projects_BQ,
    Merge_Data_Sprints_BQ,
    Merge_Data_Tickets_BQ,
)

# ===========
# Config
# ===========
PROJECT_ID   = os.getenv("PROJECT_ID", "data-warehouse-311917")
DATASET_FINAL = os.getenv("DATASET_FINAL", "Jira")
DATASET_TEMP  = os.getenv("DATASET_TEMP",  "zt_Clear_Lab")

MERGE_FUNCS = {
    "projects": Merge_Data_Projects_BQ,
    "sprints":  Merge_Data_Sprints_BQ,
    "tickets":  Merge_Data_Tickets_BQ,
}

# ===========
# Helpers
# ===========
def _full_table_id(dataset: str, table: str, project: Optional[str] = None) -> str:
    """Arma project.dataset.table de forma segura."""
    prj = project or PROJECT_ID
    return f"{prj}.{dataset}.{table}"

def _get_temp_table_name(tabla_final: str, tabla_temp: Optional[str]) -> str:
    """Si no se define tabla_temp, usar <tabla_final>_temp."""
    return tabla_temp or f"{tabla_final}_temp"

# ===========
# Carga genérica por entidad
# ===========
# etl/loader.py
from bigquery.bigquery_func import Insertar_Datos_BQ
import os
from bigquery.querys import (
    Merge_Data_Projects_BQ,
    Merge_Data_Sprints_BQ,
    Merge_Data_Tickets_BQ,
)

datasetFinal = os.getenv("DATASET_FINAL")
datasetTemp = os.getenv("DATASET_TEMP")

def cargar_entidad(logger, client, entidad, df):
    if df.empty:
        print(f"No hay datos para {entidad['nombre']}.")
        return

    # Carga a STAGING (dataset TEMP)
    filas_cargadas = Insertar_Datos_BQ(
        logger=logger,
        client=client,
        schema=entidad["schema"],
        nombre_tabla=entidad["tabla_final"],   # antes: table_id
        df_panda=df,
        tipo="temp",
        metodo="WRITE_TRUNCATE",               # antes: write_disposition
    )

    # MERGE: TEMP -> FINAL
    mb_fac = entidad["merge_func"](
        client,
        f"data-warehouse-311917.{datasetFinal}.{entidad['tabla_final']}",
        f"data-warehouse-311917.{datasetTemp}.{entidad['tabla_final']}_temp",
    )

    return mb_fac