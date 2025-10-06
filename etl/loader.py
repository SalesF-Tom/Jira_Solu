# etl/loader.py
import os
from typing import Optional

from bigquery.bigquery_func import Insertar_Datos_BQ


PROJECT_ID = os.getenv("PROJECT_ID", "data-warehouse-311917")
DATASET_FINAL = os.getenv("DATASET_FINAL", "Jira")
DATASET_TEMP = os.getenv("DATASET_TEMP", "zt_Jira_temp")


def _full_table_id(dataset: str, table: str, project: Optional[str] = None) -> str:
    """Compone project.dataset.table de forma segura."""
    prj = project or PROJECT_ID
    return f"{prj}.{dataset}.{table}"


def _get_temp_table_name(tabla_final: str, tabla_temp: Optional[str]) -> str:
    """Si no se define tabla_temp, usa <tabla_final>_temp."""
    return tabla_temp or f"{tabla_final}_temp"


def cargar_entidad(logger, client, entidad, df):
    if df is None or df.empty:
        logger.info(f"[{entidad['nombre']}] Sin datos nuevos; se omite carga/merge.")
        return 0.0

    Insertar_Datos_BQ(
        logger=logger,
        client=client,
        schema=entidad["schema"],
        nombre_tabla=entidad["tabla_final"],
        df_panda=df,
        tipo="temp",
        metodo="WRITE_TRUNCATE",
    )

    final_id = _full_table_id(DATASET_FINAL, entidad["tabla_final"], PROJECT_ID)
    temp_table_name = _get_temp_table_name(entidad["tabla_final"], entidad.get("tabla_temp"))
    temp_id = _full_table_id(DATASET_TEMP, temp_table_name, PROJECT_ID)

    merge_func = entidad["merge_func"]
    mb_fact = merge_func(client, final_id, temp_id)
    return mb_fact
