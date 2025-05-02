# etl/loader.py
from bigquery.bigquery_func import Insertar_Datos_BQ
from bigquery.querys import (
    Merge_Data_Projects_BQ,
    Merge_Data_Sprints_BQ,
    Merge_Data_Tickets_BQ,
)

def cargar_entidad(client, entidad, df):
    if df.empty:
        print(f"No hay datos para {entidad['nombre']}.")
        return

    Insertar_Datos_BQ(
        client, entidad["schema"], entidad["tabla_final"], df, "temp", metodo="WRITE_TRUNCATE"
    )

    entidad["merge_func"](
        client,
        f"data-warehouse-311917.Jira.{entidad['tabla_final']}",
        f"data-warehouse-311917.zt_productive_temp.{entidad['tabla_final']}_temp",
    )
