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

    Insertar_Datos_BQ(
        logger, 
        client,
        entidad["schema"],
        entidad["tabla_final"],
        df,
        "temp",
        metodo="WRITE_TRUNCATE"
    )

    entidad["merge_func"](
        client,
        f"data-warehouse-311917.{datasetFinal}.{entidad['tabla_final']}",
        f"data-warehouse-311917.{datasetTemp}.{entidad['tabla_final']}_temp",
    )
