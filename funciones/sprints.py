import pandas as pd
import requests
from datetime import datetime, timedelta
from funciones.projects import get_projects

def get_sprints(headers, filtro="diario"):
    """
    Obtiene sprints actualizados según el filtro especificado (diario, semanal, mensual, historico).

    :param headers: Encabezados para la autenticación en la API
    :param filtro: Frecuencia del filtro (diario, semanal, mensual, historico)
    :return: DataFrame con la información de los sprints
    """
    # Calcular la fecha base según el filtro
    if filtro == "diario":
        fecha_inicio = datetime.now() - timedelta(days=1)
    elif filtro == "semanal":
        fecha_inicio = datetime.now() - timedelta(days=7)
    elif filtro == "mensual":
        fecha_inicio = datetime.now().replace(day=1) - timedelta(days=1)
        fecha_inicio = fecha_inicio.replace(day=1)  # Primer día del mes pasado
    elif filtro == "historico":
        fecha_inicio = None  # No se aplica filtro de fecha
    else:
        raise ValueError("El filtro debe ser 'diario', 'semanal', 'mensual' o 'historico'.")

    # Convertir fecha_inicio a tipo compatible con pandas si no es None
    if fecha_inicio:
        fecha_inicio = pd.Timestamp(fecha_inicio, tz="UTC")

    projects_df = get_projects(headers)
    all_sprints = []

    # Recorrer cada board para obtener sus sprints
    for _, project in projects_df.iterrows():
        board_id = project["board_id"]
        url_sprints = f"https://wearesolu.atlassian.net/rest/agile/1.0/board/{board_id}/sprint"

        response = requests.get(url_sprints, headers=headers)
        if response.status_code != 200:
            continue

        sprints = response.json().get("values", [])
        for sprint in sprints:
            sprint_end_date = pd.to_datetime(sprint.get("endDate", ""), errors="coerce", utc=True)
            sprint_state = sprint.get("state", "")

            if filtro == "historico" or (
                sprint_end_date is not None and sprint_end_date >= fecha_inicio
            ) or sprint_state != "closed":
                all_sprints.append({
                    "board_id": board_id,
                    "sprint_id": str(sprint["id"]),
                    "sprint_name": sprint.get("name", ""),
                    "sprint_state": sprint_state,
                    "sprint_startDate": sprint.get("startDate", ""),
                    "sprint_endDate": sprint.get("endDate", ""),
                    "sprint_completeDate": sprint.get("completeDate", ""),
                    "sprint_goal": sprint.get("goal", ""),
                })

    df = pd.DataFrame(all_sprints)

    # Convertir las columnas datetime
    datetime_columns = ["sprint_startDate", "sprint_endDate", "sprint_completeDate"]
    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    return df
