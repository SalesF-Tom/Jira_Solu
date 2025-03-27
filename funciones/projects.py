import pandas as pd
import requests
from datetime import datetime, timedelta

def get_projects(headers, filtro=None):
    """
    Obtiene proyectos actualizados según el filtro especificado (diario, semanal, mensual).
    
    :param headers: Encabezados para la autenticación en la API
    :param filtro: Frecuencia del filtro (diario, semanal, mensual)
    :return: DataFrame con la información de los boards (proyectos)
    """
    url_boards = "https://wearesolu.atlassian.net/rest/agile/1.0/board"
    start_at = 0
    max_results = 50
    all_boards = []

    # Obtener todos los boards con paginación
    while True:
        response = requests.get(url_boards, headers=headers, params={"startAt": start_at, "maxResults": max_results})
        if response.status_code != 200:
            raise Exception(f"Error al obtener boards: {response.status_code} - {response.text}")
        
        data = response.json()
        all_boards.extend(data.get("values", []))
        if data.get("isLast", True):
            break
        start_at += max_results

    # Estructurar los datos en un DataFrame
    projects_data = [
        {
            "board_id": str(board["id"]),
            "board_name": board["name"],
            "board_type": board["type"],
            "project_key": board["location"].get("projectKey", "Desconocido"),
            "project_name": board["location"].get("projectName", "Desconocido"),
            "project_location": board["location"].get("displayName", "Desconocido")
        }
        for board in all_boards
    ]

    df = pd.DataFrame(projects_data)

    return df
