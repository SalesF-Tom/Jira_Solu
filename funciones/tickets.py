import pandas as pd
import requests
from datetime import datetime, timedelta
from funciones.sprints import get_sprints

def get_tickets(headers, filtro="diario"):
    """
    Obtiene tickets actualizados según el filtro especificado (diario, semanal, mensual, historico).
    
    :param headers: Encabezados para la autenticación en la API
    :param filtro: Frecuencia del filtro (diario, semanal, mensual, historico)
    :return: DataFrame con la información de los tickets
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

    # Convertir fecha_inicio a tipo compatible con formato JQL
    jql_filter = f"updated >= {fecha_inicio.strftime('%Y-%m-%d')}" if fecha_inicio else None

    sprints_df = get_sprints(headers, filtro)
    all_tickets = []

    # Recorrer cada sprint para obtener sus tickets actualizados
    for _, sprint in sprints_df.iterrows():
        sprint_id = sprint["sprint_id"]
        url_tickets = f"https://wearesolu.atlassian.net/rest/agile/1.0/sprint/{sprint_id}/issue"
        params = {"jql": jql_filter} if jql_filter else {}

        response = requests.get(url_tickets, headers=headers, params=params)
        if response.status_code != 200:
            continue
        
        tickets = response.json().get("issues", [])
        for ticket in tickets:
            assignee = ticket["fields"].get("assignee")
            resolution = ticket["fields"].get("resolution")
            
            all_tickets.append({
                "sprint_id": sprint_id,
                "ticket_id": str(ticket.get("id", "")),
                "ticket_key": ticket.get("key", ""),
                "ticket_summary": ticket["fields"].get("summary", ""),
                "ticket_status": ticket["fields"].get("status", {}).get("name", ""),
                "ticket_assignee": assignee.get("displayName", "Sin asignar") if assignee else "Sin asignar",
                "ticket_priority": ticket["fields"].get("priority", {}).get("name", "Sin prioridad"),
                "ticket_type": ticket["fields"].get("issuetype", {}).get("name", "Sin tipo"),
                "ticket_created": ticket["fields"].get("created", ""),
                "ticket_original_estimate": ticket["fields"].get("aggregatetimeoriginalestimate", None),
                "ticket_updated": ticket["fields"].get("updated", ""),
                "ticket_resolution": resolution.get("name", "Sin resolución") if resolution else "Sin resolución",
                "ticket_labels": ", ".join(ticket["fields"].get("labels", [])),
            })

    df = pd.DataFrame(all_tickets)

    # Convertir las columnas datetime
    datetime_columns = ["ticket_created", "ticket_updated"]
    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    return df
