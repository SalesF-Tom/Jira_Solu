import pandas as pd
import requests
from datetime import datetime, timedelta
from funciones.sprints import get_sprints
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_tickets(headers, filtro="diario"):
    if filtro == "diario":
        fecha_inicio = datetime.now() - timedelta(days=1)
    elif filtro == "semanal":
        fecha_inicio = datetime.now() - timedelta(days=7)
    elif filtro == "mensual":
        fecha_inicio = datetime.now().replace(day=1) - timedelta(days=1)
        fecha_inicio = fecha_inicio.replace(day=1)
    elif filtro == "historico":
        fecha_inicio = None
    else:
        raise ValueError("El filtro debe ser 'diario', 'semanal', 'mensual' o 'historico'.")

    jql_filter = f"updated >= {fecha_inicio.strftime('%Y-%m-%d')}" if fecha_inicio else None
    sprints_df = get_sprints(headers, filtro)
    all_tickets = []

    def fetch_tickets_from_sprint(sprint):
        sprint_id = sprint["sprint_id"]
        base_url = f"https://wearesolu.atlassian.net/rest/agile/1.0/sprint/{sprint_id}/issue"
        start_at = 0
        max_results = 50
        tickets_result = []

        while True:
            params = {"startAt": start_at, "maxResults": max_results}
            if jql_filter:
                params["jql"] = jql_filter
            
            response = requests.get(base_url, headers=headers, params=params)
            if response.status_code != 200:
                break

            data = response.json()
            issues = data.get("issues", [])
            for ticket in issues:
                assignee = ticket["fields"].get("assignee")
                resolution = ticket["fields"].get("resolution")

                tickets_result.append({
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

            if data.get("isLast", True) or len(issues) < max_results:
                break

            start_at += max_results

        return tickets_result

    # Paralelizar fetch de tickets por sprint
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(fetch_tickets_from_sprint, row) for _, row in sprints_df.iterrows()]
        for future in as_completed(futures):
            try:
                all_tickets.extend(future.result())
            except Exception as e:
                print(f"Error obteniendo tickets: {e}")

    df = pd.DataFrame(all_tickets)

    # Convertir columnas datetime
    datetime_columns = ["ticket_created", "ticket_updated"]
    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    return df