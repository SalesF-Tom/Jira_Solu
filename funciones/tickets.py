import pandas as pd
import requests
from datetime import datetime, timedelta
from funciones.sprints import get_sprints
from concurrent.futures import ThreadPoolExecutor, as_completed

JIRA_BASE = "https://wearesolu.atlassian.net"

# ----------------------
# Helpers Epic
# ----------------------
def _get_json(url, headers, params=None, timeout=60):
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def discover_epic_link_field_id(headers) -> str | None:
    """Descubre el ID del campo 'Epic Link' (p.ej. customfield_10014)."""
    try:
        fields = _get_json(f"{JIRA_BASE}/rest/api/3/field", headers)
        for f in fields:
            name = (f.get("name") or "").strip().lower()
            if name in ("epic link", "epic-link", "epic"):
                return f.get("id")
    except Exception:
        pass
    return None

def fetch_epic_summary(epic_key: str, headers, cache: dict) -> str | None:
    """Trae el summary (nombre visible) de la épica, con caché por epic_key."""
    if not epic_key:
        return None
    if epic_key in cache:
        return cache[epic_key]
    try:
        data = _get_json(f"{JIRA_BASE}/rest/api/3/issue/{epic_key}", headers, params={"fields": "summary"})
        name = (data.get("fields") or {}).get("summary")
    except Exception:
        name = None
    cache[epic_key] = name
    return name

def extract_epic_from_issue(fields: dict, epic_link_cf: str | None, headers, epic_cache: dict) -> tuple[str | None, str | None]:
    """
    Devuelve (epic_key, epic_name) cubriendo:
      1) parent si es Epic
      2) Epic Link (customfield_****) -> key; resolvemos name por API
      3) fields['epic'] (team-managed)
    """
    epic_key = None
    epic_name = None

    # (1) Epic como parent (muy común en tu instancia)
    parent = fields.get("parent")
    if isinstance(parent, dict):
        p_fields = parent.get("fields") or {}
        p_type = ((p_fields.get("issuetype") or {}).get("name") or "").lower()
        if p_type == "epic":
            epic_key = parent.get("key") or parent.get("id")
            epic_name = p_fields.get("summary")
            if epic_key:
                return epic_key, epic_name

    # (2) Company-managed: Epic Link (customfield)
    if epic_link_cf:
        maybe_key = fields.get(epic_link_cf)
        if isinstance(maybe_key, str) and maybe_key:
            epic_key = maybe_key

    # (3) Team-managed: objeto 'epic'
    if not epic_key:
        epic_obj = fields.get("epic")
        if isinstance(epic_obj, dict):
            epic_key = epic_obj.get("key") or epic_obj.get("id")
            epic_name = epic_obj.get("name")

    # Si tengo key y no name, lo resuelvo por API
    if epic_key and not epic_name:
        epic_name = fetch_epic_summary(epic_key, headers, epic_cache)

    return epic_key, epic_name


# ----------------------
# Main function
# ----------------------
def get_tickets(headers, filtro: str = "diario") -> pd.DataFrame:
    """
    Descarga tickets de Jira por sprint y agrega epic_key / epic_name (Checkout, PLP, etc.).
    :param headers: dict con Authorization/Accept
    :param filtro: 'diario' | 'semanal' | 'mensual' | 'historico'
    """
    # --- Definir fecha de inicio según filtro ---
    if filtro == "diario":
        fecha_inicio = datetime.now() - timedelta(days=1)
    elif filtro == "semanal":
        fecha_inicio = datetime.now() - timedelta(days=7)
    elif filtro == "mensual":
        # primer día del mes actual - 1 día -> primer día del mes anterior, luego .replace(day=1)
        fecha_inicio = (datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1)
    elif filtro == "historico":
        fecha_inicio = None
    else:
        raise ValueError("El filtro debe ser 'diario', 'semanal', 'mensual' o 'historico'.")

    jql_filter = f"updated >= {fecha_inicio.strftime('%Y-%m-%d')}" if fecha_inicio else None

    # --- Detectar Epic Link (si existe) y preparar caché de épicas ---
    epic_link_cf = discover_epic_link_field_id(headers)  # ej. customfield_10014
    epic_cache: dict[str, str | None] = {}

    # --- Sprints a procesar ---
    sprints_df = get_sprints(headers, filtro)
    all_tickets: list[dict] = []

    def fetch_tickets_from_sprint(sprint_row):
        sprint_id = sprint_row["sprint_id"]
        base_url = f"{JIRA_BASE}/rest/agile/1.0/sprint/{sprint_id}/issue"
        start_at = 0
        max_results = 50
        rows = []

        while True:
            params = {"startAt": start_at, "maxResults": max_results}
            if jql_filter:
                params["jql"] = jql_filter

            try:
                data = _get_json(base_url, headers, params=params)
            except requests.HTTPError as e:
                print(f"⚠️ HTTP {e.response.status_code} en sprint {sprint_id}")
                break
            except Exception as e:
                print(f"⚠️ Error en sprint {sprint_id}: {e}")
                break

            issues = data.get("issues", [])
            for it in issues:
                f = it.get("fields", {}) or {}
                assignee = f.get("assignee")
                resolution = f.get("resolution")

                # --- Epic detection (parent / Epic Link / fields.epic) ---
                epic_key, epic_name = extract_epic_from_issue(f, epic_link_cf, headers, epic_cache)

                rows.append({
                    "sprint_id": sprint_id,
                    "ticket_id": str(it.get("id", "")),
                    "ticket_key": it.get("key", ""),
                    "ticket_summary": f.get("summary", ""),
                    "ticket_status": (f.get("status") or {}).get("name", ""),
                    "ticket_assignee": (assignee or {}).get("displayName", "Sin asignar"),
                    "ticket_priority": (f.get("priority") or {}).get("name", "Sin prioridad"),
                    "ticket_type": (f.get("issuetype") or {}).get("name", "Sin tipo"),
                    "ticket_created": f.get("created", ""),
                    "ticket_original_estimate": f.get("aggregatetimeoriginalestimate"),
                    "ticket_updated": f.get("updated", ""),
                    "ticket_resolution": (resolution or {}).get("name", "Sin resolución"),
                    "ticket_labels": ", ".join(f.get("labels", []) or []),
                    "epic_key": epic_key,
                    "epic_name": epic_name,
                })

            if data.get("isLast", True) or len(issues) < max_results:
                break
            start_at += max_results

        return rows

    # --- Paralelizar por sprint ---
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(fetch_tickets_from_sprint, r) for _, r in sprints_df.iterrows()]
        for fut in as_completed(futures):
            try:
                all_tickets.extend(fut.result())
            except Exception as e:
                print(f"Error obteniendo tickets: {e}")

    df = pd.DataFrame(all_tickets)

    # --- Convertir a datetime ---
    for col in ["ticket_created", "ticket_updated"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    return df