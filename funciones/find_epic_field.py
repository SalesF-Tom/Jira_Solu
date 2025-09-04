import os
import sys
import json
import requests
from typing import Dict, Tuple, Optional, Any, List

# Carga .env
try:
    from dotenv import load_dotenv
    load_dotenv()  # lee variables desde .env, sin romper si no existe
except Exception:
    pass

# =======================
# CONFIG desde .env
# =======================
JIRA_BASE = os.getenv("JIRA_BASE", "https://wearesolu.atlassian.net")
API_ISSUE_SEARCH = f"{JIRA_BASE}/rest/api/3/search"
API_FIELDS = f"{JIRA_BASE}/rest/api/3/field"
API_ISSUE = f"{JIRA_BASE}/rest/api/3/issue"

# =======================
# HELPERS
# =======================
def build_headers() -> Dict[str, str]:
    """
    Construye headers para JIRA:
    - Si existe AUTHORIZATION (base64 de 'user:token'), lo usa como 'Basic <AUTHORIZATION>'
    - Si no, intenta con JIRA_EMAIL + JIRA_API_TOKEN
    """
    auth_b64 = os.getenv("AUTHORIZATION")
    email = os.getenv("JIRA_EMAIL")
    token = os.getenv("JIRA_API_TOKEN")

    if auth_b64:
        auth_header = f"Basic {auth_b64}"
    else:
        if not email or not token:
            print(
                "❌ Faltan credenciales para Jira.\n"
                "   Opciones:\n"
                "   1) AUTHORIZATION=<base64(user:token)> en .env\n"
                "   2) JIRA_EMAIL y JIRA_API_TOKEN en .env\n"
            )
            sys.exit(1)
        # requests._basic_auth_str devuelve 'Basic <b64>', pero prefiero separar el prefijo
        basic = requests.auth._basic_auth_str(email, token)  # 'Basic <b64>'
        auth_header = basic  # ya incluye 'Basic ...'

    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": auth_header,
    }


def get_fields_index(headers: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    """ Devuelve {fieldId: fieldObject} desde /rest/api/3/field """
    r = requests.get(API_FIELDS, headers=headers)
    r.raise_for_status()
    fields = r.json()
    return {f["id"]: f for f in fields}


def detect_epic_link_field_id(fields_index: Dict[str, Dict[str, Any]]) -> Optional[str]:
    """
    Busca el fieldId cuyo nombre sea 'Epic Link' (o similar).
    Retorna p.ej. 'customfield_10014' o None si no existe (team-managed).
    """
    for fid, fobj in fields_index.items():
        name = (fobj.get("name") or "").strip().lower()
        if name in ("epic link", "epic-link", "epic"):
            return fid
    return None


def find_epic_fields_in_issue(issue: Dict[str, Any]) -> Dict[str, Any]:
    """
    Inspecciona un issue y devuelve las keys/valores que contienen 'epic' en 'fields'.
    Útil para descubrir cómo llega en tu instancia.
    """
    fields = issue.get("fields", {}) or {}
    found = {}
    for k, v in fields.items():
        k_low = k.lower()
        if "epic" in k_low:
            found[k] = v
        else:
            # si es dict/list, buscamos 'epic' en su representación
            if isinstance(v, (dict, list)) and "epic" in json.dumps(v).lower():
                found[k] = v
    return found


def fetch_epic_summary(epic_key: str, headers: Dict[str, str], cache: Dict[str, Optional[str]]) -> Optional[str]:
    """ Dado un epic_key (p.ej. 'SM-123'), trae el summary del epic (con caché) """
    if not epic_key:
        return None
    if epic_key in cache:
        return cache[epic_key]
    url = f"{API_ISSUE}/{epic_key}"
    r = requests.get(url, headers=headers, params={"fields": "summary"})
    summary = None
    if r.status_code == 200:
        summary = (r.json().get("fields") or {}).get("summary")
    cache[epic_key] = summary
    return summary


def extract_epic_from_issue(
    issue: Dict[str, Any],
    epic_link_field_id: Optional[str],
    headers: Dict[str, str],
    epic_cache: Dict[str, Optional[str]],
) -> Tuple[Optional[str], Optional[str]]:
    """
    Intenta extraer (epic_key, epic_name) combinando:
    - Team-managed: fields['epic'] → { key, name }
    - Company-managed: fields[epic_link_field_id] → 'KEY'; luego resuelve nombre con fetch_epic_summary
    """
    fields = issue.get("fields", {}) or {}
    epic_key = None
    epic_name = None

    # Team-managed
    epic_obj = fields.get("epic")
    if isinstance(epic_obj, dict):
        epic_key = epic_obj.get("key") or epic_obj.get("id")
        epic_name = epic_obj.get("name")

    # Company-managed via Epic Link custom field
    if not epic_key and epic_link_field_id:
        maybe_key = fields.get(epic_link_field_id)
        if isinstance(maybe_key, str):
            epic_key = maybe_key

    # Si tengo key pero no name, lo resuelvo vía API (cacheado)
    if epic_key and not epic_name:
        epic_name = fetch_epic_summary(epic_key, headers, epic_cache)

    return epic_key, epic_name


def search_recent_issues(headers: Dict[str, str],
                         fields_index: Dict[str, Dict[str, Any]],
                         jql: Optional[str] = None,
                         max_results: int = 20) -> List[Dict[str, Any]]:
    """
    Busca issues con posibilidad de épica. Incluye el Epic Link (customfield_****) si existe.
    """
    if jql is None:
        jql = "ORDER BY updated DESC"

    epic_field_id = detect_epic_link_field_id(fields_index)

    base_fields = [
        "summary", "status", "assignee", "priority", "issuetype",
        "created", "aggregatetimeoriginalestimate", "updated",
        "resolution", "labels", "parent", "epic"
    ]
    if epic_field_id:
        base_fields.append(epic_field_id)  # <-- pedimos explícitamente el Epic Link

    params = {
        "jql": jql,
        "maxResults": max_results,
        "fields": ",".join(base_fields),
        "expand": "names"  # útil para mapear nombres de custom fields
    }
    r = requests.get(API_ISSUE_SEARCH, headers=headers, params=params)
    r.raise_for_status()
    return r.json().get("issues", [])


# =======================
# MAIN
# =======================
def main():
    headers = build_headers()

    print("🔎 Cargando índice de campos...")
    fields_index = get_fields_index(headers)
    epic_field_id = detect_epic_link_field_id(fields_index)
    if epic_field_id:
        print(f"✅ Epic Link detectado: {epic_field_id} (name='{fields_index[epic_field_id]['name']}')\n")
    else:
        print("ℹ️ No se encontró 'Epic Link' en /field. Puede ser team-managed o venir como fields['epic'].\n")

    print("🔎 Buscando issues recientes...")
    issues = search_recent_issues(headers, fields_index,
                              jql="project in (SM, JMC, HA) AND 'Epic Link' is not EMPTY ORDER BY updated DESC",
                              max_results=20)
    if not issues:
        print("No se encontraron issues. Ajustá JQL en search_recent_issues().")
        return

    epic_cache: Dict[str, Optional[str]] = {}
    print("=========== RESULTADOS ===========")
    for i, issue in enumerate(issues, 1):
        key = issue.get("key")
        itype = (issue.get("fields", {}).get("issuetype", {}) or {}).get("name")
        print(f"\n#{i} Issue {key}  (type={itype})")

        # 1) Mostrar todas las fields que contienen 'epic' para inspección
        epic_candidates = find_epic_fields_in_issue(issue)
        if epic_candidates:
            print("   • Campos que contienen 'epic':")
            for k, v in epic_candidates.items():
                vs = json.dumps(v, ensure_ascii=False)
                vs = (vs[:200] + "...") if len(vs) > 200 else vs
                print(f"     - {k}: {vs}")
        else:
            print("   • No se detectaron campos 'epic' en fields (por nombre/valor).")

        # 2) Extraer epic_key / epic_name de forma robusta
        epic_key, epic_name = extract_epic_from_issue(issue, epic_field_id, headers, epic_cache)
        print(f"   • epic_key: {epic_key}")
        print(f"   • epic_name: {epic_name}")

    print("\n🏁 Listo. Si ya identificaste la key correcta (p.ej. 'customfield_10014'), podés usarla en tu ETL.")


if __name__ == "__main__":
    main()