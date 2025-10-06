import pandas as pd
from typing import Dict, Any, List

from funciones.tickets import _get_json, JIRA_BASE


def _flatten_items(items: List[Dict[str, Any]]) -> str:
    """Convierte una lista de objetos en string con los nombres separados por coma."""
    if not items:
        return ""
    names = []
    for item in items:
        name = item.get("name") or item.get("displayName")
        if name:
            names.append(str(name))
    return ", ".join(sorted(set(names)))


def get_users(headers, filtro: str | None = None) -> pd.DataFrame:
    """Descarga todos los usuarios de Jira (REST API v3) paginando /users/search."""
    url = f"{JIRA_BASE}/rest/api/3/users/search"
    start_at = 0
    max_results = 100
    rows: List[Dict[str, Any]] = []

    while True:
        params = {"startAt": start_at, "maxResults": max_results}
        data = _get_json(url, headers, params=params)

        if not isinstance(data, list) or not data:
            if isinstance(data, list) and not data:
                break
            # Si viene un dict es un error inesperado; salimos.
            break

        for user in data:
            avatar_urls = user.get("avatarUrls") or {}
            groups = user.get("groups") or {}
            application_roles = user.get("applicationRoles") or {}

            rows.append(
                {
                    "account_id": user.get("accountId"),
                    "account_type": user.get("accountType"),
                    "account_status": user.get("accountStatus"),
                    "display_name": user.get("displayName"),
                    "public_name": user.get("publicName"),
                    "email_address": user.get("emailAddress"),
                    "active": user.get("active"),
                    "time_zone": user.get("timeZone"),
                    "locale": user.get("locale"),
                    "self_url": user.get("self"),
                    "avatar_16x16": avatar_urls.get("16x16"),
                    "avatar_24x24": avatar_urls.get("24x24"),
                    "avatar_32x32": avatar_urls.get("32x32"),
                    "avatar_48x48": avatar_urls.get("48x48"),
                    "group_names": _flatten_items(groups.get("items", [])),
                    "application_roles": _flatten_items(application_roles.get("items", [])),
                }
            )

        start_at += len(data)
        if len(data) < max_results:
            break

    return pd.DataFrame(rows)
