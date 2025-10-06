# etl/transformer.py
import pandas as pd
from typing import List

# ---------- utilidades ----------

def _to_datetime_utc(series: pd.Series, errors: str = "coerce") -> pd.Series:
    """Convierte a datetime UTC de forma segura."""
    return pd.to_datetime(series, errors=errors, utc=True)

def _to_str(series: pd.Series) -> pd.Series:
    """Convierte a string y quita espacios extremos; maneja NaN."""
    s = series.astype("string")
    return s.str.strip()

def _join_labels(val) -> str:
    """Normaliza labels: lista -> 'a, b, c'; string -> trim."""
    if isinstance(val, list):
        return ", ".join(map(str, val))
    if pd.isna(val):
        return ""
    return str(val).strip()

# ---------- tickets ----------

def clean_tickets(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza de tickets (Jira):
      - Valida y tipa columnas clave
      - Convierte datetimes a UTC
      - Convierte ticket_original_estimate (seg) -> ticket_original_estimate_hours (float)
      - Normaliza labels
      - Deduplica por ticket_key (mantiene el más reciente por ticket_updated)
    """
    if df is None or df.empty:
        return df

    df = df.copy()

    # Filtros mínimos
    must_have = ["ticket_key", "ticket_status"]
    for c in must_have:
        if c not in df.columns:
            df[c] = pd.NA

    df = df[df["ticket_key"].notna()]
    df = df[df["ticket_status"].notna()]

    # Strings estándar
    str_cols: List[str] = [
        "ticket_key", "ticket_status", "ticket_summary", "ticket_assignee",
        "ticket_priority", "ticket_type", "ticket_resolution", "ticket_labels",
        "epic_key", "epic_name"
    ]
    for c in str_cols:
        if c in df.columns:
            df[c] = _to_str(df[c])

    # Labels (si vienen como lista desde algún extractor)
    if "ticket_labels" in df.columns:
        df["ticket_labels"] = df["ticket_labels"].apply(_join_labels)

    # Datetimes → UTC
    if "ticket_created" in df.columns:
        df["ticket_created"] = _to_datetime_utc(df["ticket_created"])
    if "ticket_updated" in df.columns:
        df["ticket_updated"] = _to_datetime_utc(df["ticket_updated"])

    # Original estimate (segundos → horas)
    if "ticket_original_estimate" in df.columns:
        # preservo la original por si la querés mantener; si no, podés dropearla
        df["ticket_original_estimate_seconds"] = pd.to_numeric(
            df["ticket_original_estimate"], errors="coerce"
        )
        df["ticket_original_estimate_hours"] = (
            df["ticket_original_estimate_seconds"] / 3600.0
        )

    # Deduplicación: una fila por ticket_key (la más reciente por updated)
    if "ticket_updated" not in df.columns:
        # si falta, lo creamos para no romper el sort; todo NaT
        df["ticket_updated"] = pd.NaT

    df = (
        df.sort_values(["ticket_key", "ticket_updated"], ascending=[True, False])
          .drop_duplicates(subset=["ticket_key"], keep="first")
          .reset_index(drop=True)
    )

    return df

# ---------- sprints ----------

def clean_sprints(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza de sprints:
      - Valida columnas mínimas
      - Strings y datetimes a tipos correctos
      - Deduplica por (sprint_id, board_id) quedándose con el más “reciente”
    """
    if df is None or df.empty:
        return df

    df = df.copy()

    # Presencia de columnas clave
    for c in ["sprint_id", "board_id", "sprint_name", "sprint_state"]:
        if c not in df.columns:
            df[c] = pd.NA

    # Filtrado mínimo
    df = df[df["sprint_name"].notna()]
    df = df[df["sprint_state"].notna()]

    # Tipos string
    for c in ["board_id", "sprint_id", "sprint_name", "sprint_state", "sprint_goal"]:
        if c in df.columns:
            df[c] = _to_str(df[c])

    # Datetimes
    for c in ["sprint_startDate", "sprint_endDate", "sprint_completeDate"]:
        if c in df.columns:
            df[c] = _to_datetime_utc(df[c])

    # Dedup por sprint_id + board_id (elige el con mayor endDate/completeDate)
    # Creamos una columna auxiliar para ordenar
    sort_col = None
    if "sprint_completeDate" in df.columns:
        sort_col = "sprint_completeDate"
    elif "sprint_endDate" in df.columns:
        sort_col = "sprint_endDate"

    if sort_col:
        df = (
            df.sort_values(["board_id", "sprint_id", sort_col], ascending=[True, True, False])
              .drop_duplicates(subset=["board_id", "sprint_id"], keep="first")
              .reset_index(drop=True)
        )
    else:
        df = (
            df.sort_values(["board_id", "sprint_id"], ascending=[True, True])
              .drop_duplicates(subset=["board_id", "sprint_id"], keep="last")
              .reset_index(drop=True)
        )

    return df

# ---------- projects ----------

def clean_projects(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza de proyectos/boards:
      - Valida columnas mínimas
      - Tipos string
      - Dedup por board_id
    """
    if df is None or df.empty:
        return df

    df = df.copy()

    # Presencia de columnas clave
    for c in ["board_id", "board_name"]:
        if c not in df.columns:
            df[c] = pd.NA

    # Filtrado mínimo
    df = df[df["board_id"].notna()]
    df = df[df["board_name"].notna()]

    # Tipos string
    for c in ["board_id", "board_name", "board_type", "project_key", "project_name", "project_location"]:
        if c in df.columns:
            df[c] = _to_str(df[c])

    # Dedup por board_id (mantengo la última por orden natural del dataset)
    df = (
        df.sort_values(["board_id"], ascending=True)
          .drop_duplicates(subset=["board_id"], keep="last")
          .reset_index(drop=True)
    )

    return df


def clean_users(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza el catálogo de usuarios de Jira antes de cargarlo en BigQuery."""
    if df is None or df.empty:
        return df

    df = df.copy()

    if "account_id" not in df.columns:
        df["account_id"] = pd.NA

    df = df[df["account_id"].notna()]

    str_cols: List[str] = [
        "account_id",
        "account_type",
        "account_status",
        "display_name",
        "public_name",
        "email_address",
        "time_zone",
        "locale",
        "self_url",
        "avatar_16x16",
        "avatar_24x24",
        "avatar_32x32",
        "avatar_48x48",
        "group_names",
        "application_roles",
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = _to_str(df[col])

    if "active" in df.columns:
        df["active"] = df["active"].fillna(False).astype(bool)

    df = (
        df.sort_values(["account_id"], ascending=True)
        .drop_duplicates(subset=["account_id"], keep="last")
        .reset_index(drop=True)
    )

    return df
