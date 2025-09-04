# etl/extractor.py
from __future__ import annotations
import pandas as pd
from typing import Literal, Dict, Any

# Importes diferidos para evitar ciclos y para que el módulo cargue rápido
# (solo se importan cuando se llaman las funciones).
ValidFiltro = Literal["diario", "semanal", "mensual", "historico"]

def _validate_filtro(filtro: str) -> ValidFiltro:
    valid = {"diario", "semanal", "mensual", "historico"}
    if filtro not in valid:
        raise ValueError(f"filtro inválido: '{filtro}'. Debe ser uno de {sorted(valid)}.")
    return filtro  # type: ignore[return-value]

def get_raw_sprints(headers: Dict[str, str], filtro: ValidFiltro = "diario") -> pd.DataFrame:
    """
    Devuelve sprints crudos desde Jira (por boards/sprints segun filtro).
    """
    from funciones.sprints import get_sprints  # import diferido
    try:
        _validate_filtro(filtro)
        df = get_sprints(headers, filtro)
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    except Exception as e:
        print(f"[extractor] Error get_raw_sprints({filtro}): {e}")
        return pd.DataFrame()

def get_raw_tickets(headers: Dict[str, str], filtro: ValidFiltro = "diario") -> pd.DataFrame:
    """
    Devuelve tickets crudos desde Jira por sprint (incluye epic_key/epic_name
    si tu funciones.tickets.get_tickets ya los incorpora).
    """
    from funciones.tickets import get_tickets  # import diferido
    try:
        _validate_filtro(filtro)
        df = get_tickets(headers, filtro)
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    except Exception as e:
        print(f"[extractor] Error get_raw_tickets({filtro}): {e}")
        return pd.DataFrame()

def get_raw_projects(headers: Dict[str, str], filtro: ValidFiltro = "diario") -> pd.DataFrame:
    """
    Devuelve proyectos/boards crudos desde Jira.
    """
    from funciones.projects import get_projects  # import diferido
    try:
        _validate_filtro(filtro)
        df = get_projects(headers, filtro)
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    except Exception as e:
        print(f"[extractor] Error get_raw_projects({filtro}): {e}")
        return pd.DataFrame()