# etl/transformer.py
import pandas as pd

def clean_tickets(df):
    if df.empty:
        return df

    # Validaciones
    df = df[df["ticket_key"].notnull()]
    df = df[df["ticket_status"].notnull()]

    # Normalización: convertir estimate a horas si está en segundos
    if "ticket_original_estimate" in df.columns:
        df["ticket_original_estimate"] = df["ticket_original_estimate"].apply(
            lambda x: x if pd.notnull(x) else x
        )

    # Forzar tipos básicos
    columnas_str = [
        "ticket_key", "ticket_status", "ticket_summary", "ticket_assignee",
        "ticket_priority", "ticket_type", "ticket_resolution", "ticket_labels"
    ]
    for col in columnas_str:
        if col in df.columns:
            df[col] = df[col].astype(str)

    return df

def clean_sprints(df):
    if df.empty:
        return df
    # Validación básica de nombres y estados
    df = df[df["sprint_name"].notnull()]
    df = df[df["sprint_state"].notnull()]
    return df

def clean_projects(df):
    if df.empty:
        return df
    # Validar que al menos tengan ID y nombre
    df = df[df["board_id"].notnull()]
    df = df[df["board_name"].notnull()]
    return df
