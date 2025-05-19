# etl/extractor.py
import requests
import pandas as pd
from funciones.projects import get_projects

def get_raw_sprints(headers, filtro):
    from funciones.sprints import get_sprints
    return get_sprints(headers, filtro)

def get_raw_tickets(headers, filtro):
    from funciones.tickets import get_tickets
    return get_tickets(headers, filtro)

def get_raw_projects(headers, filtro):
    return get_projects(headers, filtro)
