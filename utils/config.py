# utils/config.py
import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Carga .env si existe (no es obligatorio)
load_dotenv(override=False)

@dataclass(frozen=True)
class Settings:
    # === GCP / BigQuery ===
    PROJECT_ID: str = os.getenv("PROJECT_ID", "data-warehouse-311917")
    BQ_LOCATION: str = os.getenv("BQ_LOCATION", "US")

    # === Jira (si aplica en este repo) ===
    JIRA_BASE_URL: str = os.getenv("JIRA_BASE_URL", "")
    JIRA_EMAIL: str = os.getenv("JIRA_EMAIL", "")
    JIRA_API_TOKEN: str = os.getenv("JIRA_API_TOKEN", "")

    # === Observabilidad (opcional) ===
    DISCORD_WEBHOOK: str = os.getenv("DISCORD_WEBHOOK", "")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # === Credenciales GCP (opcional, SOLO si tu IT las usa con JSON) ===
    GOOGLE_APPLICATION_CREDENTIALS: str = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

_settings = Settings()

def get_settings() -> Settings:
    """Acceso centralizado a la config (mantiene firma simple)."""
    return _settings