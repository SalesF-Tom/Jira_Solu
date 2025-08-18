# utils/config.py
from pydantic import BaseSettings, AnyHttpUrl, Field
from functools import lru_cache

class Settings(BaseSettings):
    # GCP/BQ
    PROJECT_ID: str
    BQ_LOCATION: str = "US"
    BQ_DATASET_RAW: str
    BQ_DATASET_TEMP: str
    BQ_DATASET_SILVER: str
    BQ_DATASET_GOLD: str
    BQ_DATASET_META: str

    # Jira
    JIRA_BASE_URL: AnyHttpUrl
    JIRA_EMAIL: str
    JIRA_API_TOKEN: str

    # Obs
    DISCORD_WEBHOOK: AnyHttpUrl | None = None
    LOG_LEVEL: str = Field(default="INFO", pattern="^(DEBUG|INFO|WARN|ERROR)$")

    # Env
    ENV: str = Field(default="local", pattern="^(local|dev|prod)$")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

@lru_cache
def get_settings() -> Settings:
    return Settings()  # levanta del .env o variables del sistema