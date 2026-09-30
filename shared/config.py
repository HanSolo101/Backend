"""Central configuration for the Glynac Backend Ingestion Platform."""

from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables with sensible defaults."""

    # --- MinIO Configuration ---
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin123"
    MINIO_SECURE: bool = False

    # MinIO Bucket Names
    MINIO_SALESFORCE_BUCKET: str = "salesforce"
    MINIO_HUBSPOT_BUCKET: str = "hubspot"
    MINIO_SLACK_BUCKET: str = "slack"
    MINIO_CHECKPOINT_BUCKET: str = "checkpoints"

    # --- ClickHouse Configuration ---
    CLICKHOUSE_HOST: str = "localhost"
    CLICKHOUSE_PORT: int = 8123
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = "clickhouse123"
    CLICKHOUSE_DATABASE: str = "glynac"

    # --- Mock Server Configuration ---
    MOCK_SALESFORCE_URL: str = "http://localhost:8100"
    MOCK_HUBSPOT_URL: str = "http://localhost:8200"
    MOCK_SLACK_URL: str = "http://localhost:8300"
    MOCK_SLACK_WS_URL: str = "ws://localhost:8301"

    # --- Ingestion Defaults ---
    DEFAULT_BATCH_SIZE: int = 1000
    DEFAULT_MAX_RETRIES: int = 5
    DEFAULT_RETRY_BASE_DELAY: float = 1.0
    DEFAULT_PARALLEL_WORKERS: int = 4

    # --- API Server ---
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000

    # --- Organisation ---
    DEFAULT_ORG_ID: str = "org_glynac_001"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    """Return cached settings singleton."""
    return Settings()
