import os
from typing import List, Union
from pydantic_settings import BaseSettings
from pydantic import field_validator


class Settings(BaseSettings):
    # App Info
    APP_NAME: str = "C2L Assessment API"
    VERSION: str = "1.0.0"
    ENVIRONMENT: str = "development"
    
    # Database
    DATABASE_URL: str
    
    # Security
    SECRET_KEY: str
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 43200  # 30 days
    
    # Google OAuth
    GOOGLE_CLIENT_ID: str = ""
    GOOGLE_CLIENT_SECRET: str = ""
    GOOGLE_REDIRECT_URI: str = ""
    
    # CORS
    CORS_ORIGINS: Union[str, List[str]] = "http://localhost:3000"
    
    @field_validator('CORS_ORIGINS', mode='before')
    @classmethod
    def parse_cors_origins(cls, v):
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(',')]
        return v
    
    # File Upload
    UPLOAD_DIR: str = "./temp/uploads"
    MAX_UPLOAD_SIZE_MB: int = 100
    ALLOWED_EXTENSIONS: List[str] = [".zip", ".xml", ".json"]

    # Google BigQuery
    BIGQUERY_PROJECT_ID: str = ""
    BIGQUERY_CREDENTIALS_PATH: str = ""  # Path to service account JSON; or set GOOGLE_APPLICATION_CREDENTIALS
    BIGQUERY_LOCATION: str = "US"  # Default location for jobs (e.g. US, EU)
    # Optional: fully qualified table for complexity/feature lookup used by report service (dataset.table or project.dataset.table)
    BIGQUERY_FEATURE_TABLE: str = ""

    # Google Cloud Storage (optional – uploads go to GCS when set)
    GCS_BUCKET: str = ""
    GCS_PREFIX: str = "uploads"  # Object key prefix, e.g. uploads/assessment_id/file_id.zip
    GCS_CREDENTIALS_PATH: str = ""  # Path to service account JSON; or set GOOGLE_APPLICATION_CREDENTIALS
    
    class Config:
        env_file = ".env"
        case_sensitive = True
        
    @property
    def max_upload_size_bytes(self) -> int:
        return self.MAX_UPLOAD_SIZE_MB * 1024 * 1024

    @property
    def bigquery_enabled(self) -> bool:
        """True if BigQuery is configured (project ID set). Credentials via BIGQUERY_CREDENTIALS_PATH or GOOGLE_APPLICATION_CREDENTIALS."""
        return bool(self.BIGQUERY_PROJECT_ID)

    @property
    def gcs_enabled(self) -> bool:
        """True if uploads should go to Google Cloud Storage (GCS_BUCKET set)."""
        return bool(self.GCS_BUCKET)


# Create settings instance
settings = Settings()

# Ensure upload directory exists
os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
