"""
Google BigQuery client and FastAPI dependency.

Configure via environment or .env:
  - BIGQUERY_PROJECT_ID (required)
  - BIGQUERY_CREDENTIALS_PATH or GOOGLE_APPLICATION_CREDENTIALS (path to service account JSON)
  - BIGQUERY_LOCATION (optional, default US)
"""

from typing import Generator, Optional

from app.config import settings
from google.cloud import bigquery
from google.oauth2 import service_account

# BigQuery-only scope to avoid 403 "getting Drive credentials" when SA has no Drive access
BIGQUERY_SCOPE = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive",]


def get_bigquery_client() -> Optional[bigquery.Client]:
    """
    Create and return a BigQuery client, or None if BigQuery is not configured.
    Uses BigQuery-only scope to avoid Drive/Sheets permission errors.
    """
    if not settings.BIGQUERY_PROJECT_ID:
        return None
    if settings.BIGQUERY_CREDENTIALS_PATH:
        credentials = service_account.Credentials.from_service_account_file(
            settings.BIGQUERY_CREDENTIALS_PATH,
            scopes=BIGQUERY_SCOPE,
        )
        client = bigquery.Client(
            credentials=credentials,
            project=settings.BIGQUERY_PROJECT_ID,
            location=settings.BIGQUERY_LOCATION,
        )
    else:
        client = bigquery.Client(
            project=settings.BIGQUERY_PROJECT_ID,
            location=settings.BIGQUERY_LOCATION,
        )
    return client


def get_bigquery() -> Generator[Optional[bigquery.Client], None, None]:
    """
    FastAPI dependency that yields a BigQuery client or None.
    Use in route handlers when BigQuery is optional.
    """
    yield get_bigquery_client()


def require_bigquery() -> Generator[bigquery.Client, None, None]:
    """
    FastAPI dependency that yields a BigQuery client.
    Raises if BigQuery is not configured; use for routes that require BigQuery.
    """
    client = get_bigquery_client()
    if client is None:
        raise RuntimeError(
            "BigQuery is not configured. Set BIGQUERY_PROJECT_ID and "
            "BIGQUERY_CREDENTIALS_PATH or GOOGLE_APPLICATION_CREDENTIALS."
        )
    yield client
