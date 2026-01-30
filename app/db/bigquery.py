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


def get_bigquery_client() -> Optional[bigquery.Client]:
    """
    Create and return a BigQuery client, or None if BigQuery is not configured.
    """
    if not settings.BIGQUERY_PROJECT_ID:
        return None
    if settings.BIGQUERY_CREDENTIALS_PATH:
        client = bigquery.Client.from_service_account_json(
            settings.BIGQUERY_CREDENTIALS_PATH,
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
