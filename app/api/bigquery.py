"""
BigQuery connection status and optional query endpoints.
"""

import os
from fastapi import APIRouter, Depends, HTTPException
from app.config import settings
from app.db.bigquery import get_bigquery_client, require_bigquery

router = APIRouter()


@router.get("/bigquery/status")
def bigquery_status():
    """
    Return BigQuery configuration and connection status.
    Does not run a query; use this to verify project/credentials are set.
    """
    return {
        "enabled": settings.bigquery_enabled,
        "project_id": settings.BIGQUERY_PROJECT_ID or None,
        "location": settings.BIGQUERY_LOCATION,
        "credentials_configured": bool(
            settings.BIGQUERY_CREDENTIALS_PATH
            or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        ),
    }


@router.get("/bigquery/connect")
def bigquery_connect(client=Depends(get_bigquery_client)):
    """
    Test BigQuery connectivity by creating a client and optionally running a dry run.
    Returns 200 if BigQuery is configured and client can be created.
    """
    if not settings.bigquery_enabled:
        raise HTTPException(
            status_code=503,
            detail="BigQuery is not configured. Set BIGQUERY_PROJECT_ID in .env",
        )
    if client is None:
        raise HTTPException(
            status_code=503,
            detail="BigQuery client could not be created. Check BIGQUERY_CREDENTIALS_PATH or GOOGLE_APPLICATION_CREDENTIALS.",
        )
    return {
        "status": "connected",
        "project": client.project,
        "location": getattr(client, "location", None) or settings.BIGQUERY_LOCATION,
    }


@router.get("/example")
def example(client = Depends(require_bigquery)):
    
    # Visualization_Type: feature list for Visualization feature_area
    job = client.query(
        "SELECT * FROM `tableau-to-looker-migration.C2L_Complexity_Rules.Feature_List_Looker_Perspective` "
        "LIMIT 1000"
    )
    # job = client.query("SELECT * FROM `tableau-to-looker-migration.C2L_Complexity_analysis.Complexity_Analysis_List` LIMIT 1000")


    return list(job.result())