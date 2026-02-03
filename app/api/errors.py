from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional
import uuid

from app.db.session import get_db
from app.models.user import User
from app.models.assessment import Assessment
from app.models.file import UploadedFile
from app.models.error import ParseError
from app.api.auth import get_current_user
from pydantic import BaseModel, ConfigDict
from datetime import datetime
from uuid import UUID


router = APIRouter()


class ParseErrorResponse(BaseModel):
    id: UUID
    file_id: UUID
    filename: str
    error_type: str
    error_message: str
    location: Optional[str] = None
    context: Optional[str] = None
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)


@router.get("/assessments/{assessment_id}/errors", response_model=List[ParseErrorResponse])
async def list_errors(
    assessment_id: str,
    error_type: Optional[str] = None,
    file_id: Optional[str] = None,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List parse errors for an assessment"""
    try:
        assessment_uuid = uuid.UUID(assessment_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid assessment ID format")
    
    assessment = db.query(Assessment).filter(Assessment.id == assessment_uuid).first()
    
    if not assessment:
        raise HTTPException(status_code=404, detail="Assessment not found")
    
    # Get all file IDs for this assessment
    file_ids = db.query(UploadedFile.id).filter(
        UploadedFile.assessment_id == assessment_uuid
    ).subquery()
    
    # Query errors for those files
    query = db.query(ParseError, UploadedFile.filename).join(
        UploadedFile, ParseError.file_id == UploadedFile.id
    ).filter(
        ParseError.file_id.in_(file_ids)
    )
    
    if error_type:
        query = query.filter(ParseError.error_type == error_type)
    
    if file_id:
        try:
            file_uuid = uuid.UUID(file_id)
            query = query.filter(ParseError.file_id == file_uuid)
        except ValueError:
            pass
    
    results = query.offset(skip).limit(limit).all()
    
    # Transform results to include filename
    response = []
    for error, filename in results:
        error_dict = {
            "id": error.id,
            "file_id": error.file_id,
            "filename": filename,
            "error_type": error.error_type,
            "error_message": error.error_message,
            "location": error.location,
            "context": error.context,
            "created_at": error.created_at
        }
        response.append(ParseErrorResponse(**error_dict))
    
    return response
