from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional, Dict, Any
import uuid
from pydantic import BaseModel, ConfigDict

from app.db.session import get_db
from app.models.user import User
from app.models.assessment import Assessment
from app.models.file import UploadedFile, ParseStatus
from app.models.object import ExtractedObject, ObjectRelationship
from app.models.error import ParseError
from app.schemas.object import ExtractedObjectResponse, ExtractedObjectDetail, ObjectRelationshipResponse
from app.api.auth import get_current_user


class AssessmentStatsResponse(BaseModel):
    total_objects: int
    total_relationships: int
    total_files: int
    total_errors: int
    objects_by_type: Dict[str, int]
    relationships_by_type: Dict[str, int]
    parse_success_rate: float
    
    model_config = ConfigDict(from_attributes=True)


router = APIRouter()

@router.get("/assessments/{assessment_id}/objects", response_model=List[ExtractedObjectResponse])
async def list_objects(
    assessment_id: str,
    object_type: Optional[str] = None,
    search: Optional[str] = None,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=1000),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List extracted objects for an assessment"""
    try:
        assessment_uuid = uuid.UUID(assessment_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid assessment ID format")
    
    assessment = db.query(Assessment).filter(Assessment.id == assessment_uuid).first()
    
    if not assessment:
        raise HTTPException(status_code=404, detail="Assessment not found")
        
    query = db.query(ExtractedObject).filter(
        ExtractedObject.assessment_id == assessment_uuid
    )
    
    if object_type:
        query = query.filter(ExtractedObject.object_type == object_type)
        
    if search:
        query = query.filter(ExtractedObject.name.ilike(f"%{search}%"))
        
    return query.offset(skip).limit(limit).all()

@router.get("/assessments/{assessment_id}/objects/{object_id}", response_model=ExtractedObjectDetail)
async def get_object(
    assessment_id: str,
    object_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get detailed information about a specific object"""
    try:
        assessment_uuid = uuid.UUID(assessment_id)
        object_uuid = uuid.UUID(object_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid ID format")
        
    assessment = db.query(Assessment).filter(Assessment.id == assessment_uuid).first()
    
    if not assessment:
        raise HTTPException(status_code=404, detail="Assessment not found")
    
    obj = db.query(ExtractedObject).filter(
        ExtractedObject.id == object_uuid,
        ExtractedObject.assessment_id == assessment_uuid
    ).first()
    
    if not obj:
        raise HTTPException(status_code=404, detail="Object not found")
        
    return obj

@router.get("/assessments/{assessment_id}/relationships", response_model=List[ObjectRelationshipResponse])
async def list_relationships(
    assessment_id: str,
    relationship_type: Optional[str] = None,
    source_id: Optional[str] = None,
    target_id: Optional[str] = None,
    limit: int = Query(1000, ge=1, le=5000),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List relationships for dependency graph"""
    try:
        assessment_uuid = uuid.UUID(assessment_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid assessment ID format")
        
    assessment = db.query(Assessment).filter(Assessment.id == assessment_uuid).first()
    
    if not assessment:
        raise HTTPException(status_code=404, detail="Assessment not found")
        
    query = db.query(ObjectRelationship).filter(
        ObjectRelationship.assessment_id == assessment_uuid
    )
    
    if relationship_type:
        query = query.filter(ObjectRelationship.relationship_type == relationship_type)
        
    if source_id:
        try:
            source_uuid = uuid.UUID(source_id)
            query = query.filter(ObjectRelationship.source_object_id == source_uuid)
        except ValueError:
            pass
            
    if target_id:
        try:
            target_uuid = uuid.UUID(target_id)
            query = query.filter(ObjectRelationship.target_object_id == target_uuid)
        except ValueError:
            pass
            
    return query.limit(limit).all()


@router.get("/assessments/{assessment_id}/stats", response_model=AssessmentStatsResponse)
async def get_assessment_stats(
    assessment_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get summary statistics for an assessment"""
    try:
        assessment_uuid = uuid.UUID(assessment_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid assessment ID format")
    
    assessment = db.query(Assessment).filter(Assessment.id == assessment_uuid).first()
    
    if not assessment:
        raise HTTPException(status_code=404, detail="Assessment not found")
    
    # Total objects
    total_objects = db.query(func.count(ExtractedObject.id)).filter(
        ExtractedObject.assessment_id == assessment_uuid
    ).scalar() or 0
    
    # Total relationships
    total_relationships = db.query(func.count(ObjectRelationship.id)).filter(
        ObjectRelationship.assessment_id == assessment_uuid
    ).scalar() or 0
    
    # Total files
    total_files = db.query(func.count(UploadedFile.id)).filter(
        UploadedFile.assessment_id == assessment_uuid
    ).scalar() or 0
    
    # Get file IDs for error counting
    file_ids = db.query(UploadedFile.id).filter(
        UploadedFile.assessment_id == assessment_uuid
    ).subquery()
    
    # Total errors
    total_errors = db.query(func.count(ParseError.id)).filter(
        ParseError.file_id.in_(file_ids)
    ).scalar() or 0
    
    # Objects by type
    objects_by_type_query = db.query(
        ExtractedObject.object_type,
        func.count(ExtractedObject.id)
    ).filter(
        ExtractedObject.assessment_id == assessment_uuid
    ).group_by(ExtractedObject.object_type).all()
    
    objects_by_type = {obj_type: count for obj_type, count in objects_by_type_query}
    
    # Relationships by type
    relationships_by_type_query = db.query(
        ObjectRelationship.relationship_type,
        func.count(ObjectRelationship.id)
    ).filter(
        ObjectRelationship.assessment_id == assessment_uuid
    ).group_by(ObjectRelationship.relationship_type).all()
    
    relationships_by_type = {rel_type: count for rel_type, count in relationships_by_type_query}
    
    # Parse success rate
    if total_files > 0:
        completed_files = db.query(func.count(UploadedFile.id)).filter(
            UploadedFile.assessment_id == assessment_uuid,
            UploadedFile.parse_status == ParseStatus.COMPLETED
        ).scalar() or 0
        parse_success_rate = (completed_files / total_files) * 100
    else:
        parse_success_rate = 0.0
    
    return AssessmentStatsResponse(
        total_objects=total_objects,
        total_relationships=total_relationships,
        total_files=total_files,
        total_errors=total_errors,
        objects_by_type=objects_by_type,
        relationships_by_type=relationships_by_type,
        parse_success_rate=round(parse_success_rate, 1)
    )
