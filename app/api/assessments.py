from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from sqlalchemy.orm import Session, selectinload
from typing import Optional
import uuid
import logging

from app.db.session import get_db

logger = logging.getLogger(__name__)
from app.models.user import User
from app.models.assessment import Assessment, AssessmentStatus
from app.models.error import ParseError
from app.schemas.assessment import (
    AssessmentCreate,
    AssessmentUpdate,
    AssessmentResponse,
    AssessmentListResponse,
)
from app.schemas.report import AssessmentReportResponse
from app.api.auth import get_current_user
from app.services.parser_service import ParserService
from app.services.report_service import ReportService
from app.models.object import ExtractedObject, ObjectRelationship

router = APIRouter()


@router.post("", response_model=AssessmentResponse, status_code=status.HTTP_201_CREATED)
async def create_assessment(
    assessment_data: AssessmentCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Create a new assessment"""
    assessment = Assessment(
        name=assessment_data.name,
        bi_tool=assessment_data.bi_tool,
        user_id=current_user.id,
        status=AssessmentStatus.CREATED
    )
    
    db.add(assessment)
    db.commit()
    db.refresh(assessment)
    
    # Add counts
    response = AssessmentResponse.from_orm(assessment)
    response.files_count = 0
    response.objects_count = 0
    response.relationships_count = 0
    
    return response


@router.get("", response_model=AssessmentListResponse)
async def list_assessments(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status_filter: Optional[AssessmentStatus] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List all assessments (not restricted by user)"""
    query = db.query(Assessment)
    
    # Apply filters
    if status_filter:
        query = query.filter(Assessment.status == status_filter)
    
    # Get total count
    total = query.count()
    
    # Pagination
    offset = (page - 1) * page_size
    assessments = query.order_by(Assessment.created_at.desc()).offset(offset).limit(page_size).all()
    
    # Add counts for each assessment
    assessment_responses = []
    for assessment in assessments:
        response = AssessmentResponse.from_orm(assessment)
        response.files_count = len(assessment.files)
        response.objects_count = len(assessment.objects)
        response.relationships_count = len(assessment.relationships)
        assessment_responses.append(response)
    
    return AssessmentListResponse(
        assessments=assessment_responses,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=(total + page_size - 1) // page_size
    )


@router.get("/{assessment_id}", response_model=AssessmentResponse)
async def get_assessment(
    assessment_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get a specific assessment"""
    try:
        assessment_uuid = uuid.UUID(assessment_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid assessment ID format")
    
    assessment = db.query(Assessment).filter(
        Assessment.id == assessment_uuid,
        Assessment.user_id == current_user.id
    ).first()
    
    if not assessment:
        raise HTTPException(status_code=404, detail="Assessment not found")
    
    response = AssessmentResponse.from_orm(assessment)
    response.files_count = len(assessment.files)
    response.objects_count = len(assessment.objects)
    response.relationships_count = len(assessment.relationships)
    
    return response


@router.get("/{assessment_id}/report", response_model=AssessmentReportResponse)
async def get_assessment_report(
    assessment_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get the report for an assessment (visualization details, etc.). All data from API; no client-side computation."""
    try:
        assessment_uuid = uuid.UUID(assessment_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid assessment ID format")

    assessment = (
        db.query(Assessment)
        .filter(
            Assessment.id == assessment_uuid,
            # Assessment.user_id == current_user.id,
        )
        .options(
            selectinload(Assessment.objects),
            selectinload(Assessment.relationships),
        )
        .first()
    )

    if not assessment:
        raise HTTPException(status_code=404, detail="Assessment not found")

    report_service = ReportService(db)
    report = report_service.generate_report_for_assessment(assessment)
    return AssessmentReportResponse(**report)


@router.patch("/{assessment_id}", response_model=AssessmentResponse)
async def update_assessment(
    assessment_id: str,
    update_data: AssessmentUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update an assessment"""
    try:
        assessment_uuid = uuid.UUID(assessment_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid assessment ID format")
    
    assessment = db.query(Assessment).filter(
        Assessment.id == assessment_uuid,
        # Assessment.user_id == current_user.id
    ).first()
    
    if not assessment:
        raise HTTPException(status_code=404, detail="Assessment not found")
    
    # Update fields
    if update_data.name is not None:
        assessment.name = update_data.name
    if update_data.status is not None:
        assessment.status = update_data.status
    
    db.commit()
    db.refresh(assessment)
    
    response = AssessmentResponse.from_orm(assessment)
    response.files_count = len(assessment.files)
    response.objects_count = len(assessment.objects)
    response.relationships_count = len(assessment.relationships)
    
    return response


@router.delete("/{assessment_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_assessment(
    assessment_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Delete an assessment and all related data"""
    try:
        assessment_uuid = uuid.UUID(assessment_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid assessment ID format")
    
    assessment = db.query(Assessment).filter(
        Assessment.id == assessment_uuid,
        Assessment.user_id == current_user.id
    ).first()
    
    if not assessment:
        raise HTTPException(status_code=404, detail="Assessment not found")
    
    # Cascade delete will handle related records
    db.delete(assessment)
    db.commit()
    
    return None


@router.post("/{assessment_id}/run", response_model=AssessmentResponse)
async def run_assessment_analysis(
    assessment_id: str,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Trigger analysis for an assessment"""
    try:
        assessment_uuid = uuid.UUID(assessment_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid assessment ID format")
    
    assessment = db.query(Assessment).filter(
        Assessment.id == assessment_uuid,
        # Assessment.user_id == current_user.id
    ).first()
    
    if not assessment:
        raise HTTPException(status_code=404, detail="Assessment not found")
    
    if assessment.status == AssessmentStatus.PROCESSING:
        raise HTTPException(status_code=400, detail="Assessment is already in progress")
    
    # Initialize service
    parser_service = ParserService(db)
    
    # Run in background to avoid blocking
    # Using background_tasks.add_task(parser_service.run_assessment, assessment_id)
    # However, for this demo/MVP, running synchronously might be easier to debug, 
    # but let's stick to background for correctness.
    # Note: run_assessment grabs the DB session which might be closed if not handled carefully in background tasks 
    # with `Depends(get_db)`.
    # A cleaner way for background tasks is to create a new session or pass the ID only.
    # Given the simplicity, we'll run it synchronously for now to ensure atomic completion 
    # and immediate feedback for this CLI-driven interaction.
    # In production -> Redis Queue (Celery/RQ)
    
    updated_assessment = parser_service.run_assessment(str(assessment_uuid))

    # Generate the report for the assessment (all parsed objects, one by one)
    report_service = ReportService(db)
    report_service.generate_report_for_assessment(updated_assessment)

    return AssessmentResponse.from_orm(updated_assessment)