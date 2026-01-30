from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from sqlalchemy.orm import Session
from typing import List
import uuid
import os
from pathlib import Path

from app.db.session import get_db
from app.config import settings
from app.models.user import User
from app.models.assessment import Assessment
from app.models.file import UploadedFile, FileType, ParseStatus
from app.schemas.file import UploadedFileResponse, FileUploadResponse
from app.api.auth import get_current_user
from app.services.storage_service import upload_file as storage_upload, delete_file as storage_delete

router = APIRouter()


def get_file_type(filename: str) -> FileType:
    """Determine file type from extension"""
    ext = Path(filename).suffix.lower()
    if ext == ".zip":
        return FileType.ZIP
    elif ext == ".xml":
        return FileType.XML
    elif ext == ".json":
        return FileType.JSON
    else:
        raise ValueError(f"Unsupported file type: {ext}")


def validate_file(file: UploadFile) -> None:
    """Validate uploaded file"""
    # Check extension
    ext = Path(file.filename).suffix.lower()
    if ext not in settings.ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"File type not allowed. Supported: {', '.join(settings.ALLOWED_EXTENSIONS)}"
        )


@router.post("/assessments/{assessment_id}/files", response_model=FileUploadResponse)
async def upload_files(
    assessment_id: str,
    files: List[UploadFile] = File(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Upload one or more files to an assessment (local disk or Google Cloud Storage when GCS_BUCKET is set)."""

    # Validate assessment
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
    
    # Create assessment directory when using local storage (for GCS it's not used)
    if not settings.gcs_enabled:
        assessment_dir = Path(settings.UPLOAD_DIR) / str(assessment_id)
        assessment_dir.mkdir(parents=True, exist_ok=True)
    
    uploaded = []
    failed = []
    
    for file in files:
        try:
            # Validate file
            validate_file(file)
            
            # Check file size
            file.file.seek(0, 2)  # Seek to end
            file_size = file.file.tell()
            file.file.seek(0)  # Reset to beginning
            
            if file_size > settings.max_upload_size_bytes:
                failed.append({
                    "filename": file.filename,
                    "error": f"File size exceeds {settings.MAX_UPLOAD_SIZE_MB}MB limit"
                })
                continue
            
            # Determine file type
            file_type = get_file_type(file.filename)
            
            # Generate unique filename
            file_id = uuid.uuid4()
            ext = Path(file.filename).suffix
            content = file.file.read()
            stored_path = storage_upload(str(assessment_id), str(file_id), ext, content)
            
            # Create database record
            uploaded_file = UploadedFile(
                assessment_id=assessment_uuid,
                filename=file.filename,
                file_path=stored_path,
                file_type=file_type,
                file_size=file_size,
                parse_status=ParseStatus.PENDING
            )
            
            db.add(uploaded_file)
            db.commit()
            db.refresh(uploaded_file)
            
            uploaded.append(UploadedFileResponse.from_orm(uploaded_file))
            
        except Exception as e:
            failed.append({
                "filename": file.filename,
                "error": str(e)
            })
    
    return FileUploadResponse(
        files=uploaded,
        total_uploaded=len(uploaded),
        failed=failed
    )


@router.get("/assessments/{assessment_id}/files", response_model=List[UploadedFileResponse])
async def list_files(
    assessment_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List all files for an assessment"""
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
    
    files = db.query(UploadedFile).filter(
        UploadedFile.assessment_id == assessment_uuid
    ).order_by(UploadedFile.uploaded_at.desc()).all()
    
    return [UploadedFileResponse.from_orm(f) for f in files]


@router.delete("/files/{file_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_file(
    file_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Delete an uploaded file"""
    try:
        file_uuid = uuid.UUID(file_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid file ID format")
    
    uploaded_file = db.query(UploadedFile).filter(UploadedFile.id == file_uuid).first()
    
    if not uploaded_file:
        raise HTTPException(status_code=404, detail="File not found")
    
    # Check ownership
    assessment = db.query(Assessment).filter(
        Assessment.id == uploaded_file.assessment_id,
        Assessment.user_id == current_user.id
    ).first()
    
    if not assessment:
        raise HTTPException(status_code=404, detail="File not found")
    
    # Delete physical file (local or GCS)
    storage_delete(uploaded_file.file_path)
    
    # Delete database record (cascade will handle related data)
    db.delete(uploaded_file)
    db.commit()
    
    return None
