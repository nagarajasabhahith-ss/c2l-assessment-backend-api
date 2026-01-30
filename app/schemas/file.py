from pydantic import BaseModel, ConfigDict
from typing import Optional
from datetime import datetime
from uuid import UUID
from app.models.file import FileType, ParseStatus


class UploadedFileResponse(BaseModel):
    id: UUID
    assessment_id: UUID
    filename: str
    file_type: FileType
    file_size: int
    parse_status: ParseStatus
    uploaded_at: datetime
    parsed_at: Optional[datetime] = None
    
    model_config = ConfigDict(from_attributes=True)


class FileUploadResponse(BaseModel):
    files: list[UploadedFileResponse]
    total_uploaded: int
    failed: list[dict] = []
