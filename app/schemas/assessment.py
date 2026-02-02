from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Any
from datetime import datetime
from uuid import UUID
from app.models.assessment import AssessmentStatus


class AssessmentBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    bi_tool: str = Field(default="cognos", pattern="^(cognos|tableau|powerbi)$")


class AssessmentCreate(AssessmentBase):
    pass


class AssessmentUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    status: Optional[AssessmentStatus] = None


class AssessmentResponse(AssessmentBase):
    id: UUID
    status: AssessmentStatus
    user_id: UUID
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime] = None
    # Optional usage stats from usage_stats.json upload (usage_stats, content_creation, user_stats, performance, quick_wins, pilot_recommendations)
    usage_stats: Optional[dict[str, Any]] = None

    # Counts (computed)
    files_count: int = 0
    objects_count: int = 0
    relationships_count: int = 0

    model_config = ConfigDict(from_attributes=True)


class AssessmentListResponse(BaseModel):
    assessments: list[AssessmentResponse]
    total: int
    page: int
    page_size: int
    total_pages: int
