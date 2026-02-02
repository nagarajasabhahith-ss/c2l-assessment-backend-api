import uuid
import enum
from datetime import datetime
from sqlalchemy import Column, String, DateTime, Enum as SQLEnum, ForeignKey, Uuid, JSON
from sqlalchemy.orm import relationship
from app.db.session import Base


class AssessmentStatus(str, enum.Enum):
    CREATED = "created"
    PROCESSING = "processing"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"


class Assessment(Base):
    __tablename__ = "assessments"

    id = Column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    bi_tool = Column(String, default="cognos")  # cognos, tableau, powerbi
    status = Column(SQLEnum(AssessmentStatus), default=AssessmentStatus.CREATED)
    user_id = Column(Uuid(as_uuid=True), ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    # Optional usage stats JSON (uploaded as usage_stats.json); structure: usage_stats, content_creation, user_stats, performance, quick_wins, pilot_recommendations
    usage_stats = Column(JSON, nullable=True)

    # Relationships
    files = relationship("UploadedFile", back_populates="assessment", cascade="all, delete-orphan")
    objects = relationship("ExtractedObject", back_populates="assessment", cascade="all, delete-orphan")
    relationships = relationship("ObjectRelationship", back_populates="assessment", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Assessment {self.name} ({self.status})>"
