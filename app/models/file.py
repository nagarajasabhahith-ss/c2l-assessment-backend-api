import uuid
import enum
from datetime import datetime
from sqlalchemy import Column, String, DateTime, Integer, Enum as SQLEnum, ForeignKey, Uuid
from sqlalchemy.orm import relationship
from app.db.session import Base


class FileType(str, enum.Enum):
    ZIP = "zip"
    XML = "xml"
    JSON = "json"


class ParseStatus(str, enum.Enum):
    PENDING = "pending"
    PARSING = "parsing"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"


class UploadedFile(Base):
    __tablename__ = "uploaded_files"

    id = Column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    assessment_id = Column(Uuid(as_uuid=True), ForeignKey("assessments.id"), nullable=False)
    filename = Column(String, nullable=False)
    file_path = Column(String, nullable=False)
    file_type = Column(SQLEnum(FileType), nullable=False)
    file_size = Column(Integer, nullable=False)  # in bytes
    parse_status = Column(SQLEnum(ParseStatus), default=ParseStatus.PENDING)
    uploaded_at = Column(DateTime, default=datetime.utcnow)
    parsed_at = Column(DateTime, nullable=True)

    # Relationships
    assessment = relationship("Assessment", back_populates="files")
    objects = relationship("ExtractedObject", back_populates="file", cascade="all, delete-orphan")
    errors = relationship("ParseError", back_populates="file", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<UploadedFile {self.filename} ({self.parse_status})>"
