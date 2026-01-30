import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, Text, ForeignKey, Uuid
from sqlalchemy.orm import relationship
from app.db.session import Base


class ParseError(Base):
    __tablename__ = "parse_errors"

    id = Column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    file_id = Column(Uuid(as_uuid=True), ForeignKey("uploaded_files.id"), nullable=False)
    
    error_type = Column(String, nullable=False)  # xml_parse, validation, missing_field, etc.
    error_message = Column(Text, nullable=False)
    location = Column(String, nullable=True)  # xpath, line number, etc.
    context = Column(Text, nullable=True)  # Additional context
    
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    file = relationship("UploadedFile", back_populates="errors")

    def __repr__(self):
        return f"<ParseError {self.error_type}: {self.error_message[:50]}>"
