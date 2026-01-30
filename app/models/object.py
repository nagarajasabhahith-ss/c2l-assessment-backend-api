import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, Text, ForeignKey, Index, JSON, Uuid, Float, Integer
from sqlalchemy.orm import relationship
from app.db.session import Base


class ExtractedObject(Base):
    __tablename__ = "extracted_objects"

    id = Column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    assessment_id = Column(Uuid(as_uuid=True), ForeignKey("assessments.id"), nullable=False)
    file_id = Column(Uuid(as_uuid=True), ForeignKey("uploaded_files.id"), nullable=False)
    
    object_type = Column(String, nullable=False, index=True)  # report, dashboard, data_module, etc.
    name = Column(String, nullable=False, index=True)
    path = Column(String, nullable=True)  # folder path in Cognos
    
    properties = Column(JSON, nullable=True)  # All extracted properties
    raw_xml = Column(Text, nullable=True)  # Original XML for reference
    
    # Complexity fields
    complexity_score_looker = Column(Float, nullable=True)
    complexity_level_looker = Column(String(20), nullable=True)
    complexity_score_custom = Column(Float, nullable=True)
    complexity_level_custom = Column(String(20), nullable=True)
    hierarchy_depth = Column(Integer, nullable=True)
    hierarchy_level = Column(Integer, nullable=True)
    hierarchy_path = Column(Text, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    assessment = relationship("Assessment", back_populates="objects")
    file = relationship("UploadedFile", back_populates="objects")
    
    # Relationships as source or target
    outgoing_relationships = relationship(
        "ObjectRelationship", 
        foreign_keys="ObjectRelationship.source_object_id",
        back_populates="source_object",
        cascade="all, delete-orphan"
    )
    incoming_relationships = relationship(
        "ObjectRelationship", 
        foreign_keys="ObjectRelationship.target_object_id",
        back_populates="target_object",
        cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index('ix_extracted_objects_assessment_type', 'assessment_id', 'object_type'),
        Index('ix_extracted_objects_name_search', 'name'),
    )

    def __repr__(self):
        return f"<ExtractedObject {self.object_type}: {self.name}>"



class ObjectRelationship(Base):
    __tablename__ = "object_relationships"

    id = Column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    assessment_id = Column(Uuid(as_uuid=True), ForeignKey("assessments.id"), nullable=False)
    
    source_object_id = Column(Uuid(as_uuid=True), ForeignKey("extracted_objects.id"), nullable=False)
    target_object_id = Column(Uuid(as_uuid=True), ForeignKey("extracted_objects.id"), nullable=False)
    
    relationship_type = Column(String, nullable=False)  # uses, references, contains, etc.
    details = Column(JSON, nullable=True)  # Additional relationship metadata
    
    # Complexity fields for relationships
    complexity_score = Column(Float, nullable=True)
    complexity_level = Column(String(20), nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    assessment = relationship("Assessment", back_populates="relationships")
    source_object = relationship(
        "ExtractedObject", 
        foreign_keys=[source_object_id],
        back_populates="outgoing_relationships"
    )
    target_object = relationship(
        "ExtractedObject", 
        foreign_keys=[target_object_id],
        back_populates="incoming_relationships"
    )

    __table_args__ = (
        Index('ix_relationships_assessment', 'assessment_id'),
        Index('ix_relationships_source', 'source_object_id'),
        Index('ix_relationships_target', 'target_object_id'),
    )

    def __repr__(self):
        return f"<Relationship {self.relationship_type}>"
