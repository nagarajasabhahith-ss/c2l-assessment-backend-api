from pydantic import BaseModel, ConfigDict
from typing import Optional, Any
from uuid import UUID
from datetime import datetime

class ObjectRelationshipResponse(BaseModel):
    id: UUID
    assessment_id: UUID
    source_object_id: UUID
    target_object_id: UUID
    relationship_type: str
    details: Optional[Any] = None
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)

class ExtractedObjectBase(BaseModel):
    object_type: str
    name: str
    path: Optional[str] = None
    properties: Optional[Any] = None

class ExtractedObjectResponse(ExtractedObjectBase):
    id: UUID
    assessment_id: UUID
    file_id: UUID
    created_at: datetime
    
    # We might want to include relationships counts or IDs here, but keeping it simple for list view
    
    model_config = ConfigDict(from_attributes=True)

class ExtractedObjectDetail(ExtractedObjectResponse):
    raw_xml: Optional[str] = None
    # outgoing_relationships: list[ObjectRelationshipResponse]
    # incoming_relationships: list[ObjectRelationshipResponse]
