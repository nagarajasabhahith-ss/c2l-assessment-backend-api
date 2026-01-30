"""
Service for conducting parsing operations on assessment files.
Supports local paths and gs:// (GCS) paths via temporary download.
"""
from pathlib import Path
from sqlalchemy.orm import Session
from fastapi import HTTPException
from typing import List

from app.models.assessment import Assessment, AssessmentStatus
from app.models.file import UploadedFile
from app.services.storage_service import get_local_path
from app.models.object import ExtractedObject, ObjectRelationship
from app.db.session import get_db

# Import from our local parser library
# Note: In a real deployment, this might be an installed package
import sys

# Add project root to path to find bi_parsers
current_file = Path(__file__).resolve()
# Try different depths:
# 1. Local dev: backend/app/services/../../.. -> cognos-to-looker
# 2. Docker: /app/app/services/.. -> /app
possible_roots = [
    current_file.parent.parent.parent.parent,
    current_file.parent.parent.parent,
]

for root in possible_roots:
    if (root / "bi_parsers").exists():
        if str(root) not in sys.path:
            sys.path.append(str(root))
        break

try:
    from bi_parsers import create_parser, ParseResult
except ImportError as e:
    print(f"Warning: Could not import bi_parsers: {e}")
    # Define dummy placeholders to prevent ImportErrors during type checking
    def create_parser(*args, **kwargs):
        raise ImportError("bi_parsers not found")
    class ParseResult:
        objects = []
        relationships = []
        errors = []


class ParserService:
    def __init__(self, db: Session):
        self.db = db

    def run_assessment(self, assessment_id: str) -> Assessment:
        """
        Run parsing for all files in an assessment.
        """
        assessment = self.db.query(Assessment).filter(Assessment.id == assessment_id).first()
        if not assessment:
            raise HTTPException(status_code=404, detail="Assessment not found")

        assessment.status = AssessmentStatus.PROCESSING
        self.db.commit()

        try:
            total_objects = 0
            
            for file in assessment.files:
                # Resolve path: local file or download from GCS to temp
                try:
                    with get_local_path(file.file_path) as local_path:
                        # Determine parser type based on assessment metadata or file extension
                        tool_name = assessment.bi_tool.lower() if assessment.bi_tool else "cognos"
                        try:
                            parser = create_parser(tool_name)
                            result = parser.parse(local_path)
                            self._persist_results(result, assessment.id, file.id)
                            total_objects += len(result.objects)
                        except Exception as e:
                            print(f"Error parsing file {file.file_path}: {e}")
                            continue
                except Exception as e:
                    print(f"Error resolving/reading file {file.file_path}: {e}")
                    continue
            
            assessment.status = AssessmentStatus.COMPLETED
            self.db.commit()
            self.db.refresh(assessment)
            return assessment

        except Exception as e:
            assessment.status = AssessmentStatus.FAILED
            self.db.commit()
            raise e

    def _persist_results(self, result: ParseResult, assessment_id: str, file_id: str):
        """
        Save parsing results to the database.
        """
        # 1. Save Objects
        # Keep track of object_id maps (source_id -> db_id) for relationships
        # Cognos IDs are strings, DB IDs are UUIDs. 
        # We need to map source IDs (string) to the DB UUIDs to create relationships.
        
        # However, IDs might be duplicated across files. 
        # For this implementation, we assume objects are unique per file or we create new entries.
        
        source_id_to_db_id = {}
        
        for obj in result.objects:
            # Merge parser fields into properties so we don't lose any data.
            # Parser ExtractedObject has: object_id, object_type, name, parent_id, path,
            # properties, created_at, modified_at, owner, source_file, bi_tool.
            # We store object_type, name, path as columns; the rest go into properties.
            props = dict(obj.properties) if obj.properties else {}
            props["original_id"] = obj.object_id
            if obj.parent_id is not None:
                props["parent_id"] = obj.parent_id
            if obj.created_at is not None:
                props["created_at"] = obj.created_at.isoformat() if hasattr(obj.created_at, "isoformat") else str(obj.created_at)
            if obj.modified_at is not None:
                props["modified_at"] = obj.modified_at.isoformat() if hasattr(obj.modified_at, "isoformat") else str(obj.modified_at)
            if obj.owner is not None:
                props["owner"] = obj.owner
            if obj.source_file is not None:
                props["source_file"] = obj.source_file
            if getattr(obj, "bi_tool", None):
                props["bi_tool"] = obj.bi_tool

            db_obj = ExtractedObject(
                assessment_id=assessment_id,
                file_id=file_id,
                object_type=obj.object_type,
                name=obj.name,
                path=obj.path,
                properties=props,
            )

            self.db.add(db_obj)
            self.db.flush() # Flush to get UUID
            
            source_id_to_db_id[obj.object_id] = db_obj.id

        # 2. Save Relationships
        for rel in result.relationships:
            source_db_id = source_id_to_db_id.get(rel.source_id)
            target_db_id = source_id_to_db_id.get(rel.target_id)
            
            if source_db_id and target_db_id:
                db_rel = ObjectRelationship(
                    assessment_id=assessment_id,
                    source_object_id=source_db_id,
                    target_object_id=target_db_id,
                    relationship_type=rel.relationship_type,
                    details=rel.properties
                )
                self.db.add(db_rel)
                
            # Note: references to objects NOT in this file (e.g. cross-file dependencies)
            # are currently dropped. A global resolver step would be needed for full linkage.

        self.db.commit()
