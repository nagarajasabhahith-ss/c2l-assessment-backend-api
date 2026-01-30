"""
Custom Mapping Loader

Loads custom complexity mappings from CSV files or Google Sheets.
Maps Cognos features to complexity levels based on external configuration.
"""
import csv
import os
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class CustomMappingLoader:
    """Load and manage custom complexity mappings from CSV files."""
    
    def __init__(self, mapping_file_path: Optional[str] = None):
        """
        Initialize the mapping loader.
        
        Args:
            mapping_file_path: Path to CSV mapping file. If None, looks for default file.
        """
        self.mapping_file_path = mapping_file_path
        self.mappings: Dict[str, Dict[str, Any]] = {}
        self._load_mappings()
    
    def _get_default_mapping_path(self) -> Optional[str]:
        """Get default mapping file path."""
        # Look in project root for the CSV file
        project_root = Path(__file__).parent.parent.parent.parent
        default_paths = [
            project_root / "Copy of Cognos Feature List and Complexity Analysis - Feature_List_Looker Perspective.csv",
            project_root / "cognos_complexity_mapping.csv",
            project_root / "mappings" / "cognos_complexity_mapping.csv",
        ]
        
        for path in default_paths:
            if path.exists():
                return str(path)
        
        return None
    
    def _load_mappings(self) -> None:
        """Load mappings from CSV file."""
        file_path = self.mapping_file_path or self._get_default_mapping_path()
        
        if not file_path or not os.path.exists(file_path):
            logger.warning(f"Mapping file not found: {file_path}")
            return
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    feature_area = row.get('Feature Area', '').strip()
                    feature = row.get('Feature', '').strip()
                    complexity = row.get('Complexity', '').strip()
                    feasibility = row.get('Feasibility', '').strip()
                    description = row.get('Description', '').strip()
                    recommended_approach = row.get('Recommended Approach', '').strip()
                    
                    if not feature:
                        continue
                    
                    # Create mapping key
                    key = self._create_mapping_key(feature_area, feature)
                    
                    # Convert complexity to score
                    complexity_score = self._complexity_to_score(complexity)
                    
                    # Adjust based on feasibility
                    if feasibility.lower() == "no":
                        complexity_score = min(complexity_score + 10, 100)
                    elif feasibility.lower() == "partial":
                        complexity_score = min(complexity_score + 5, 100)
                    
                    self.mappings[key] = {
                        "feature_area": feature_area,
                        "feature": feature,
                        "complexity": complexity,
                        "complexity_score": complexity_score,
                        "feasibility": feasibility,
                        "description": description,
                        "recommended_approach": recommended_approach,
                    }
            
            logger.info(f"Loaded {len(self.mappings)} mappings from {file_path}")
        
        except Exception as e:
            logger.error(f"Error loading mapping file {file_path}: {e}", exc_info=True)
    
    def _create_mapping_key(self, feature_area: str, feature: str) -> str:
        """
        Create a unique key for a feature mapping.
        
        Args:
            feature_area: Feature area/category
            feature: Feature name
            
        Returns:
            Normalized key string
        """
        # Normalize: lowercase, remove special chars, join with underscore
        area = feature_area.lower().replace(" ", "_").replace("-", "_")
        feat = feature.lower().replace(" ", "_").replace("-", "_")
        return f"{area}::{feat}"
    
    def _complexity_to_score(self, complexity: str) -> float:
        """
        Convert complexity level to numeric score.
        
        Args:
            complexity: Complexity level (Low, Medium, High, Critical)
            
        Returns:
            Complexity score (0-100)
        """
        complexity_map = {
            "low": 20,
            "medium": 50,
            "high": 75,
            "critical": 95,
        }
        
        return complexity_map.get(complexity.lower(), 50)  # Default to medium
    
    def find_mapping(
        self, 
        object_type: str, 
        properties: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Find matching mapping for an object.
        
        Args:
            object_type: Type of the object
            properties: Object properties
            
        Returns:
            Mapping dictionary if found, None otherwise
        """
        # Try exact matches first
        candidates = []
        
        # Match by object type
        type_key = self._create_mapping_key("", object_type)
        if type_key in self.mappings:
            candidates.append(self.mappings[type_key])
        
        # Match by visualization type
        viz_type = properties.get("visualization_type") or properties.get("viz_type")
        if viz_type:
            viz_key = self._create_mapping_key("Visualization", viz_type)
            if viz_key in self.mappings:
                candidates.append(self.mappings[viz_key])
        
        # Match by feature patterns
        for key, mapping in self.mappings.items():
            feature = mapping["feature"].lower()
            obj_type_lower = object_type.lower()
            
            # Check if feature name matches object type or properties
            if obj_type_lower in feature or feature in obj_type_lower:
                candidates.append(mapping)
            
            # Check visualization type match
            if viz_type and viz_type.lower() in feature:
                candidates.append(mapping)
        
        # Return best match (first candidate, or highest complexity if multiple)
        if candidates:
            # Prefer exact matches
            exact_matches = [c for c in candidates if c["complexity_score"] > 0]
            if exact_matches:
                return max(exact_matches, key=lambda x: x["complexity_score"])
            return candidates[0]
        
        return None
    
    def get_complexity_score(
        self, 
        object_type: str, 
        properties: Dict[str, Any]
    ) -> Tuple[Optional[float], Optional[str]]:
        """
        Get complexity score and level from custom mapping.
        
        Args:
            object_type: Type of the object
            properties: Object properties
            
        Returns:
            Tuple of (complexity_score, complexity_level) or (None, None) if not found
        """
        mapping = self.find_mapping(object_type, properties)
        
        if mapping:
            score = mapping["complexity_score"]
            level = self._score_to_level(score)
            return score, level
        
        return None, None
    
    def _score_to_level(self, score: float) -> str:
        """Convert score to level."""
        if score <= 30:
            return "low"
        elif score <= 60:
            return "medium"
        elif score <= 80:
            return "high"
        else:
            return "critical"
    
    def get_all_mappings(self) -> List[Dict[str, Any]]:
        """Get all loaded mappings."""
        return list(self.mappings.values())
    
    def reload_mappings(self, file_path: Optional[str] = None) -> None:
        """
        Reload mappings from file.
        
        Args:
            file_path: Optional path to mapping file. If None, uses current path.
        """
        if file_path:
            self.mapping_file_path = file_path
        self.mappings.clear()
        self._load_mappings()
