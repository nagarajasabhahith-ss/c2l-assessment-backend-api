"""
Hierarchy Service

Builds object hierarchy tree from top to bottom, handles cycle detection,
and identifies root nodes.
"""
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class HierarchyNode:
    """Represents a node in the object hierarchy tree."""
    
    def __init__(self, object_id: str, object_type: str, name: str, properties: dict = None):
        self.object_id = object_id
        self.object_type = object_type
        self.name = name
        self.properties = properties or {}
        
        # Hierarchy structure
        self.depth = 0
        self.level = 0
        self.children: List['HierarchyNode'] = []
        self.parent: Optional['HierarchyNode'] = None
        self.path: str = ""
        
        # Complexity metrics (calculated later)
        self.child_count = 0
        self.descendant_count = 0
        self.relationship_count = 0
    
    def __repr__(self):
        return f"<HierarchyNode {self.object_type}: {self.name} (depth={self.depth})>"


class HierarchyService:
    """Service for building and managing object hierarchy trees."""
    
    # Relationship types that create hierarchy (in priority order)
    HIERARCHY_RELATIONSHIP_TYPES = [
        "parent_child",  # Highest priority
        "contains",      # Container relationships
        "has_column",    # Table -> Column
    ]
    
    def __init__(self):
        self.nodes: Dict[str, HierarchyNode] = {}
        self.relationships_by_source: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
        self.relationships_by_target: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
        self.detected_cycles: List[List[str]] = []
    
    def build_hierarchy(
        self, 
        objects: List[Dict], 
        relationships: List[Dict]
    ) -> Dict[str, HierarchyNode]:
        """
        Build hierarchy tree from objects and relationships.
        
        Args:
            objects: List of object dictionaries with id, object_type, name, properties
            relationships: List of relationship dictionaries with source_id, target_id, relationship_type
        
        Returns:
            Dictionary of object_id -> HierarchyNode
        """
        # Reset state
        self.nodes.clear()
        self.relationships_by_source.clear()
        self.relationships_by_target.clear()
        self.detected_cycles.clear()
        
        # Create nodes for all objects
        for obj in objects:
            node = HierarchyNode(
                object_id=obj["id"],
                object_type=obj.get("object_type", "unknown"),
                name=obj.get("name", "Unknown"),
                properties=obj.get("properties", {})
            )
            self.nodes[obj["id"]] = node
        
        # Build relationship maps
        self._build_relationship_maps(relationships)
        
        # Detect and break cycles
        filtered_relationships = self._detect_and_break_cycles(relationships)
        
        # Rebuild relationship maps after cycle breaking
        self._build_relationship_maps(filtered_relationships)
        
        # Identify root nodes
        root_nodes = self._identify_root_nodes(objects)
        
        # Build tree structure
        for root in root_nodes:
            self._build_tree_from_root(root, 0, "")
        
        # Handle orphaned nodes
        self._handle_orphaned_nodes()
        
        # Calculate metrics
        self._calculate_metrics()
        
        return self.nodes
    
    def _build_relationship_maps(self, relationships: List[Dict]) -> None:
        """Build maps of relationships by source and target."""
        self.relationships_by_source.clear()
        self.relationships_by_target.clear()
        
        for rel in relationships:
            rel_type = rel.get("relationship_type", "")
            source_id = rel.get("source_object_id") or rel.get("source_id")
            target_id = rel.get("target_object_id") or rel.get("target_id")
            
            if not source_id or not target_id:
                continue
            
            # Only include hierarchy-creating relationships
            if rel_type in self.HIERARCHY_RELATIONSHIP_TYPES:
                self.relationships_by_source[source_id].append((target_id, rel_type))
                self.relationships_by_target[target_id].append((source_id, rel_type))
    
    def _identify_root_nodes(self, objects: List[Dict]) -> List[str]:
        """
        Identify root nodes (top-level objects).
        
        Root nodes are:
        1. Folders (always potential roots)
        2. Objects with no incoming PARENT_CHILD or CONTAINS relationships
        3. Objects with parent_id = None
        """
        root_candidates = []
        processed = set()
        
        for obj in objects:
            obj_id = obj["id"]
            obj_type = obj.get("object_type", "").lower()
            parent_id = obj.get("parent_id")
            
            # Folders are always potential roots
            if obj_type == "folder":
                if obj_id not in processed:
                    root_candidates.append(obj_id)
                    processed.add(obj_id)
                continue
            
            # Check if object has incoming hierarchy relationships
            has_incoming = obj_id in self.relationships_by_target
            
            # Check if object has parent_id
            has_parent_id = parent_id is not None
            
            # Object is a root if:
            # - No incoming hierarchy relationships AND
            # - No parent_id OR parent_id doesn't exist in objects
            if not has_incoming and (not has_parent_id or parent_id not in self.nodes):
                if obj_id not in processed:
                    root_candidates.append(obj_id)
                    processed.add(obj_id)
        
        logger.info(f"Identified {len(root_candidates)} root nodes")
        return root_candidates
    
    def _detect_and_break_cycles(
        self, 
        relationships: List[Dict]
    ) -> List[Dict]:
        """
        Detect circular dependencies and break them.
        
        Returns filtered relationships with cycles broken.
        """
        # Build adjacency graph for hierarchy relationships only
        graph = defaultdict(list)
        relationship_map = {}  # (source, target) -> relationship dict
        
        for rel in relationships:
            rel_type = rel.get("relationship_type", "")
            if rel_type not in self.HIERARCHY_RELATIONSHIP_TYPES:
                continue
            
            source_id = rel.get("source_object_id") or rel.get("source_id")
            target_id = rel.get("target_object_id") or rel.get("target_id")
            
            if not source_id or not target_id:
                continue
            
            graph[source_id].append(target_id)
            relationship_map[(source_id, target_id)] = rel
        
        # Detect cycles using DFS
        visited = set()
        rec_stack = set()
        cycles = []
        edges_to_remove = set()
        
        def has_cycle(node: str, path: List[str]) -> bool:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            
            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    if has_cycle(neighbor, path.copy()):
                        return True
                elif neighbor in rec_stack:
                    # Cycle detected
                    cycle_start = path.index(neighbor)
                    cycle = path[cycle_start:] + [neighbor]
                    cycles.append(cycle)
                    
                    # Break cycle by removing the last edge
                    if len(cycle) >= 2:
                        source = cycle[-2]
                        target = cycle[-1]
                        edges_to_remove.add((source, target))
                        logger.warning(
                            f"Cycle detected and broken: {' -> '.join(cycle)} "
                            f"(removed edge {source} -> {target})"
                        )
                    return True
            
            rec_stack.remove(node)
            return False
        
        # Detect all cycles
        for node in graph:
            if node not in visited:
                has_cycle(node, [])
        
        self.detected_cycles = cycles
        
        # Filter out relationships that create cycles
        filtered = [
            rel for rel in relationships
            if (
                rel.get("source_object_id") or rel.get("source_id"),
                rel.get("target_object_id") or rel.get("target_id")
            ) not in edges_to_remove
        ]
        
        if cycles:
            logger.info(f"Detected {len(cycles)} cycles, removed {len(edges_to_remove)} edges")
        
        return filtered
    
    def _build_tree_from_root(
        self, 
        root_id: str, 
        depth: int, 
        parent_path: str
    ) -> None:
        """
        Recursively build tree structure from a root node.
        
        Args:
            root_id: ID of the root node
            depth: Current depth level
            parent_path: Path string from root
        """
        if root_id not in self.nodes:
            return
        
        node = self.nodes[root_id]
        node.depth = depth
        node.level = depth
        node.path = f"{parent_path} > {node.name}" if parent_path else node.name
        
        # Get children from relationships (in priority order)
        children = []
        for rel_type in self.HIERARCHY_RELATIONSHIP_TYPES:
            for target_id, actual_rel_type in self.relationships_by_source.get(root_id, []):
                if actual_rel_type == rel_type and target_id in self.nodes:
                    children.append((target_id, rel_type))
        
        # Remove duplicates (keep first occurrence)
        seen = set()
        unique_children = []
        for child_id, rel_type in children:
            if child_id not in seen:
                seen.add(child_id)
                unique_children.append((child_id, rel_type))
        
        # Build child nodes
        for child_id, rel_type in unique_children:
            if child_id in self.nodes:
                child_node = self.nodes[child_id]
                
                # Only add if not already has a parent (prevent duplicates)
                if child_node.parent is None:
                    child_node.parent = node
                    node.children.append(child_node)
                    self._build_tree_from_root(child_id, depth + 1, node.path)
    
    def _handle_orphaned_nodes(self) -> None:
        """Handle nodes that don't have a parent (orphaned)."""
        orphaned = [
            node for node in self.nodes.values()
            if node.parent is None and node.depth == 0
        ]
        
        if orphaned:
            logger.info(f"Found {len(orphaned)} orphaned nodes, attaching to virtual root")
            
            # Create virtual root
            virtual_root = HierarchyNode(
                object_id="__virtual_root__",
                object_type="virtual_root",
                name="Root"
            )
            virtual_root.depth = -1
            virtual_root.level = -1
            virtual_root.path = "Root"
            
            # Attach orphaned nodes to virtual root
            for node in orphaned:
                node.parent = virtual_root
                virtual_root.children.append(node)
                self._build_tree_from_root(node.object_id, 0, "Root")
    
    def _calculate_metrics(self) -> None:
        """Calculate metrics for all nodes (child_count, descendant_count)."""
        def calculate_node_metrics(node: HierarchyNode) -> int:
            """Recursively calculate descendant count."""
            node.child_count = len(node.children)
            node.descendant_count = node.child_count
            
            for child in node.children:
                node.descendant_count += calculate_node_metrics(child)
            
            return node.descendant_count
        
        # Calculate for all root nodes
        root_nodes = [node for node in self.nodes.values() if node.parent is None]
        for root in root_nodes:
            calculate_node_metrics(root)
    
    def get_node(self, object_id: str) -> Optional[HierarchyNode]:
        """Get a node by object ID."""
        return self.nodes.get(object_id)
    
    def get_root_nodes(self) -> List[HierarchyNode]:
        """Get all root nodes."""
        return [node for node in self.nodes.values() if node.parent is None]
    
    def get_detected_cycles(self) -> List[List[str]]:
        """Get list of detected cycles."""
        return self.detected_cycles
