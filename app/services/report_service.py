"""
Service for report-related operations.
Generates reports for assessments by iterating over parsed objects one by one.
Only counts actual visualization types (Pie, Bar, Line, etc.); excludes
calculated_field, report, data_module, and other non-viz object types.

Uses a containment tree built from CONTAINS relationships so that
dashboard/report roots are resolved by traversing the graph (bottom-up or top-down).
"""
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Optional
import sys

# BigQuery: feature list for individual complexity (Visualization, etc.)
FEATURE_LIST_QUERY = (
    "SELECT feature_area, feature, complexity, feasibility, description, recommended "
    "FROM `tableau-to-looker-migration.C2L_Complexity_Rules.Feature_List_Looker_Perspective` "
    "LIMIT 1000"
)
FEATURE_AREA_VISUALIZATION = "Visualization"

from sqlalchemy.orm import Session

from app.config import settings
from app.db.bigquery import get_bigquery_client
from app.models.assessment import Assessment
from app.models.object import ExtractedObject, ObjectRelationship

# Allowlist: only these count as visualizations (Pie, Bar, Line, etc.)
# Excludes calculated_field, report, dashboard, data_module, etc.
try:
    _current = Path(__file__).resolve()
    for _root in [_current.parent.parent.parent.parent, _current.parent.parent.parent]:
        if (_root / "bi_parsers").exists():
            if str(_root) not in sys.path:
                sys.path.insert(0, str(_root))
            break
    from bi_parsers.cognos.visualization_types import ALL_VISUALIZATION_TYPES
    VISUALIZATION_TYPE_NAMES = frozenset(ALL_VISUALIZATION_TYPES)
except ImportError:
    VISUALIZATION_TYPE_NAMES = frozenset({
        "Pie", "Bar", "Line", "Area", "Donut", "Scatter", "Bubble", "Radar",
        "Clustered Bar", "Stacked Bar", "Clustered Column", "Stacked Column",
        "Pie Chart", "Bar Chart", "Line Chart", "Area Chart", "Chart",
        "Data Table", "List", "CrossTab", "Map", "KPI", "Gauge", "Heatmap",
    })


# Relationship types that mean "source contains target" (parser may store enum value or name)
CONTAINMENT_REL_TYPES = frozenset({"contains", "parent_child"})
# Relationship types that mean "source uses/references target" (for bottom-to-top: target is data_module/package/data_source)
USAGE_REL_TYPES = frozenset({"uses", "references", "connects_to"})
ROOT_OBJECT_TYPES = frozenset({"dashboard", "report"})


def _normalize_rel_type(rel_type: Any) -> str:
    """DB may have enum value ('contains') or enum name ('CONTAINS'); normalize to lowercase."""
    if rel_type is None:
        return ""
    if hasattr(rel_type, "value"):
        return (rel_type.value or "").strip().lower()
    return str(rel_type).strip().lower()


def _normalize_object_type(ot: Any) -> str:
    """DB may have enum value or name; normalize to lowercase."""
    if ot is None:
        return ""
    if hasattr(ot, "value"):
        return (ot.value or "").strip().lower()
    return str(ot).strip().lower()


class ContainmentTree:
    """
    Reusable tree built from CONTAINS relationships.
    Traverse top-down (parent -> children) or bottom-up (child -> parent to root).
    """
    __slots__ = ("id_to_obj", "contains_parent", "contains_children", "_root_cache")

    def __init__(
        self,
        id_to_obj: dict[Any, ExtractedObject],
        contains_parent: dict[Any, Any],
        contains_children: dict[Any, list[Any]],
    ):
        self.id_to_obj = id_to_obj
        self.contains_parent = contains_parent
        self.contains_children = contains_children
        self._root_cache: dict[Any, tuple[Any, Optional[str]]] = {}

    def get_root(self, object_id: Any) -> tuple[Optional[Any], Optional[str]]:
        """
        Walk bottom-up via CONTAINS until we find a dashboard or report root.
        Returns (root_object_id, "dashboard"|"report"|None).
        """
        if object_id in self._root_cache:
            return self._root_cache[object_id]
        visited: set[Any] = set()
        current: Any = object_id
        while current and current not in visited:
            visited.add(current)
            obj = self.id_to_obj.get(current)
            if obj:
                ot = _normalize_object_type(obj.object_type)
                if ot in ROOT_OBJECT_TYPES:
                    self._root_cache[object_id] = (current, ot)
                    return (current, ot)
            current = self.contains_parent.get(current)
        self._root_cache[object_id] = (None, None)
        return (None, None)

    def roots_top_down(self) -> list[Any]:
        """Root object IDs (dashboard/report) that have no CONTAINS parent in this set."""
        with_parent = set(self.contains_parent.values())
        return [oid for oid in self.id_to_obj if oid not in with_parent]

    def children_of(self, object_id: Any) -> list[Any]:
        """Direct children (CONTAINS target) for top-down traversal."""
        return list(self.contains_children.get(object_id, []))

    def get_descendants(self, root_id: Any) -> set[Any]:
        """All descendant object IDs under root (BFS top-down). Includes root_id."""
        out: set[Any] = {root_id}
        queue: list[Any] = [root_id]
        while queue:
            node = queue.pop(0)
            for child in self.contains_children.get(node, []):
                if child not in out:
                    out.add(child)
                    queue.append(child)
        return out


def build_containment_tree(
    objects: list[ExtractedObject],
    relationships: list[ObjectRelationship],
) -> ContainmentTree:
    """
    Build complete tree from objects and containment relationships.
    Uses both CONTAINS and PARENT_CHILD (source = parent, target = child).
    - contains_parent[child_id] = parent_id (for bottom-up traversal)
    - contains_children[parent_id] = [child_ids] (for top-down traversal)
    """
    id_to_obj = {obj.id: obj for obj in objects}
    contains_parent: dict[Any, Any] = {}
    contains_children: dict[Any, list[Any]] = defaultdict(list)
    for rel in relationships:
        rt = _normalize_rel_type(rel.relationship_type)
        if rt not in CONTAINMENT_REL_TYPES:
            continue
        src, tgt = rel.source_object_id, rel.target_object_id
        if src not in id_to_obj or tgt not in id_to_obj:
            continue
        contains_parent[tgt] = src
        contains_children[src].append(tgt)
    return ContainmentTree(
        id_to_obj=id_to_obj,
        contains_parent=contains_parent,
        contains_children=dict(contains_children),
    )


class ReportService:
    def __init__(self, db: Session):
        self.db = db
        # Cached feature list from BigQuery (loaded once, reused for individual complexity)
        self._feature_list_cache: Optional[list[dict[str, Any]]] = None

    def _run_bigquery_query(self, query: str) -> list[dict[str, Any]]:
        """
        Run a BigQuery query and return rows as list of dicts.
        Returns [] if BigQuery is not configured or query fails.
        """
        client = get_bigquery_client()
        if not client:
            return []
        try:
            job = client.query(query)
            rows = job.result()
            return [dict(row) for row in rows]
        except Exception:
            return []

    def _get_feature_list(self) -> list[dict[str, Any]]:
        """
        Load feature list from BigQuery once and cache on this instance.
        Reused for individual complexity (e.g. Visualization breakdown).
        """
        if self._feature_list_cache is None:
            self._feature_list_cache = self._run_bigquery_query(FEATURE_LIST_QUERY)
        return self._feature_list_cache

    def _get_visualization_complexity_lookup(self) -> dict[str, dict[str, Any]]:
        """
        From cached feature list, filter feature_area == 'Visualization' and build
        a lookup by normalized feature name -> {complexity, feasibility, description, recommended}.
        Used to set per-visualization complexity in visualization_details breakdown.
        """
        rows = self._get_feature_list()
        lookup: dict[str, dict[str, Any]] = {}
        for r in rows:
            area = (r.get("feature_area") or "").strip()
            if area != FEATURE_AREA_VISUALIZATION:
                continue
            feature = (r.get("feature") or "").strip()
            if not feature:
                continue
            key = feature.lower()
            # Keep first occurrence if duplicates (e.g. same feature name)
            if key not in lookup:
                lookup[key] = {
                    "complexity": r.get("complexity"),
                    "feasibility": r.get("feasibility"),
                    "description": r.get("description"),
                    "recommended": r.get("recommended"),
                }
        return lookup

    def get_containment_tree(self, assessment: Assessment) -> ContainmentTree:
        """
        Build the complete containment tree for an assessment (reusable for
        visualization details and other report calculations).
        Callers can traverse top-down (roots_top_down, children_of) or
        bottom-up (get_root from any object id).
        """
        objects = list(assessment.objects)
        relationships = list(assessment.relationships)
        return build_containment_tree(objects, relationships)

    def generate_report_for_assessment(self, assessment: Assessment) -> dict[str, Any]:
        """
        Generate report for an assessment by processing all parsed objects one by one.
        Uses containment tree (CONTAINS/PARENT_CHILD) for dashboard/report resolution.
        """
        objects = list(assessment.objects)
        tree = self.get_containment_tree(assessment)
        relationships = list(assessment.relationships)
        report: dict[str, Any] = {
            "assessment_id": str(assessment.id),
            "sections": {},
        }
        report["sections"]["visualization_details"] = self._get_visualization_details(
            objects, tree, relationships
        )
        report["sections"]["dashboards_breakdown"] = self._get_dashboards_breakdown(
            objects, tree
        )
        report["sections"]["reports_breakdown"] = self._get_reports_breakdown(
            objects, tree, relationships
        )
        report["sections"]["packages_breakdown"] = self._get_packages_breakdown(
            objects, tree
        )
        report["sections"]["data_source_connections_breakdown"] = (
            self._get_data_source_connections_breakdown(objects, tree, relationships)
        )
        report["sections"]["calculated_fields_breakdown"] = self._get_calculated_fields_breakdown(
            objects, tree, relationships
        )
        report["sections"]["filters_breakdown"] = self._get_filters_breakdown(
            objects, tree, relationships
        )
        report["sections"]["parameters_breakdown"] = self._get_parameters_breakdown(objects)
        report["sections"]["sorts_breakdown"] = self._get_sorts_breakdown(objects)
        report["sections"]["prompts_breakdown"] = self._get_prompts_breakdown(objects)
        report["sections"]["data_modules_breakdown"] = self._get_data_modules_breakdown(
            objects, tree, relationships
        )
        report["sections"]["queries_breakdown"] = self._get_queries_breakdown(objects, tree)
        report["sections"]["measures_breakdown"] = self._get_measures_breakdown(
            objects, tree, relationships
        )
        report["sections"]["dimensions_breakdown"] = self._get_dimensions_breakdown(
            objects, tree, relationships
        )

        vd = report["sections"]["visualization_details"]
        by_complexity = vd.get("by_complexity") or {}
        cf_breakdown = report["sections"].get("calculated_fields_breakdown") or {}
        by_cf = cf_breakdown.get("by_complexity") or {}
        filters_breakdown = report["sections"].get("filters_breakdown") or {}
        by_filter = filters_breakdown.get("by_complexity") or {}
        report["complex_analysis"] = {
            "visualization": [
                {
                    "complexity": level,
                    "visualization_count": by_complexity.get(level, {}).get("visualization_count", 0),
                    "dashboards_containing_count": by_complexity.get(level, {}).get("dashboards_containing_count", 0),
                    "reports_containing_count": by_complexity.get(level, {}).get("reports_containing_count", 0),
                }
                for level in ("low", "medium", "high", "critical")
            ],
            "dashboard": [
                {
                    "complexity": level,
                    "dashboards_containing_count": by_complexity.get(level, {}).get("dashboards_containing_count", 0),
                }
                for level in ("low", "medium", "high", "critical")
            ],
            "report": [
                {
                    "complexity": level,
                    "reports_containing_count": by_complexity.get(level, {}).get("reports_containing_count", 0),
                }
                for level in ("low", "medium", "high", "critical")
            ],
            "calculated_field": [
                {
                    "complexity": level,
                    "calculated_field_count": by_cf.get(level, {}).get("calculated_field_count", 0),
                    "dashboards_containing_count": by_cf.get(level, {}).get("dashboards_containing_count", 0),
                    "reports_containing_count": by_cf.get(level, {}).get("reports_containing_count", 0),
                }
                for level in ("low", "medium", "high", "critical")
            ],
            "filter": [
                {
                    "complexity": level,
                    "filter_count": by_filter.get(level, {}).get("filter_count", 0),
                    "dashboards_containing_count": by_filter.get(level, {}).get("dashboards_containing_count", 0),
                    "reports_containing_count": by_filter.get(level, {}).get("reports_containing_count", 0),
                }
                for level in ("low", "medium", "high", "critical")
            ],
        }

        return report

    def _file_to_container_type(
        self, objects: list[ExtractedObject]
    ) -> dict[Any, str]:
        """
        Fallback: file_id -> "dashboard"|"report" from object_type in same file.
        Used when tree traversal does not find a dashboard/report root.
        """
        file_types: dict[Any, set[str]] = defaultdict(set)
        for obj in objects:
            ot = _normalize_object_type(obj.object_type)
            if ot:
                file_types[obj.file_id].add(ot)
        result: dict[Any, str] = {}
        for fid, types in file_types.items():
            if "dashboard" in types:
                result[fid] = "dashboard"
            elif "report" in types:
                result[fid] = "report"
        return result

    def _resolve_containment_root(
        self,
        obj: ExtractedObject,
        tree: ContainmentTree,
        file_container: dict[Any, str],
        relationships_by_target: dict[Any, list[Any]],
    ) -> tuple[int, int, Any, Any]:
        """
        Resolve dashboard/report containment for an object.
        Returns (dashboards_containing_count, reports_containing_count, dash_root_key, report_root_key).
        Counts are 0 or 1; root keys are used for distinct aggregation (or None).
        Tries: (1) tree.get_root(obj.id), (2) get_root of any object that references this one (relationship target), (3) file_container fallback.
        """
        root_id, root_kind = tree.get_root(obj.id)
        if root_kind == "dashboard" and root_id is not None:
            return (1, 0, root_id, None)
        if root_kind == "report" and root_id is not None:
            return (0, 1, None, root_id)
        # Fallback: any relationship where this object is target — use source's root
        for key in (obj.id, str(obj.id)):
            for src_id in relationships_by_target.get(key, []):
                rid, rkind = tree.get_root(src_id)
                if rkind == "dashboard" and rid is not None:
                    return (1, 0, rid, None)
                if rkind == "report" and rid is not None:
                    return (0, 1, None, rid)
        # File fallback
        container = file_container.get(obj.file_id)
        if container == "dashboard":
            return (1, 0, ("file", obj.file_id), None)
        if container == "report":
            return (0, 1, None, ("file", obj.file_id))
        return (0, 0, None, None)

    def _get_visualization_details(
        self,
        objects: list[ExtractedObject],
        tree: ContainmentTree,
        relationships: Optional[list[ObjectRelationship]] = None,
    ) -> dict[str, Any]:
        """
        Aggregate visualization counts using containment tree: total and per-type
        count, plus how many distinct dashboards and reports contain each type.
        Resolves root by traversing CONTAINS/PARENT_CHILD upward; falls back to
        file-based container when tree root is missing.
        When relationships are provided, also counts distinct queries used by each
        viz type (USES/REFERENCES from visualization to query); parser links viz→query
        via USES, not CONTAINS, so containment walk-up would not find queries.
        """
        file_container = self._file_to_container_type(objects)
        viz_to_count_and_containers: dict[str, tuple[int, set[Any], set[Any]]] = defaultdict(
            lambda: (0, set(), set())
        )
        viz_to_queries: dict[str, set[Any]] = defaultdict(set)
        id_to_obj = tree.id_to_obj
        # From relationships: viz → query (USES/REFERENCES) so we count queries used by each viz type
        if relationships:
            for rel in relationships:
                rt = _normalize_rel_type(rel.relationship_type)
                if rt not in USAGE_REL_TYPES:
                    continue
                src, tgt = rel.source_object_id, rel.target_object_id
                if src not in id_to_obj or tgt not in id_to_obj:
                    continue
                src_obj = id_to_obj[src]
                tgt_obj = id_to_obj[tgt]
                if not self._is_visualization_object(src_obj):
                    continue
                if _normalize_object_type(tgt_obj.object_type) != "query":
                    continue
                viz_type = self._get_visualization_type_for_object(src_obj)
                viz_to_queries[viz_type].add(tgt)
        for obj in objects:
            if not self._is_visualization_object(obj):
                continue
            viz_type = self._get_visualization_type_for_object(obj)
            count, dash_roots, report_roots = viz_to_count_and_containers[viz_type]
            count += 1
            root_id, root_kind = tree.get_root(obj.id)
            if root_kind == "dashboard" and root_id is not None:
                dash_roots.add(root_id)
            elif root_kind == "report" and root_id is not None:
                report_roots.add(root_id)
            else:
                # Fallback: same file as a dashboard/report object
                container = file_container.get(obj.file_id)
                if container == "dashboard":
                    dash_roots.add(("file", obj.file_id))
                elif container == "report":
                    report_roots.add(("file", obj.file_id))
            # Query count is already filled from USES/REFERENCES (viz→query) above; containment
            # walk-up would not find query because parser links report→viz (CONTAINS) and viz→query (USES).
            viz_to_count_and_containers[viz_type] = (count, dash_roots, report_roots)
        total = sum(t[0] for t in viz_to_count_and_containers.values())
        viz_complexity_lookup = self._get_visualization_complexity_lookup()
        # Per-complexity: union of dashboard/report roots that contain at least one viz of that complexity
        complexity_to_dash_roots: dict[str, set[Any]] = defaultdict(set)
        complexity_to_report_roots: dict[str, set[Any]] = defaultdict(set)
        breakdown = []
        for viz, (count, dash_roots, report_roots) in sorted(
            viz_to_count_and_containers.items(), key=lambda x: -x[1][0]
        ):
            key = (viz or "").strip().lower()
            info = viz_complexity_lookup.get(key) or {}
            complexity = (info.get("complexity") if info else None) or "Unknown"
            c_key = (complexity or "").strip().lower()
            if c_key in ("low", "medium", "high", "critical"):
                complexity_to_dash_roots[c_key].update(dash_roots)
                complexity_to_report_roots[c_key].update(report_roots)
            breakdown.append({
                "visualization": viz,
                "count": count,
                "complexity": complexity,
                "feasibility": info.get("feasibility"),
                "description": info.get("description"),
                "recommended": info.get("recommended"),
                "dashboards_containing_count": len(dash_roots),
                "reports_containing_count": len(report_roots),
                "queries_using_count": len(viz_to_queries.get(viz, set())),
            })
        # Aggregate counts by complexity (low, medium, high, critical)
        stats: dict[str, int] = {"low": 0, "medium": 0, "high": 0, "critical": 0}
        for item in breakdown:
            c = (item.get("complexity") or "").strip().lower()
            cnt = item.get("count") or 0
            if c == "low":
                stats["low"] += cnt
            elif c == "medium":
                stats["medium"] += cnt
            elif c == "high":
                stats["high"] += cnt
            elif c == "critical":
                stats["critical"] += cnt
        overall_complexity = None
        # By complexity: visualization count and distinct dashboards/reports containing that complexity
        by_complexity: dict[str, dict[str, Any]] = {}
        for level in ("low", "medium", "high", "critical"):
            by_complexity[level] = {
                "visualization_count": stats.get(level, 0),
                "dashboards_containing_count": len(complexity_to_dash_roots.get(level, set())),
                "reports_containing_count": len(complexity_to_report_roots.get(level, set())),
            }

        return {
            "total_visualization": total,
            "overall_complexity": overall_complexity,
            "stats": stats,
            "by_complexity": by_complexity,
            "breakdown": breakdown,
        }

    def _is_visualization_object(self, obj: ExtractedObject) -> bool:
        """
        True only if this object is a visualization (Pie, Bar, Line, etc.).
        Excludes calculated_field, report, data_module, and other non-viz types.
        """
        resolved = self._get_visualization_type_for_object(obj)
        return resolved in VISUALIZATION_TYPE_NAMES

    def _get_visualization_type_for_object(self, obj: ExtractedObject) -> str:
        """
        Get the visualization type for a single object.
        Prefer properties.visualization_type or similar; fallback to object_type.
        """
        if obj.properties and isinstance(obj.properties, dict):
            viz = obj.properties.get("visualization_type") or obj.properties.get(
                "type"
            )
            if viz is not None:
                return str(viz)
        return obj.object_type or "unknown"

    def _get_dashboards_breakdown(
        self,
        objects: list[ExtractedObject],
        tree: ContainmentTree,
    ) -> dict[str, Any]:
        """
        Per-dashboard breakdown: total dashboards, and for each dashboard the counts of
        visualizations, tabs, measures, dimensions, calculated fields, data modules,
        packages, data sources.
        """
        # Group object IDs by dashboard root (objects whose get_root is this dashboard)
        dashboard_to_ids: dict[Any, set[Any]] = defaultdict(set)
        for obj in objects:
            root_id, root_kind = tree.get_root(obj.id)
            if root_kind == "dashboard" and root_id is not None:
                dashboard_to_ids[root_id].add(obj.id)

        # Also include dashboard roots that have no other objects (standalone)
        id_to_obj = tree.id_to_obj
        for oid, obj in id_to_obj.items():
            ot = _normalize_object_type(obj.object_type)
            if ot == "dashboard":
                dashboard_to_ids[oid].add(oid)

        viz_complexity_lookup = self._get_visualization_complexity_lookup()
        dashboards_list: list[dict[str, Any]] = []
        for dash_id, member_ids in dashboard_to_ids.items():
            dash_obj = id_to_obj.get(dash_id)
            name = (dash_obj.name if dash_obj else None) or str(dash_id)
            counts: dict[str, int] = defaultdict(int)
            for oid in member_ids:
                obj = id_to_obj.get(oid)
                if not obj:
                    continue
                ot = _normalize_object_type(obj.object_type)
                if self._is_visualization_object(obj):
                    counts["visualizations"] += 1
                    key = (self._get_visualization_type_for_object(obj) or "").strip().lower()
                    info = viz_complexity_lookup.get(key) or {}
                    c = ((info.get("complexity") or "") or "").strip().lower()
                    if c == "low":
                        counts["visualizations_low"] += 1
                    elif c == "medium":
                        counts["visualizations_medium"] += 1
                    elif c == "high":
                        counts["visualizations_high"] += 1
                    elif c == "critical":
                        counts["visualizations_critical"] += 1
                elif ot == "tab":
                    counts["tabs"] += 1
                elif ot == "measure":
                    counts["measures"] += 1
                elif ot == "dimension":
                    counts["dimensions"] += 1
                elif ot == "calculated_field":
                    counts["calculated_fields"] += 1
                elif ot == "data_module":
                    counts["data_modules"] += 1
                elif ot == "package":
                    counts["packages"] += 1
                elif ot in ("data_source", "data_source_connection"):
                    counts["data_sources"] += 1
            viz_by_complexity = {
                "low": counts["visualizations_low"],
                "medium": counts["visualizations_medium"],
                "high": counts["visualizations_high"],
                "critical": counts["visualizations_critical"],
            }
            # Dashboard complexity = worst level present (critical > high > medium > low)
            if viz_by_complexity["critical"] > 0:
                dashboard_complexity = "Critical"
            elif viz_by_complexity["high"] > 0:
                dashboard_complexity = "High"
            elif viz_by_complexity["medium"] > 0:
                dashboard_complexity = "Medium"
            elif viz_by_complexity["low"] > 0:
                dashboard_complexity = "Low"
            else:
                dashboard_complexity = "Unknown"  # no viz or all unknown
            dashboards_list.append({
                "dashboard_id": str(dash_id),
                "dashboard_name": name,
                "complexity": dashboard_complexity,
                "total_visualizations": counts["visualizations"],
                "visualizations_by_complexity": viz_by_complexity,
                "total_tabs": counts["tabs"],
                "total_measures": counts["measures"],
                "total_dimensions": counts["dimensions"],
                "total_calculated_fields": counts["calculated_fields"],
                "total_data_modules": counts["data_modules"],
                "total_packages": counts["packages"],
                "total_data_sources": counts["data_sources"],
            })
        total_dashboards = len(dashboard_to_ids)
        # Stats: count of dashboards by derived complexity (Low, Medium, High, Critical)
        dashboard_stats: dict[str, int] = {"low": 0, "medium": 0, "high": 0, "critical": 0}
        for d in dashboards_list:
            c = (d.get("complexity") or "").strip().lower()
            if c == "low":
                dashboard_stats["low"] += 1
            elif c == "medium":
                dashboard_stats["medium"] += 1
            elif c == "high":
                dashboard_stats["high"] += 1
            elif c == "critical":
                dashboard_stats["critical"] += 1
        return {
            "total_dashboards": total_dashboards,
            "stats": dashboard_stats,
            "dashboards": dashboards_list,
        }
    
    def _get_report_type(self, obj: ExtractedObject) -> str:
        """Report type from properties: report, interactiveReport, reportView, dataSet2, reportVersion."""
        if obj.properties and isinstance(obj.properties, dict):
            t = (
                obj.properties.get("reportType")
                or obj.properties.get("report_type")
                or obj.properties.get("cognosClass")
            )
            if t is not None:
                return str(t)
        return "report"

    def _get_reports_breakdown(
        self,
        objects: list[ExtractedObject],
        tree: ContainmentTree,
        relationships: list[ObjectRelationship],
    ) -> dict[str, Any]:
        """
        Per-report breakdown: total reports, and for each report the counts of
        visualizations, pages, tables, columns, filters, parameters, sorts, prompts,
        calculated fields, measures, dimensions (from containment and from data
        modules used by the report). Data modules, packages, and data sources are
        counted by traversing bottom-to-top: from each data_module/package/data_source
        object, find incoming USES/REFERENCES/CONNECTS_TO edges and resolve the
        source's report root; distinct counts. Tables and columns: (1) objects in
        containment under the report, (2) sum of table_count/column_count from each
        data module used by the report. Report name and type (report, interactiveReport,
        reportView, dataSet2).
        """
        id_to_obj = tree.id_to_obj
        report_to_ids: dict[Any, set[Any]] = defaultdict(set)
        for obj in objects:
            root_id, root_kind = tree.get_root(obj.id)
            if root_kind == "report" and root_id is not None:
                report_to_ids[root_id].add(obj.id)

        for oid, obj in id_to_obj.items():
            ot = _normalize_object_type(obj.object_type)
            if ot == "report":
                report_to_ids[oid].add(oid)

        # Relationship fallback: objects not in CONTAINS chain (e.g. filters/calculated fields
        # linked via REFERENCES/USES) — resolve report root via any object that references this one.
        relationships_by_target: dict[Any, list[Any]] = defaultdict(list)
        for rel in relationships:
            tgt = rel.target_object_id
            relationships_by_target[tgt].append(rel.source_object_id)
            if str(tgt) != tgt:
                relationships_by_target[str(tgt)] = relationships_by_target[tgt]
        obj_ids_in_reports = {oid for member_ids in report_to_ids.values() for oid in member_ids}
        for obj in objects:
            if obj.id in obj_ids_in_reports:
                continue
            for key in (obj.id, str(obj.id)):
                for src_id in relationships_by_target.get(key, []):
                    root_id, root_kind = tree.get_root(src_id)
                    if root_kind == "report" and root_id is not None:
                        report_to_ids[root_id].add(obj.id)
                        obj_ids_in_reports.add(obj.id)
                        break
                if obj.id in obj_ids_in_reports:
                    break

        # Data modules, packages, data sources: distinct counts via (1) bottom-to-top usage,
        # (2) top-down usage from report (nested: report→query→data_module), (3) containment.
        report_to_data_modules: dict[Any, set[Any]] = defaultdict(set)
        report_to_packages: dict[Any, set[Any]] = defaultdict(set)
        report_to_data_sources: dict[Any, set[Any]] = defaultdict(set)

        # Build usage graph (forward): source -> [targets] for USES/REFERENCES/CONNECTS_TO
        usage_children: dict[Any, list[Any]] = defaultdict(list)
        for rel in relationships:
            rt = _normalize_rel_type(rel.relationship_type)
            if rt not in USAGE_REL_TYPES:
                continue
            src, tgt = rel.source_object_id, rel.target_object_id
            if src in id_to_obj and tgt in id_to_obj:
                usage_children[src].append(tgt)

        def add_external_by_type(rid: Any, oid: Any, ot: str) -> None:
            if ot == "data_module":
                report_to_data_modules[rid].add(oid)
            elif ot == "package":
                report_to_packages[rid].add(oid)
            elif ot in ("data_source", "data_source_connection"):
                report_to_data_sources[rid].add(oid)

        # (1) Bottom-to-top: for each usage rel, target = data_module/package/data_source,
        #     source's report root gets it (handles query→data_module when get_root(query)=report).
        for rel in relationships:
            rt = _normalize_rel_type(rel.relationship_type)
            if rt not in USAGE_REL_TYPES:
                continue
            tgt = rel.target_object_id
            src = rel.source_object_id
            if tgt not in id_to_obj or src not in id_to_obj:
                continue
            tgt_obj = id_to_obj[tgt]
            tgt_ot = _normalize_object_type(tgt_obj.object_type)
            root_id, root_kind = tree.get_root(src)
            if root_kind != "report" or root_id is None:
                continue
            add_external_by_type(root_id, tgt, tgt_ot)

        # (2) Top-down from report: BFS along usage edges so nested report→query→data_module counts
        for report_id in report_to_ids:
            seen: set[Any] = set()
            queue: list[Any] = [report_id]
            while queue:
                node = queue.pop(0)
                if node in seen:
                    continue
                seen.add(node)
                obj = id_to_obj.get(node)
                if obj:
                    ot = _normalize_object_type(obj.object_type)
                    add_external_by_type(report_id, node, ot)
                for child in usage_children.get(node, []):
                    if child not in seen:
                        queue.append(child)

        # (3) Containment: objects under this report (CONTAINS tree) that are data_module/package/data_source
        for report_id, member_ids in report_to_ids.items():
            for oid in member_ids:
                obj = id_to_obj.get(oid)
                if not obj:
                    continue
                ot = _normalize_object_type(obj.object_type)
                add_external_by_type(report_id, oid, ot)

        viz_complexity_lookup = self._get_visualization_complexity_lookup()
        reports_list: list[dict[str, Any]] = []
        for report_id, member_ids in report_to_ids.items():
            report_obj = id_to_obj.get(report_id)
            name = (report_obj.name if report_obj else None) or str(report_id)
            report_type = self._get_report_type(report_obj) if report_obj else "report"
            counts: dict[str, int] = defaultdict(int)
            for oid in member_ids:
                obj = id_to_obj.get(oid)
                if not obj:
                    continue
                ot = _normalize_object_type(obj.object_type)
                if self._is_visualization_object(obj):
                    counts["visualizations"] += 1
                    key = (self._get_visualization_type_for_object(obj) or "").strip().lower()
                    info = viz_complexity_lookup.get(key) or {}
                    c = ((info.get("complexity") or "") or "").strip().lower()
                    if c == "low":
                        counts["visualizations_low"] += 1
                    elif c == "medium":
                        counts["visualizations_medium"] += 1
                    elif c == "high":
                        counts["visualizations_high"] += 1
                    elif c == "critical":
                        counts["visualizations_critical"] += 1
                elif ot == "page":
                    counts["pages"] += 1
                elif ot == "filter":
                    counts["filters"] += 1
                elif ot == "parameter":
                    counts["parameters"] += 1
                elif ot == "sort":
                    counts["sorts"] += 1
                elif ot == "prompt":
                    counts["prompts"] += 1
                elif ot == "calculated_field":
                    counts["calculated_fields"] += 1
                    props = (obj.properties or {}) if isinstance(obj.properties, dict) else {}
                    calc_type = (self._get_prop_any_case(props, "calculation_type") or "").strip() or "expression"
                    expr_raw = self._get_prop_any_case(props, "expression", "formula", "calculation")
                    cf_complexity = self._calculated_field_complexity(calc_type, expr_raw)
                    cf_c = (cf_complexity or "").strip().lower()
                    if cf_c == "low":
                        counts["calculated_fields_low"] += 1
                    elif cf_c == "medium":
                        counts["calculated_fields_medium"] += 1
                    elif cf_c == "high":
                        counts["calculated_fields_high"] += 1
                    elif cf_c == "critical":
                        counts["calculated_fields_critical"] += 1
                elif ot == "measure":
                    counts["measures"] += 1
                elif ot == "dimension":
                    counts["dimensions"] += 1
                elif ot == "table":
                    counts["tables"] += 1
                elif ot == "column":
                    counts["columns"] += 1
                # data_module, package, data_source: not from containment; use bottom-to-top sets below
            # Tables/columns also from data modules used by this report (parser stores table_count, column_count on module)
            for mid in report_to_data_modules.get(report_id, set()):
                mobj = id_to_obj.get(mid)
                if mobj and isinstance(mobj.properties, dict):
                    counts["tables"] += int(mobj.properties.get("table_count") or 0)
                    counts["columns"] += int(mobj.properties.get("column_count") or 0)
            viz_by_complexity = {
                "low": counts["visualizations_low"],
                "medium": counts["visualizations_medium"],
                "high": counts["visualizations_high"],
                "critical": counts["visualizations_critical"],
            }
            calculated_fields_by_complexity = {
                "low": counts["calculated_fields_low"],
                "medium": counts["calculated_fields_medium"],
                "high": counts["calculated_fields_high"],
                "critical": counts["calculated_fields_critical"],
            }

            # Report complexity: interactiveReport → Critical; else worst level present (critical > high > medium > low)
            if (report_type or "").strip().lower() == "interactivereport":
                report_complexity = "Critical"
            elif viz_by_complexity["critical"] > 0:
                report_complexity = "Critical"
            elif viz_by_complexity["high"] > 0:
                report_complexity = "High"
            elif viz_by_complexity["medium"] > 0:
                report_complexity = "Medium"
            elif viz_by_complexity["low"] > 0:
                report_complexity = "Low"
            else:
                report_complexity = "Unknown"


            reports_list.append({
                "report_id": str(report_id),
                "report_name": name,
                "report_type": report_type,
                "complexity": report_complexity,
                "total_visualizations": counts["visualizations"],
                "visualizations_by_complexity": viz_by_complexity,
                "calculated_fields_by_complexity": calculated_fields_by_complexity,
                "total_pages": counts["pages"],
                "total_data_modules": len(report_to_data_modules.get(report_id, set())),
                "total_packages": len(report_to_packages.get(report_id, set())),
                "total_data_sources": len(report_to_data_sources.get(report_id, set())),
                "total_tables": counts["tables"],
                "total_columns": counts["columns"],
                "total_filters": counts["filters"],
                "total_parameters": counts["parameters"],
                "total_sorts": counts["sorts"],
                "total_prompts": counts["prompts"],
                "total_calculated_fields": counts["calculated_fields"],
                "total_measures": counts["measures"],
                "total_dimensions": counts["dimensions"],
            })
        total_reports = len(report_to_ids)
        # Stats: count of reports by derived complexity (Low, Medium, High, Critical)
        report_stats: dict[str, int] = {"low": 0, "medium": 0, "high": 0, "critical": 0}
        for r in reports_list:
            c = (r.get("complexity") or "").strip().lower()
            if c == "low":
                report_stats["low"] += 1
            elif c == "medium":
                report_stats["medium"] += 1
            elif c == "high":
                report_stats["high"] += 1
            elif c == "critical":
                report_stats["critical"] += 1
        return {
            "total_reports": total_reports,
            "stats": report_stats,
            "reports": reports_list,
        }

    def _get_package_root(self, object_id: Any, tree: ContainmentTree) -> Optional[Any]:
        """Walk up containment until we find a package; return that package id or None."""
        current: Any = object_id
        visited: set[Any] = set()
        while current and current not in visited:
            visited.add(current)
            obj = tree.id_to_obj.get(current)
            if obj:
                ot = _normalize_object_type(obj.object_type)
                if ot == "package":
                    return current
            current = tree.contains_parent.get(current)
        return None

    # Main data modules: module, dataModule, model. Sub-modules: smartsModule, modelView, dataSet2.
    _MAIN_DATA_MODULE_CLASSES = frozenset({"module", "dataModule", "model"})

    def _get_data_module_type(self, obj: ExtractedObject) -> str:
        """Original module kind: smartsModule, dataModule, module (parser cognosClass)."""
        if obj.properties and isinstance(obj.properties, dict):
            kind = obj.properties.get("cognosClass") or obj.properties.get("moduleType")
            if kind is not None:
                s = str(kind).strip()
                if s:
                    return s
        return "data_module"

    def _is_main_data_module(self, obj: ExtractedObject) -> bool:
        """True if this is a main/root data module (module, dataModule, model); false for smartsModule, modelView, dataSet2."""
        if obj.properties and isinstance(obj.properties, dict):
            if obj.properties.get("is_main_module") is True:
                return True
            kind = obj.properties.get("cognosClass") or obj.properties.get("moduleType")
            if kind is not None and str(kind).strip() in self._MAIN_DATA_MODULE_CLASSES:
                return True
        return False

    def _get_packages_breakdown(
        self,
        objects: list[ExtractedObject],
        tree: ContainmentTree,
    ) -> dict[str, Any]:
        """
        Per-package breakdown: total packages, and for each package the counts of
        data modules (including by type: smartsModule, dataModule, module), tables, columns.
        Objects are assigned to a package by walking up the containment tree to the nearest package.
        Packages are deduplicated by: (1) normalizing id to string; (2) merging by normalized name
        so the same logical package from multiple package*.xml files appears once, with member counts from the union.
        """
        id_to_obj = tree.id_to_obj
        # Key by str(pkg_id) so the same package is not listed twice (e.g. int vs str id)
        package_key_to_canonical_and_members: dict[str, tuple[Any, set[Any]]] = {}
        for obj in objects:
            pkg_id = self._get_package_root(obj.id, tree)
            if pkg_id is not None:
                key = str(pkg_id)
                if key not in package_key_to_canonical_and_members:
                    package_key_to_canonical_and_members[key] = (pkg_id, set())
                package_key_to_canonical_and_members[key][1].add(obj.id)
        for oid, obj in id_to_obj.items():
            ot = _normalize_object_type(obj.object_type)
            if ot == "package":
                key = str(oid)
                if key not in package_key_to_canonical_and_members:
                    package_key_to_canonical_and_members[key] = (oid, set())
                package_key_to_canonical_and_members[key][1].add(oid)

        # Deduplicate by normalized name: same package can appear as multiple objects (e.g. from different package*.xml files).
        # Merge member_ids by name so we count each member once.
        name_to_canonical_and_members: dict[str, tuple[Any, set[Any]]] = {}
        for _key, (pkg_id, member_ids) in package_key_to_canonical_and_members.items():
            pkg_obj = id_to_obj.get(pkg_id) or id_to_obj.get(str(pkg_id))
            name = (pkg_obj.name if pkg_obj else None) or str(pkg_id)
            norm_name = (name or "").strip().lower() or ("_id_:" + str(pkg_id))
            if norm_name not in name_to_canonical_and_members:
                name_to_canonical_and_members[norm_name] = (pkg_id, set(member_ids))
            else:
                name_to_canonical_and_members[norm_name][1].update(member_ids)

        packages_list = []
        for _norm_name, (pkg_id, member_ids) in name_to_canonical_and_members.items():
            pkg_obj = id_to_obj.get(pkg_id) or id_to_obj.get(str(pkg_id))
            name = (pkg_obj.name if pkg_obj else None) or str(pkg_id)
            counts: dict[str, int] = defaultdict(int)
            data_module_types: dict[str, int] = defaultdict(int)
            for oid in member_ids:
                obj = id_to_obj.get(oid)
                if not obj:
                    continue
                ot = _normalize_object_type(obj.object_type)
                if ot == "data_module":
                    counts["data_modules"] += 1
                    if self._is_main_data_module(obj):
                        counts["main_data_modules"] += 1
                    kind = self._get_data_module_type(obj)
                    data_module_types[kind] += 1
                elif ot == "table":
                    counts["tables"] += 1
                elif ot == "column":
                    counts["columns"] += 1
            complexity = "Medium" if counts["data_modules"] > 2 else "Low"
            packages_list.append({
                "package_id": str(pkg_id),
                "package_name": name,
                "complexity": complexity,
                "total_data_modules": counts["data_modules"],
                "main_data_modules": counts["main_data_modules"],
                "data_modules_by_type": dict(data_module_types),
                "total_tables": counts["tables"],
                "total_columns": counts["columns"],
            })
        total_packages = len(packages_list)
        _stats = {"low": 0, "medium": 0, "high": 0, "critical": 0}
        for p in packages_list:
            c = (p.get("complexity") or "").strip().lower()
            if c == "low": _stats["low"] += 1
            elif c == "medium": _stats["medium"] += 1
            elif c == "high": _stats["high"] += 1
            elif c == "critical": _stats["critical"] += 1
        return {
            "total_packages": total_packages,
            "stats": _stats,
            "packages": packages_list,
        }

    def _get_connection_properties(self, obj: ExtractedObject) -> dict[str, Any]:
        """Extract display-safe properties for a data source/connection (connection_type, identifier, storeID, etc.)."""
        out: dict[str, Any] = {}
        if not obj.properties or not isinstance(obj.properties, dict):
            return out
        props = obj.properties
        if props.get("storeID") is not None:
            out["identifier"] = str(props["storeID"]).strip() or None
        if props.get("identifier") is not None:
            out["identifier"] = out.get("identifier") or str(props["identifier"]).strip() or None
        if props.get("data_source_type") is not None:
            out["connection_type"] = str(props["data_source_type"]).strip() or None
        if props.get("connection_type") is not None:
            out["connection_type"] = out.get("connection_type") or str(props["connection_type"]).strip() or None
        if props.get("cognosClass") is not None:
            out["cognos_class"] = str(props["cognosClass"]).strip() or None
        # connection_string may be sensitive; expose only a hint (e.g. first 80 chars) or omit
        conn_str = props.get("connection_string")
        if conn_str and isinstance(conn_str, str):
            s = conn_str.strip()
            out["connection_string_preview"] = (s[:80] + "…") if len(s) > 80 else s if s else None
        return {k: v for k, v in out.items() if v is not None}

    def _get_data_source_connections_breakdown(
        self,
        objects: list[ExtractedObject],
        tree: ContainmentTree,
        relationships: list[ObjectRelationship],
    ) -> dict[str, Any]:
        """
        Data source connections breakdown: total data sources, data modules, packages;
        per connection: name, connection_type, identifier, and counts of dashboards and
        reports that use it (via CONNECTS_TO / USES / REFERENCES). Unique by normalized
        name or storeID; edge cases (unnamed, missing props) handled.
        """
        id_to_obj = tree.id_to_obj
        # Collect all data_source and data_source_connection objects
        connection_objects: list[tuple[Any, ExtractedObject]] = []
        for obj in objects:
            ot = _normalize_object_type(obj.object_type)
            if ot in ("data_source", "data_source_connection"):
                connection_objects.append((obj.id, obj))

        # Build forward usage graph: source -> [targets] for USES/REFERENCES/CONNECTS_TO (top-down traversal)
        usage_children: dict[Any, list[Any]] = defaultdict(list)
        for rel in relationships:
            rt = _normalize_rel_type(rel.relationship_type)
            if rt not in USAGE_REL_TYPES:
                continue
            src, tgt = rel.source_object_id, rel.target_object_id
            if src in id_to_obj and tgt in id_to_obj:
                usage_children[src].append(tgt)

        # Collect all dashboard and report root IDs
        dashboard_roots: set[Any] = set()
        report_roots: set[Any] = set()
        for oid, obj in id_to_obj.items():
            ot = _normalize_object_type(obj.object_type)
            if ot == "dashboard":
                dashboard_roots.add(oid)
            elif ot == "report":
                report_roots.add(oid)
        for obj in objects:
            root_id, root_kind = tree.get_root(obj.id)
            if root_id is not None and root_kind is not None:
                if root_kind == "dashboard":
                    dashboard_roots.add(root_id)
                elif root_kind == "report":
                    report_roots.add(root_id)

        # Top-down BFS from each root: follow both containment (root→tab→widget…) and usage (→data_module→data_source).
        connection_id_to_dashboard_roots: dict[Any, set[Any]] = defaultdict(set)
        connection_id_to_report_roots: dict[Any, set[Any]] = defaultdict(set)
        contains_children = tree.contains_children

        def bfs_reach_connections(root_id: Any, root_kind: str) -> None:
            seen: set[Any] = set()
            queue: list[Any] = [root_id]
            while queue:
                node = queue.pop(0)
                if node in seen:
                    continue
                seen.add(node)
                obj = id_to_obj.get(node)
                if obj:
                    ot = _normalize_object_type(obj.object_type)
                    if ot in ("data_source", "data_source_connection"):
                        if root_kind == "dashboard":
                            connection_id_to_dashboard_roots[node].add(root_id)
                        else:
                            connection_id_to_report_roots[node].add(root_id)
                for child in contains_children.get(node, []) + usage_children.get(node, []):
                    if child not in seen:
                        queue.append(child)

        for rid in dashboard_roots:
            bfs_reach_connections(rid, "dashboard")
        for rid in report_roots:
            bfs_reach_connections(rid, "report")

        def dashboards_and_reports_using(connection_ids: set[Any]) -> tuple[int, int]:
            dash: set[Any] = set()
            rep: set[Any] = set()
            for cid in connection_ids:
                dash.update(connection_id_to_dashboard_roots.get(cid, set()))
                rep.update(connection_id_to_report_roots.get(cid, set()))
            return len(dash), len(rep)

        # Deduplicate connections by stable key: storeID if present, else normalized name.
        # Track all connection IDs per key so we can union dashboard/report counts for merged rows.
        key_to_canonical: dict[str, tuple[Any, ExtractedObject]] = {}
        key_to_connection_ids: dict[str, set[Any]] = defaultdict(set)
        for oid, obj in connection_objects:
            props = (obj.properties or {}) if isinstance(obj.properties, dict) else {}
            store_id = props.get("storeID")
            if store_id is not None and str(store_id).strip():
                key = ("storeID:" + str(store_id).strip()).lower()
            else:
                name = (obj.name or "").strip()
                key = ("name:" + name.lower()) if name else ("_id_:" + str(oid))
            key_to_connection_ids[key].add(oid)
            if key not in key_to_canonical:
                key_to_canonical[key] = (oid, obj)

        # Summary totals (distinct object types in assessment)
        total_data_sources = sum(
            1 for _oid, o in connection_objects if _normalize_object_type(o.object_type) == "data_source"
        )
        total_data_source_connections = sum(
            1 for _oid, o in connection_objects if _normalize_object_type(o.object_type) == "data_source_connection"
        )
        total_data_modules = sum(
            1 for o in objects if _normalize_object_type(o.object_type) == "data_module"
        )
        total_packages = sum(
            1 for o in objects if _normalize_object_type(o.object_type) == "package"
        )
        # Unique connection count after dedupe
        total_unique_connections = len(key_to_canonical)

        connections_list: list[dict[str, Any]] = []
        for key, (conn_id, obj) in key_to_canonical.items():
            connection_ids_in_key = key_to_connection_ids.get(key, {conn_id})
            dash_count, report_count = dashboards_and_reports_using(connection_ids_in_key)
            extra = self._get_connection_properties(obj)
            name = (obj.name or "").strip() or f"<unnamed {obj.object_type}>"
            item: dict[str, Any] = {
                "connection_id": str(conn_id),
                "connection_name": name,
                "object_type": _normalize_object_type(obj.object_type),
                "complexity": "Medium",
                "dashboards_using_count": dash_count,
                "reports_using_count": report_count,
                **extra,
            }
            connections_list.append(item)

        _stats = {"low": 0, "medium": len(connections_list), "high": 0, "critical": 0}
        return {
            "total_data_sources": total_data_sources,
            "total_data_source_connections": total_data_source_connections,
            "total_unique_connections": total_unique_connections,
            "total_data_modules": total_data_modules,
            "total_packages": total_packages,
            "stats": _stats,
            "connections": connections_list,
        }

    def _safe_props(self, props: Any, keys: list[str], preview_len: Optional[int] = None) -> dict[str, Any]:
        """Extract keys from properties dict; optionally truncate long strings."""
        if not props or not isinstance(props, dict):
            return {}
        out: dict[str, Any] = {}
        for k in keys:
            v = props.get(k)
            if v is None:
                continue
            if preview_len and isinstance(v, str) and len(v) > preview_len:
                out[k] = v[:preview_len] + "…"
            elif isinstance(v, (list, dict)):
                out[k] = v
            else:
                out[k] = v
        return out

    def _get_prop_any_case(self, props: Any, *key_candidates: str) -> Any:
        """
        Get first non-None value from props for any of the keys, trying exact match
        then case-insensitive match (DB/JSON may store keys with different casing).
        """
        if not props or not isinstance(props, dict):
            return None
        for key in key_candidates:
            v = props.get(key)
            if v is not None:
                return v
        key_lower = {k.lower(): k for k in key_candidates}
        for pk, pv in props.items():
            if isinstance(pk, str) and pk.lower() in key_lower and pv is not None:
                return pv
        return None

    def _calculated_field_complexity(self, calculation_type: str, expression: Any) -> str:
        """
        Derive complexity for a calculated field from calculation_type and expression (props).
        - embeddedCalculation → Medium
        - case_expression | if_expression | aggregate_function | function → Low
        - expression: default Low; scan expression text:
          - critical: prefilter, quartile, power, position_regex, substring_regex, period
          - medium: cast, lookup, running-minimum, running-maximum, moving-total, moving-average, standard-deviation
          - else (arithmetic / default) → Low
        """
        ct = (calculation_type or "").strip().lower()
        if ct == "embeddedCalculation":
            return "Medium"
        if ct in ("case_expression", "if_expression", "aggregate_function", "function"):
            return "Low"
        # type "expression" or unknown: check expression content
        expr_str = (expression if isinstance(expression, str) else str(expression or "")).lower()
        # Critical first
        critical_terms = (
            "prefilter", "quartile", "quantile", "power", 
            "position_regex", "substring_regex", "period",
            "lookup", "running-maximum", "running-minimum",
            "moving-average", "standard-deviation",
            "regression-average", "tanhyp", "variance",
            "_ymdint_between", "_ymdint_to_date", "_ymdint_to_time",
        )
        for term in critical_terms:
            if term in expr_str:
                return "Critical"
        # Medium
        medium_terms = (
            "cast", "moving-total", 
            "_add_months", "_add_years", "_add_days", "_add_weeks", 
            "_add_months", "_add_years", "_add_days", "_add_weeks",
            "_first_of_month", "_days_between", "_first_of_year",
            "_months_between", "_years_between",
        )
        for term in medium_terms:
            if term in expr_str:
                return "Medium"
        # Default (arithmetic or plain expression) → Low
        return "Low"

    def _get_calculated_fields_breakdown(
        self,
        objects: list[ExtractedObject],
        tree: Optional[ContainmentTree] = None,
        relationships: Optional[list[ObjectRelationship]] = None,
    ) -> dict[str, Any]:
        """Total calculated fields and per-field details (expression, type, etc.).
        When tree is provided, adds dashboards_containing_count and reports_containing_count
        per field (0 or 1 from containment root; fallback via relationships or file when root is missing).
        Also builds by_complexity: per-level count of calculated fields and distinct
        dashboards/reports containing that complexity (for complex_analysis.calculated_field).
        """
        items: list[dict[str, Any]] = []
        file_container: dict[Any, str] = {}
        complexity_to_count: dict[str, int] = defaultdict(int)
        complexity_to_dash_roots: dict[str, set[Any]] = defaultdict(set)
        complexity_to_report_roots: dict[str, set[Any]] = defaultdict(set)
        relationships_by_target: dict[Any, list[Any]] = defaultdict(list)
        if tree is not None:
            file_container = self._file_to_container_type(objects)
            if relationships:
                for rel in relationships:
                    tgt = rel.target_object_id
                    relationships_by_target[tgt].append(rel.source_object_id)
                    if str(tgt) != tgt:
                        relationships_by_target[str(tgt)] = relationships_by_target[tgt]
        for obj in objects:
            if _normalize_object_type(obj.object_type) != "calculated_field":
                continue
            props = (obj.properties or {}) if isinstance(obj.properties, dict) else {}
            extra = self._safe_props(props, ["expression", "calculation_type", "cognosClass"], preview_len=500)
            calculation_type = (self._get_prop_any_case(props, "calculation_type") or "").strip() or "expression"
            expression_raw = self._get_prop_any_case(props, "expression", "formula", "calculation")
            complexity = self._calculated_field_complexity(calculation_type, expression_raw)
            c_key = (complexity or "").strip().lower()
            if c_key in ("low", "medium", "high", "critical"):
                complexity_to_count[c_key] += 1

            dashboards_containing_count = 0
            reports_containing_count = 0
            dash_root_key: Any = None
            report_root_key: Any = None
            if tree is not None:
                dashboards_containing_count, reports_containing_count, dash_root_key, report_root_key = (
                    self._resolve_containment_root(
                        obj, tree, file_container, relationships_by_target
                    )
                )
                if c_key in ("low", "medium", "high", "critical"):
                    if dash_root_key is not None:
                        complexity_to_dash_roots[c_key].add(dash_root_key)
                    if report_root_key is not None:
                        complexity_to_report_roots[c_key].add(report_root_key)

            items.append({
                "calculated_field_id": str(obj.id),
                "name": (obj.name or "").strip() or "<unnamed>",
                **extra,
                "complexity": complexity,
                "dashboards_containing_count": dashboards_containing_count,
                "reports_containing_count": reports_containing_count,
            })
        by_complexity: dict[str, dict[str, Any]] = {
            level: {
                "complexity": level,
                "calculated_field_count": complexity_to_count.get(level, 0),
                "dashboards_containing_count": len(complexity_to_dash_roots.get(level, set())),
                "reports_containing_count": len(complexity_to_report_roots.get(level, set())),
            }
            for level in ("low", "medium", "high", "critical")
        }
        return {
            "total_calculated_fields": len(items),
            "calculated_fields": items,
            "by_complexity": by_complexity,
        }

    def _get_filters_breakdown(
        self,
        objects: list[ExtractedObject],
        tree: Optional[ContainmentTree] = None,
        relationships: Optional[list[ObjectRelationship]] = None,
    ) -> dict[str, Any]:
        """
        Total filters and per-filter details.
        Includes: expression, filter_type (detail/summary), filter_scope (query_level/report_level/data_module/data_set),
        filter_style (expression/definition), is_simple/is_complex, ref_data_item, filter_definition_summary,
        postAutoAggregation, referenced_columns, parameter_references.
        When tree is provided, resolves parent_id to parent_name (report or query the filter is associated with),
        and adds dashboards_containing_count and reports_containing_count per filter (0 or 1 from containment root).
        Uses relationship fallback when filter is not in CONTAINS chain (e.g. linked via references/uses).
        Also builds by_complexity: per-level filter count and distinct dashboards/reports containing that complexity (for complex_analysis.filter).
        """
        id_to_obj = tree.id_to_obj if tree else {}
        file_container: dict[Any, str] = {}
        relationships_by_target: dict[Any, list[Any]] = defaultdict(list)
        complexity_to_count: dict[str, int] = defaultdict(int)
        complexity_to_dash_roots: dict[str, set[Any]] = defaultdict(set)
        complexity_to_report_roots: dict[str, set[Any]] = defaultdict(set)
        if tree is not None:
            file_container = self._file_to_container_type(objects)
            if relationships:
                for rel in relationships:
                    tgt = rel.target_object_id
                    relationships_by_target[tgt].append(rel.source_object_id)
                    if str(tgt) != tgt:
                        relationships_by_target[str(tgt)] = relationships_by_target[tgt]
        items: list[dict[str, Any]] = []
        for obj in objects:
            if _normalize_object_type(obj.object_type) != "filter":
                continue
            props = (obj.properties or {}) if isinstance(obj.properties, dict) else {}
            extra = self._safe_props(
                props,
                [
                    "expression", "filter_type", "filter_scope", "filter_style",
                    "is_simple", "is_complex", "ref_data_item", "filter_definition_summary",
                    "postAutoAggregation", "referenced_columns", "parameter_references", "cognosClass",
                ],
                preview_len=500,
            )
            is_complex = props.get("is_complex") is True
            complexity = "Medium" if is_complex else "Low"
            c_key = (complexity or "").strip().lower()
            if c_key in ("low", "medium", "high", "critical"):
                complexity_to_count[c_key] += 1

            dashboards_containing_count = 0
            reports_containing_count = 0
            dash_root_key: Any = None
            report_root_key: Any = None
            if tree is not None:
                dashboards_containing_count, reports_containing_count, dash_root_key, report_root_key = (
                    self._resolve_containment_root(
                        obj, tree, file_container, relationships_by_target
                    )
                )
                if c_key in ("low", "medium", "high", "critical"):
                    if dash_root_key is not None:
                        complexity_to_dash_roots[c_key].add(dash_root_key)
                    if report_root_key is not None:
                        complexity_to_report_roots[c_key].add(report_root_key)

            item: dict[str, Any] = {
                "filter_id": str(obj.id),
                "complexity": complexity,
                "name": (obj.name or "").strip() or "<unnamed>",
                **extra,
                "dashboards_containing_count": dashboards_containing_count,
                "reports_containing_count": reports_containing_count,
            }
            parent_id = props.get("parent_id") or getattr(obj, "parent_id", None)
            if parent_id and id_to_obj:
                for pid in (parent_id, str(parent_id)):
                    parent_obj = id_to_obj.get(pid)
                    if parent_obj:
                        item["parent_name"] = (parent_obj.name or "").strip() or str(pid)
                        item["parent_id"] = str(pid)
                        ot = _normalize_object_type(parent_obj.object_type)
                        item["associated_container_type"] = ot  # report, query, data_module, etc.
                        break
            items.append(item)
        _stats = {"low": 0, "medium": 0, "high": 0, "critical": 0}
        for it in items:
            c = (it.get("complexity") or "").strip().lower()
            if c == "low": _stats["low"] += 1
            elif c == "medium": _stats["medium"] += 1
            elif c == "high": _stats["high"] += 1
            elif c == "critical": _stats["critical"] += 1
        by_complexity: dict[str, dict[str, Any]] = {
            level: {
                "complexity": level,
                "filter_count": complexity_to_count.get(level, 0),
                "dashboards_containing_count": len(complexity_to_dash_roots.get(level, set())),
                "reports_containing_count": len(complexity_to_report_roots.get(level, set())),
            }
            for level in ("low", "medium", "high", "critical")
        }
        return {"total_filters": len(items), "stats": _stats, "filters": items, "by_complexity": by_complexity}

    def _get_parameters_breakdown(self, objects: list[ExtractedObject]) -> dict[str, Any]:
        """Total parameters and per-parameter details (type, etc.)."""
        items: list[dict[str, Any]] = []
        for obj in objects:
            if _normalize_object_type(obj.object_type) != "parameter":
                continue
            props = (obj.properties or {}) if isinstance(obj.properties, dict) else {}
            extra = self._safe_props(props, ["parameter_type", "variable_type", "cognosClass"])
            items.append({
                "parameter_id": str(obj.id),
                "name": (obj.name or "").strip() or "<unnamed>",
                **extra,
                "complexity": "Medium",
            })
        _stats = {"low": 0, "medium": len(items), "high": 0, "critical": 0}
        return {"total_parameters": len(items), "stats": _stats, "parameters": items}

    def _get_sorts_breakdown(self, objects: list[ExtractedObject]) -> dict[str, Any]:
        """Total sorts and per-sort details (direction, sorted column, etc.)."""
        items: list[dict[str, Any]] = []
        for obj in objects:
            if _normalize_object_type(obj.object_type) != "sort":
                continue
            props = (obj.properties or {}) if isinstance(obj.properties, dict) else {}
            extra = self._safe_props(props, ["direction", "sorted_column", "sort_items", "cognosClass"])
            items.append({
                "sort_id": str(obj.id),
                "name": (obj.name or "").strip() or "<unnamed>",
                **extra,
                "complexity": "Low",
            })
        _stats = {"low": len(items), "medium": 0, "high": 0, "critical": 0}
        return {"total_sorts": len(items), "stats": _stats, "sorts": items}

    def _get_prompts_breakdown(self, objects: list[ExtractedObject]) -> dict[str, Any]:
        """Total prompts and per-prompt details (type, value, etc.)."""
        items: list[dict[str, Any]] = []
        for obj in objects:
            if _normalize_object_type(obj.object_type) != "prompt":
                continue
            props = (obj.properties or {}) if isinstance(obj.properties, dict) else {}
            extra = self._safe_props(props, ["prompt_type", "value", "cognosClass"], preview_len=500)
            items.append({
                "prompt_id": str(obj.id),
                "name": (obj.name or "").strip() or "<unnamed>",
                **extra,
                "complexity": "Medium",
            })
        _stats = {"low": 0, "medium": len(items), "high": 0, "critical": 0}
        return {"total_prompts": len(items), "stats": _stats, "prompts": items}

    def _get_data_module_properties(self, obj: ExtractedObject) -> dict[str, Any]:
        """Extract display-safe properties for a data module (from parser: storeID, cognosClass, is_main_module, etc.)."""
        out: dict[str, Any] = {}
        if not obj.properties or not isinstance(obj.properties, dict):
            return out
        props = obj.properties
        for key in (
            "storeID", "cognosClass", "is_main_module", "table_count", "column_count",
            "calculated_field_count", "filter_count", "creationTime", "modificationTime",
            "owner", "displaySequence", "hidden", "tenantID",
        ):
            v = props.get(key)
            if v is None:
                continue
            out[key] = v
        return {k: v for k, v in out.items() if v is not None}

    def _get_data_modules_breakdown(
        self,
        objects: list[ExtractedObject],
        tree: ContainmentTree,
        relationships: list[ObjectRelationship],
    ) -> dict[str, Any]:
        """
        Data modules breakdown: total data modules; per module: name, type (cognosClass),
        dashboards/reports that use it (top-down BFS), and parser details (table_count,
        column_count, calculated_field_count, filter_count, storeID, owner, etc.).
        Deduplicate by storeID or normalized name; edge cases (unnamed, missing props) handled.

        How main data modules are connected to dashboards/reports:
        - Edges used: CONTAINS, PARENT_CHILD (containment), and USES, REFERENCES, CONNECTS_TO (usage).
        - From each dashboard or report root we BFS along containment children (folder→package→dashboard,
          dashboard→tab→visualization, report→page→query/visualization) and usage children (source→target).
        - Any data_module reached is attributed to that root. Connection paths include:
          Report → USES → data_module (from metadataModel, module, metadataModelPackage in report props)
          Report → CONTAINS → query → USES → data_module
          Report → CONTAINS → page → CONTAINS → visualization → (usage) → data_module
          Dashboard → USES → data_module (from useSpec in dashboard)
          Dashboard → CONTAINS → tab → CONTAINS → visualization → (usage) → data_module
        - Main and sub modules are treated the same for this count; both get dashboards_using_count
          and reports_using_count when reached from a dashboard/report root.
        """
        id_to_obj = tree.id_to_obj
        module_objects: list[tuple[Any, ExtractedObject]] = []
        for obj in objects:
            if _normalize_object_type(obj.object_type) != "data_module":
                continue
            module_objects.append((obj.id, obj))

        usage_children: dict[Any, list[Any]] = defaultdict(list)
        for rel in relationships:
            rt = _normalize_rel_type(rel.relationship_type)
            if rt not in USAGE_REL_TYPES:
                continue
            src, tgt = rel.source_object_id, rel.target_object_id
            if src in id_to_obj and tgt in id_to_obj:
                usage_children[src].append(tgt)

        dashboard_roots: set[Any] = set()
        report_roots: set[Any] = set()
        for oid, obj in id_to_obj.items():
            ot = _normalize_object_type(obj.object_type)
            if ot == "dashboard":
                dashboard_roots.add(oid)
            elif ot == "report":
                report_roots.add(oid)
        for obj in objects:
            root_id, root_kind = tree.get_root(obj.id)
            if root_id is not None and root_kind is not None:
                if root_kind == "dashboard":
                    dashboard_roots.add(root_id)
                elif root_kind == "report":
                    report_roots.add(root_id)

        module_id_to_dashboard_roots: dict[Any, set[Any]] = defaultdict(set)
        module_id_to_report_roots: dict[Any, set[Any]] = defaultdict(set)
        contains_children = tree.contains_children

        def bfs_reach_modules(root_id: Any, root_kind: str) -> None:
            seen: set[Any] = set()
            queue: list[Any] = [root_id]
            while queue:
                node = queue.pop(0)
                if node in seen:
                    continue
                seen.add(node)
                obj = id_to_obj.get(node)
                if obj:
                    ot = _normalize_object_type(obj.object_type)
                    if ot == "data_module":
                        if root_kind == "dashboard":
                            module_id_to_dashboard_roots[node].add(root_id)
                        else:
                            module_id_to_report_roots[node].add(root_id)
                for child in contains_children.get(node, []) + usage_children.get(node, []):
                    if child not in seen:
                        queue.append(child)

        for rid in dashboard_roots:
            bfs_reach_modules(rid, "dashboard")
        for rid in report_roots:
            bfs_reach_modules(rid, "report")

        def dashboards_and_reports_using(module_ids: set[Any]) -> tuple[int, int]:
            dash: set[Any] = set()
            rep: set[Any] = set()
            for mid in module_ids:
                dash.update(module_id_to_dashboard_roots.get(mid, set()))
                rep.update(module_id_to_report_roots.get(mid, set()))
            return len(dash), len(rep)

        key_to_canonical: dict[str, tuple[Any, ExtractedObject]] = {}
        key_to_module_ids: dict[str, set[Any]] = defaultdict(set)
        for oid, obj in module_objects:
            props = (obj.properties or {}) if isinstance(obj.properties, dict) else {}
            store_id = props.get("storeID")
            if store_id is not None and str(store_id).strip():
                key = ("storeID:" + str(store_id).strip()).lower()
            else:
                name = (obj.name or "").strip()
                key = ("name:" + name.lower()) if name else ("_id_:" + str(oid))
            key_to_module_ids[key].add(oid)
            if key not in key_to_canonical:
                key_to_canonical[key] = (oid, obj)

        total_data_modules = len(module_objects)
        total_main_data_modules = sum(1 for _oid, obj in module_objects if self._is_main_data_module(obj))
        total_unique_modules = len(key_to_canonical)

        modules_list: list[dict[str, Any]] = []
        main_modules_list: list[dict[str, Any]] = []
        for key, (module_id, obj) in key_to_canonical.items():
            module_ids_in_key = key_to_module_ids.get(key, {module_id})
            dash_count, report_count = dashboards_and_reports_using(module_ids_in_key)
            extra = self._get_data_module_properties(obj)
            name = (obj.name or "").strip() or f"<unnamed data module>"
            item: dict[str, Any] = {
                "data_module_id": str(module_id),
                "name": name,
                "complexity": "Medium",
                "dashboards_using_count": dash_count,
                "reports_using_count": report_count,
                **extra,
            }
            modules_list.append(item)
            if self._is_main_data_module(obj):
                main_modules_list.append(item)

        _stats = {"low": 0, "medium": len(modules_list), "high": 0, "critical": 0}
        return {
            "total_data_modules": total_data_modules,
            "total_main_data_modules": total_main_data_modules,
            "total_unique_modules": total_unique_modules,
            "stats": _stats,
            "data_modules": modules_list,
            "main_data_modules": main_modules_list,
        }

    def _get_queries_breakdown(
        self,
        objects: list[ExtractedObject],
        tree: ContainmentTree,
    ) -> dict[str, Any]:
        """
        Per-query breakdown: name, source_type (model / query_ref / sql), simple vs complex,
        report that contains the query (from containment), and properties (sql_content etc.).
        Simple = model or sql; complex = query_ref (references another query).
        """
        id_to_obj = tree.id_to_obj
        items: list[dict[str, Any]] = []
        for obj in objects:
            if _normalize_object_type(obj.object_type) != "query":
                continue
            props = (obj.properties or {}) if isinstance(obj.properties, dict) else {}
            source_type = props.get("source_type") or "unknown"
            sql_content = props.get("sql_content")
            root_id, root_kind = tree.get_root(obj.id)
            report_id = str(root_id) if root_kind == "report" and root_id is not None else None
            report_name = None
            if report_id and root_id is not None:
                report_obj = id_to_obj.get(root_id)
                if report_obj:
                    report_name = (report_obj.name or "").strip() or None
            is_simple = source_type in ("model", "sql")
            is_complex = source_type == "query_ref"
            complexity = "Medium" if is_complex else "Low"
            extra = self._safe_props(props, ["cognosClass", "source_type", "sql_content"], preview_len=500)
            items.append({
                "query_id": str(obj.id),
                "name": (obj.name or "").strip() or "<unnamed query>",
                "source_type": source_type,
                "is_simple": is_simple,
                "is_complex": is_complex,
                "report_id": report_id,
                "report_name": report_name,
                **extra,
                "complexity": complexity,
            })
        _stats = {"low": 0, "medium": 0, "high": 0, "critical": 0}
        for it in items:
            c = (it.get("complexity") or "").strip().lower()
            if c == "low": _stats["low"] += 1
            elif c == "medium": _stats["medium"] += 1
            elif c == "high": _stats["high"] += 1
            elif c == "critical": _stats["critical"] += 1
        return {"total_queries": len(items), "stats": _stats, "queries": items}

    def _get_measures_breakdown(
        self,
        objects: list[ExtractedObject],
        tree: ContainmentTree,
        relationships: list[ObjectRelationship],
    ) -> dict[str, Any]:
        """
        Per-measure breakdown: name, aggregation type, parent data module (walk parent_id/HAS_COLUMN
        and CONTAINS up to data_module), and other properties. Simple measure = no or trivial
        aggregation; complex = non-trivial aggregation or expression.
        Complexity is derived from the expression property only (measures have no calculation_type);
        same expression-based rules as calculated fields (critical/medium/low terms).
        """
        id_to_obj = tree.id_to_obj
        parent_map: dict[Any, Any] = {}
        for rel in relationships:
            rt = _normalize_rel_type(rel.relationship_type)
            if rt in CONTAINMENT_REL_TYPES or rt == "has_column":
                src = rel.source_object_id
                tgt = rel.target_object_id
                if src in id_to_obj and tgt in id_to_obj:
                    parent_map[tgt] = src
        for oid, obj in id_to_obj.items():
            pid = (obj.properties or {}).get("parent_id") if isinstance(obj.properties, dict) else None
            if pid is not None:
                for cand_id in (pid, str(pid)):
                    if cand_id in id_to_obj:
                        parent_map[oid] = cand_id
                        break
        def get_parent_module_id(oid: Any) -> tuple[Any, Any]:
            current = oid
            seen: set[Any] = set()
            while current and current not in seen:
                seen.add(current)
                obj = id_to_obj.get(current)
                if obj and _normalize_object_type(obj.object_type) == "data_module":
                    return current, (obj.name or "").strip() or None
                current = parent_map.get(current)
            return None, None
        items: list[dict[str, Any]] = []
        for obj in objects:
            if _normalize_object_type(obj.object_type) != "measure":
                continue
            props = (obj.properties or {}) if isinstance(obj.properties, dict) else {}
            agg = props.get("regularAggregate") or props.get("aggregation") or ""
            is_simple = (agg or "").lower() in ("", "none", "none ")
            is_complex = not is_simple
            module_id, module_name = get_parent_module_id(obj.id)
            extra = self._safe_props(props, ["cognosClass", "regularAggregate", "datatype", "usage", "expression"], preview_len=300)
            # Measures have only "expression" (no calculation_type); same expression-based complexity rules as calculated fields
            expression_raw = self._get_prop_any_case(props, "expression", "formula", "calculation")
            complexity = self._calculated_field_complexity("expression", expression_raw)
            items.append({
                "measure_id": str(obj.id),
                "name": (obj.name or "").strip() or "<unnamed measure>",
                "aggregation": agg or None,
                "is_simple": is_simple,
                "is_complex": is_complex,
                "parent_module_id": str(module_id) if module_id is not None else None,
                "parent_module_name": module_name,
                **extra,
                "complexity": complexity,
            })
        return {"total_measures": len(items), "measures": items}

    def _get_dimensions_breakdown(
        self,
        objects: list[ExtractedObject],
        tree: ContainmentTree,
        relationships: list[ObjectRelationship],
    ) -> dict[str, Any]:
        """
        Per-dimension breakdown: name, parent data module (walk parent_id/HAS_COLUMN and
        CONTAINS up to data_module), and other properties. Simple = attribute/dimension
        without hierarchy; complex = hierarchy or expression.
        """
        id_to_obj = tree.id_to_obj
        parent_map: dict[Any, Any] = {}
        for rel in relationships:
            rt = _normalize_rel_type(rel.relationship_type)
            if rt in CONTAINMENT_REL_TYPES or rt == "has_column":
                src = rel.source_object_id
                tgt = rel.target_object_id
                if src in id_to_obj and tgt in id_to_obj:
                    parent_map[tgt] = src
        for oid, obj in id_to_obj.items():
            pid = (obj.properties or {}).get("parent_id") if isinstance(obj.properties, dict) else None
            if pid is not None:
                for cand_id in (pid, str(pid)):
                    if cand_id in id_to_obj:
                        parent_map[oid] = cand_id
                        break
        def get_parent_module_id(oid: Any) -> tuple[Any, Any]:
            current = oid
            seen: set[Any] = set()
            while current and current not in seen:
                seen.add(current)
                obj = id_to_obj.get(current)
                if obj and _normalize_object_type(obj.object_type) == "data_module":
                    return current, (obj.name or "").strip() or None
                current = parent_map.get(current)
            return None, None
        items: list[dict[str, Any]] = []
        for obj in objects:
            if _normalize_object_type(obj.object_type) != "dimension":
                continue
            props = (obj.properties or {}) if isinstance(obj.properties, dict) else {}
            usage = props.get("usage") or props.get("data_usage") or ""
            is_simple = (usage or "").lower() in ("attribute", "dimension", "")
            is_complex = not is_simple
            module_id, module_name = get_parent_module_id(obj.id)
            extra = self._safe_props(props, ["cognosClass", "usage", "datatype", "expression"], preview_len=300)
            # Dimensions have only "expression" (no calculation_type); same expression-based complexity as measures
            expression_raw = self._get_prop_any_case(props, "expression", "formula", "calculation")
            complexity = self._calculated_field_complexity("expression", expression_raw)
            items.append({
                "dimension_id": str(obj.id),
                "name": (obj.name or "").strip() or "<unnamed dimension>",
                "usage": usage or None,
                "is_simple": is_simple,
                "is_complex": is_complex,
                "parent_module_id": str(module_id) if module_id is not None else None,
                "parent_module_name": module_name,
                **extra,
                "complexity": complexity,
            })
        return {"total_dimensions": len(items), "dimensions": items}

