"""
Service for report-related operations.
Generates reports for assessments by iterating over parsed objects one by one.
Only counts actual visualization types (Pie, Bar, Line, etc.); excludes
calculated_field, report, data_module, and other non-viz object types.

Uses a containment tree built from CONTAINS relationships so that
dashboard/report roots are resolved by traversing the graph (bottom-up or top-down).
"""
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional
import sys

# BigQuery: feature list for individual complexity (Visualization, etc.)
FEATURE_LIST_QUERY = (
    "SELECT feature_area, feature, complexity, feasibility, description, recommended "
    "FROM `tableau-to-looker-migration.C2L_Complexity_Rules.Looker_Perspective` "
    "LIMIT 1000"
)
FEATURE_AREA_VISUALIZATION = "Visualization"

# BigQuery: complex analysis feature lookup (feature_area + complexity -> feature)
COMPLEX_ANALYSIS_FEATURE_QUERY = (
    "SELECT feature_area, feature, complexity, `order` "
    "FROM `tableau-to-looker-migration.C2L_Complexity_Rules.Complex_Rules` "
    "LIMIT 1000"
)

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
# has_column: source = data_module, target = measure/column/dimension; follow reverse to reach data_module from contained items
HAS_COLUMN_REL_TYPE = "has_column"
ROOT_OBJECT_TYPES = frozenset({"dashboard", "report"})
COMPLEXITY_LEVELS = ("low", "medium", "high", "critical")


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

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


def _is_connection_object_type(ot: str) -> bool:
    """True if normalized object_type is data_source or data_source_connection (DB may store as datasource/datasourceconnection)."""
    if not ot:
        return False
    o = ot.replace("_", "")
    return o in ("datasource", "datasourceconnection")


def _build_complexity_stats(items: list[dict[str, Any]]) -> dict[str, int]:
    """Build stats dict counting items by complexity level."""
    stats = {"low": 0, "medium": 0, "high": 0, "critical": 0}
    for item in items:
        c = (item.get("complexity") or "").strip().lower()
        if c in stats:
            stats[c] += 1
    return stats


def _build_by_complexity(
    count_key: str,
    complexity_to_count: dict[str, int],
    complexity_to_dash_roots: dict[str, set[Any]],
    complexity_to_report_roots: dict[str, set[Any]],
) -> dict[str, dict[str, Any]]:
    """Build by_complexity dict for complex_analysis."""
    return {
        level: {
            "complexity": level,
            count_key: complexity_to_count.get(level, 0),
            "dashboards_containing_count": len(complexity_to_dash_roots.get(level, set())),
            "reports_containing_count": len(complexity_to_report_roots.get(level, set())),
        }
        for level in COMPLEXITY_LEVELS
    }


# =============================================================================
# CONTAINMENT TREE
# =============================================================================

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


# =============================================================================
# BREAKDOWN CONTEXT - Reusable state for breakdown methods
# =============================================================================

@dataclass
class BreakdownContext:
    """Reusable context for breakdown methods to avoid repeated setup code."""
    objects: list[ExtractedObject]
    tree: ContainmentTree
    relationships: list[ObjectRelationship]
    file_container: dict[Any, str] = field(default_factory=dict)
    relationships_by_target: dict[Any, list[Any]] = field(default_factory=lambda: defaultdict(list))
    
    def __post_init__(self):
        self._build_file_container()
        self._build_relationships_by_target()
    
    def _build_file_container(self) -> None:
        """Build file_id -> "dashboard"|"report" mapping."""
        file_types: dict[Any, set[str]] = defaultdict(set)
        for obj in self.objects:
            ot = _normalize_object_type(obj.object_type)
            if ot:
                file_types[obj.file_id].add(ot)
        for fid, types in file_types.items():
            if "dashboard" in types:
                self.file_container[fid] = "dashboard"
            elif "report" in types:
                self.file_container[fid] = "report"
    
    def _build_relationships_by_target(self) -> None:
        """Build target_id -> [source_ids] mapping."""
        for rel in self.relationships:
            tgt = rel.target_object_id
            self.relationships_by_target[tgt].append(rel.source_object_id)
            if str(tgt) != tgt:
                self.relationships_by_target[str(tgt)] = self.relationships_by_target[tgt]


# =============================================================================
# COMPLEXITY TRACKER - Tracks counts and roots by complexity level
# =============================================================================

@dataclass
class ComplexityTracker:
    """Tracks complexity counts and dashboard/report roots per complexity level."""
    count: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    dash_roots: dict[str, set[Any]] = field(default_factory=lambda: defaultdict(set))
    report_roots: dict[str, set[Any]] = field(default_factory=lambda: defaultdict(set))
    
    def add(self, complexity: str, dash_root_key: Any = None, report_root_key: Any = None) -> None:
        """Add an item with given complexity and optional root keys."""
        c_key = (complexity or "").strip().lower()
        if c_key in COMPLEXITY_LEVELS:
            self.count[c_key] += 1
            if dash_root_key is not None:
                self.dash_roots[c_key].add(dash_root_key)
            if report_root_key is not None:
                self.report_roots[c_key].add(report_root_key)
    
    def build_by_complexity(self, count_key: str) -> dict[str, dict[str, Any]]:
        """Build by_complexity dict for the response."""
        return _build_by_complexity(count_key, self.count, self.dash_roots, self.report_roots)


# =============================================================================
# REPORT SERVICE
# =============================================================================

class ReportService:
    def __init__(self, db: Session):
        self.db = db
        # Cached feature list from BigQuery (loaded once, reused for individual complexity)
        self._feature_list_cache: Optional[list[dict[str, Any]]] = None
        # Cached complex analysis feature lookup (feature_area + complexity -> feature)
        self._complex_analysis_feature_cache: Optional[list[dict[str, Any]]] = None

    # -------------------------------------------------------------------------
    # BigQuery and Feature List
    # -------------------------------------------------------------------------

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

    def _get_complex_analysis_feature_list(self) -> list[dict[str, Any]]:
        """
        Load Complex_Analysis_Feature from BigQuery once and cache on this instance.
        Used to add "feature" to each complex_analysis item by matching feature_area & complexity.
        """
        if self._complex_analysis_feature_cache is None:
            self._complex_analysis_feature_cache = self._run_bigquery_query(
                COMPLEX_ANALYSIS_FEATURE_QUERY
            )
        return self._complex_analysis_feature_cache

    def _get_complex_analysis_feature_lookup(self) -> dict[tuple[str, str], Optional[str]]:
        """
        Build lookup (feature_area_normalized, complexity_normalized) -> feature.
        feature_area from BigQuery (e.g. "Visualization", "Calculated Field") is normalized to
        match our entity keys (e.g. "visualization", "calculated_field"). Uses first occurrence if duplicates.
        """
        rows = self._get_complex_analysis_feature_list()
        lookup: dict[tuple[str, str], Optional[str]] = {}
        for r in rows:
            area = (r.get("feature_area") or "").strip()
            if not area:
                continue
            complexity = (r.get("complexity") or "").strip().lower()
            if not complexity:
                continue
            # Normalize feature_area to match our complex_analysis entity keys
            area_key = area.lower().replace(" ", "_")
            key = (area_key, complexity)
            if key not in lookup:
                feature = r.get("feature")
                lookup[key] = str(feature).strip() if feature is not None else None
        return lookup

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

    # -------------------------------------------------------------------------
    # Containment Tree
    # -------------------------------------------------------------------------

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

    # -------------------------------------------------------------------------
    # Main Report Generation
    # -------------------------------------------------------------------------

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
        
        # Generate all sections
        sections = report["sections"]
        sections["visualization_details"] = self._get_visualization_details(objects, tree, relationships)
        sections["dashboards_breakdown"] = self._get_dashboards_breakdown(objects, tree)
        sections["reports_breakdown"] = self._get_reports_breakdown(objects, tree, relationships)
        sections["packages_breakdown"] = self._get_packages_breakdown(objects, tree, relationships)
        sections["data_source_connections_breakdown"] = self._get_data_source_connections_breakdown(objects, tree, relationships)
        sections["calculated_fields_breakdown"] = self._get_calculated_fields_breakdown(objects, tree, relationships)
        sections["filters_breakdown"] = self._get_filters_breakdown(objects, tree, relationships)
        sections["parameters_breakdown"] = self._get_parameters_breakdown(objects, tree, relationships)
        sections["sorts_breakdown"] = self._get_sorts_breakdown(objects, tree, relationships)
        sections["prompts_breakdown"] = self._get_prompts_breakdown(objects, tree, relationships)
        sections["data_modules_breakdown"] = self._get_data_modules_breakdown(objects, tree, relationships)
        sections["queries_breakdown"] = self._get_queries_breakdown(objects, tree, relationships)
        sections["measures_breakdown"] = self._get_measures_breakdown(objects, tree, relationships)
        sections["dimensions_breakdown"] = self._get_dimensions_breakdown(objects, tree, relationships)

        # Build complex_analysis from sections
        report["complex_analysis"] = self._build_complex_analysis(sections)

        report["summary"] = self._build_summary(sections, report["complex_analysis"])

        report["challenges"] = self._get_challenges(objects, tree)

        report["appendix"] = self._get_appendix(objects, tree, relationships)

        # Include optional usage_stats from assessment (from usage_stats.json upload)
        report["usage_stats"] = getattr(assessment, "usage_stats", None)

        return report

    def _build_complex_analysis(self, sections: dict[str, Any]) -> dict[str, Any]:
        """Build complex_analysis from section data using a unified approach.
        Adds 'feature' to each item by matching feature_area & complexity from BigQuery
        (Complex_Analysis_Feature table).
        """
        # Lookup (feature_area_normalized, complexity) -> feature from BigQuery
        feature_lookup = self._get_complex_analysis_feature_lookup()

        # Entity configurations: (section_key, count_key, extra_keys)
        # entity_name is derived from count_key and must match BigQuery feature_area when normalized
        entity_configs = [
            ("visualization_details", "visualization_count", ["dashboards_containing_count", "reports_containing_count"]),
            ("calculated_fields_breakdown", "calculated_field_count", ["dashboards_containing_count", "reports_containing_count"]),
            ("filters_breakdown", "filter_count", ["dashboards_containing_count", "reports_containing_count"]),
            ("measures_breakdown", "measure_count", ["dashboards_containing_count", "reports_containing_count"]),
            ("dimensions_breakdown", "dimension_count", ["dashboards_containing_count", "reports_containing_count"]),
            ("parameters_breakdown", "parameter_count", ["dashboards_containing_count", "reports_containing_count"]),
            ("sorts_breakdown", "sort_count", ["dashboards_containing_count", "reports_containing_count"]),
            ("prompts_breakdown", "prompt_count", ["dashboards_containing_count", "reports_containing_count"]),
            ("queries_breakdown", "query_count", ["dashboards_containing_count", "reports_containing_count"]),
        ]

        complex_analysis: dict[str, Any] = {}

        # Process standard entity breakdowns
        for section_key, count_key, extra_keys in entity_configs:
            entity_name = count_key.replace("_count", "")
            by_complexity = sections.get(section_key, {}).get("by_complexity") or {}
            complex_analysis[entity_name] = []
            for level in COMPLEXITY_LEVELS:
                item: dict[str, Any] = {
                    "complexity": level,
                    count_key: by_complexity.get(level, {}).get(count_key, 0),
                    **{k: by_complexity.get(level, {}).get(k, 0) for k in extra_keys},
                }
                # Add feature from BigQuery: match feature_area (entity_name) & complexity
                item["feature"] = feature_lookup.get((entity_name, level))
                complex_analysis[entity_name].append(item)

        # Dashboard and report are special - they use stats instead of by_complexity
        dashboard_stats = sections.get("dashboards_breakdown", {}).get("stats") or {}
        complex_analysis["dashboard"] = []
        for level in COMPLEXITY_LEVELS:
            item = {"complexity": level, "dashboards_containing_count": dashboard_stats.get(level, 0)}
            item["feature"] = feature_lookup.get(("dashboard", level))
            complex_analysis["dashboard"].append(item)

        report_stats = sections.get("reports_breakdown", {}).get("stats") or {}
        complex_analysis["report"] = []
        for level in COMPLEXITY_LEVELS:
            item = {"complexity": level, "reports_containing_count": report_stats.get(level, 0)}
            item["feature"] = feature_lookup.get(("report", level))
            complex_analysis["report"].append(item)

        return complex_analysis

    def _build_summary(self, sections: dict[str, Any], complex_analysis: dict[str, Any]) -> dict[str, Any]:
        """Build report summary with key findings per feature area.
        For each feature area: representative complexity (critical if count exists, else high, else medium, else low),
        total count, and usage in dashboards/reports as percentages.
        Example key finding: (Visualization, Critical, 18, Used in 80% of Dashboards, Used in 47% of Reports).
        """
        total_dashboards = sections.get("dashboards_breakdown", {}).get("total_dashboards") or 0
        total_reports = sections.get("reports_breakdown", {}).get("total_reports") or 0

        feature_areas = [
            "visualization",
            "dashboard",
            "report",
            "calculated_field",
            "filter",
            "measure",
            "dimension",
            "parameter",
            "sort",
            "prompt",
            "query",
        ]

        key_findings: list[dict[str, Any]] = []

        for entity_name in feature_areas:
            items = complex_analysis.get(entity_name) or []
            if not items:
                continue
            if entity_name == "dashboard":
                count_key = "dashboards_containing_count"
            elif entity_name == "report":
                count_key = "reports_containing_count"
            else:
                count_key = f"{entity_name}_count"
            total_count = sum((item.get(count_key) or 0) for item in items)
            if total_count == 0:
                continue

            # Representative complexity: first level with count > 0 in order critical -> high -> medium -> low
            rep_complexity: Optional[str] = None
            rep_count = 0
            dashboards_containing_count = 0
            reports_containing_count = 0
            for level in ("critical", "high", "medium", "low"):
                for item in items:
                    if (item.get("complexity") or "").strip().lower() == level and (item.get(count_key) or 0) > 0:
                        rep_complexity = level.capitalize()
                        rep_count = item.get(count_key) or 0
                        dashboards_containing_count = item.get("dashboards_containing_count") or 0
                        reports_containing_count = item.get("reports_containing_count") or 0
                        break
                if rep_complexity is not None:
                    break
            if rep_complexity is None:
                for item in items:
                    if (item.get(count_key) or 0) > 0:
                        rep_complexity = (item.get("complexity") or "low").strip().capitalize()
                        rep_count = item.get(count_key) or 0
                        dashboards_containing_count = item.get("dashboards_containing_count") or 0
                        reports_containing_count = item.get("reports_containing_count") or 0
                        break
                if rep_complexity is None:
                    rep_complexity = "Low"

            pct_dash = round(100.0 * dashboards_containing_count / total_dashboards, 1) if total_dashboards else 0.0
            pct_reports = round(100.0 * reports_containing_count / total_reports, 1) if total_reports else 0.0

            feature_area_display = entity_name.replace("_", " ").title()

            key_findings.append({
                "feature_area": feature_area_display,
                "complexity": rep_complexity,
                "count": rep_count,
                "dashboards_summary": f"Used in {pct_dash:.1f}% of Dashboards",
                "reports_summary": f"Used in {pct_reports:.1f}% of Reports",
                "dashboards_percent": pct_dash,
                "reports_percent": pct_reports,
            })

        # High-level complexity overview by complexity level (Visualization, Dashboard, Report)
        high_level_complexity_overview: list[dict[str, Any]] = []
        viz_items = complex_analysis.get("visualization") or []
        dash_items = complex_analysis.get("dashboard") or []
        report_items = complex_analysis.get("report") or []
        for level in COMPLEXITY_LEVELS:
            viz_item = next((i for i in viz_items if (i.get("complexity") or "").strip().lower() == level), {})
            dash_item = next((i for i in dash_items if (i.get("complexity") or "").strip().lower() == level), {})
            report_item = next((i for i in report_items if (i.get("complexity") or "").strip().lower() == level), {})
            high_level_complexity_overview.append({
                "complexity": level.capitalize(),
                "visualization_count": viz_item.get("visualization_count") or 0,
                "dashboard_count": dash_item.get("dashboards_containing_count") or 0,
                "report_count": report_item.get("reports_containing_count") or 0,
            })

        # Inventory: total count of each asset type (Dashboard, Report, Visualization, etc.)
        def _section_total(section_key: str, total_key: str) -> int:
            return int(sections.get(section_key, {}).get(total_key) or 0)

        inventory: list[dict[str, Any]] = [
            {"asset_type": "Dashboard", "count": _section_total("dashboards_breakdown", "total_dashboards")},
            {"asset_type": "Report", "count": _section_total("reports_breakdown", "total_reports")},
            {"asset_type": "Visualization", "count": _section_total("visualization_details", "total_visualization")},
            {"asset_type": "Package", "count": _section_total("packages_breakdown", "total_packages")},
            {"asset_type": "Data Module", "count": _section_total("data_modules_breakdown", "total_data_modules")},
            {"asset_type": "Data Source / Connection", "count": _section_total("data_source_connections_breakdown", "total_unique_connections")},
            {"asset_type": "Calculated Field", "count": _section_total("calculated_fields_breakdown", "total_calculated_fields")},
            {"asset_type": "Filter", "count": _section_total("filters_breakdown", "total_filters")},
            {"asset_type": "Parameter", "count": _section_total("parameters_breakdown", "total_parameters")},
            {"asset_type": "Sort", "count": _section_total("sorts_breakdown", "total_sorts")},
            {"asset_type": "Prompt", "count": _section_total("prompts_breakdown", "total_prompts")},
            {"asset_type": "Query", "count": _section_total("queries_breakdown", "total_queries")},
            {"asset_type": "Measure", "count": _section_total("measures_breakdown", "total_measures")},
            {"asset_type": "Dimension", "count": _section_total("dimensions_breakdown", "total_dimensions")},
        ]

        return {
            "key_findings": key_findings,
            "high_level_complexity_overview": high_level_complexity_overview,
            "inventory": inventory,
        }

    # -------------------------------------------------------------------------
    # Helper Methods
    # -------------------------------------------------------------------------

    def _file_to_container_type(self, objects: list[ExtractedObject]) -> dict[Any, str]:
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
        Tries: (1) tree.get_root(obj.id), (2) BFS along relationships_by_target (object is target;
        follow sources like measure->data_module->query/report) until a dashboard/report root is found,
        (3) file_container fallback.
        """
        root_id, root_kind = tree.get_root(obj.id)
        if root_kind == "dashboard" and root_id is not None:
            return (1, 0, root_id, None)
        if root_kind == "report" and root_id is not None:
            return (0, 1, None, root_id)
        # Fallback: BFS — follow "who points to this?" (relationships_by_target) until we find a node with get_root() = dashboard/report
        # e.g. measure (target of has_column) -> data_module (target of uses) -> query/report -> get_root gives report
        seen: set[Any] = {obj.id, str(obj.id)} if obj.id is not None else set()
        queue: list[Any] = []
        for key in (obj.id, str(obj.id)):
            for src_id in relationships_by_target.get(key, []):
                if src_id not in seen:
                    seen.add(src_id)
                    queue.append(src_id)
        while queue:
            node = queue.pop(0)
            rid, rkind = tree.get_root(node)
            if rkind == "dashboard" and rid is not None:
                return (1, 0, rid, None)
            if rkind == "report" and rid is not None:
                return (0, 1, None, rid)
            for key in (node, str(node)):
                for src_id in relationships_by_target.get(key, []):
                    if src_id not in seen:
                        seen.add(src_id)
                        queue.append(src_id)
        # File fallback
        container = file_container.get(obj.file_id)
        if container == "dashboard":
            return (1, 0, ("file", obj.file_id), None)
        if container == "report":
            return (0, 1, None, ("file", obj.file_id))
        return (0, 0, None, None)

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
            viz = obj.properties.get("visualization_type") or obj.properties.get("type")
            if viz is not None:
                return str(viz)
        return obj.object_type or "unknown"

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

    def _derive_complexity_from_viz(self, viz_by_complexity: dict[str, int]) -> str:
        """Derive overall complexity from visualization counts by level (critical > high > medium > low)."""
        if viz_by_complexity.get("critical", 0) > 0:
            return "Critical"
        if viz_by_complexity.get("high", 0) > 0:
            return "High"
        if viz_by_complexity.get("medium", 0) > 0:
            return "Medium"
        if viz_by_complexity.get("low", 0) > 0:
            return "Low"
        return "Unknown"

    # -------------------------------------------------------------------------
    # BFS Helpers for Root Finding
    # -------------------------------------------------------------------------

    def _collect_dashboard_report_roots(
        self, 
        objects: list[ExtractedObject], 
        tree: ContainmentTree
    ) -> tuple[set[Any], set[Any]]:
        """Collect all dashboard and report root IDs."""
        id_to_obj = tree.id_to_obj
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
        return dashboard_roots, report_roots

    def _build_usage_graphs(
        self, 
        relationships: list[ObjectRelationship], 
        id_to_obj: dict[Any, ExtractedObject]
    ) -> tuple[dict[Any, list[Any]], dict[Any, list[Any]], dict[Any, list[Any]]]:
        """Build usage_children, usage_parents, and has_column_parents graphs."""
        usage_children: dict[Any, list[Any]] = defaultdict(list)
        usage_parents: dict[Any, list[Any]] = defaultdict(list)
        has_column_parents: dict[Any, list[Any]] = defaultdict(list)
        for rel in relationships:
            rt = _normalize_rel_type(rel.relationship_type)
            src, tgt = rel.source_object_id, rel.target_object_id
            if src not in id_to_obj or tgt not in id_to_obj:
                continue
            if rt in USAGE_REL_TYPES:
                usage_children[src].append(tgt)
                usage_parents[tgt].append(src)
            elif rt == HAS_COLUMN_REL_TYPE:
                has_column_parents[tgt].append(src)
        return usage_children, usage_parents, has_column_parents

    def _bfs_reach_objects_of_type(
        self,
        root_id: Any,
        root_kind: str,
        target_types: frozenset[str],
        tree: ContainmentTree,
        usage_children: dict[Any, list[Any]],
        usage_parents: dict[Any, list[Any]],
        has_column_parents: dict[Any, list[Any]],
        type_normalizer: Callable[[Any], bool],
    ) -> tuple[set[Any], str]:
        """
        BFS from root to find objects matching target types.
        Returns (set of matching object IDs, root_kind).
        """
        id_to_obj = tree.id_to_obj
        contains_children = tree.contains_children
        seen: set[Any] = set()
        queue: list[Any] = [root_id]
        matches: set[Any] = set()
        
        while queue:
            node = queue.pop(0)
            if node in seen:
                continue
            seen.add(node)
            obj = id_to_obj.get(node)
            if obj and type_normalizer(obj):
                matches.add(node)
            for child in (
                contains_children.get(node, [])
                + usage_children.get(node, [])
                + usage_parents.get(node, [])
                + has_column_parents.get(node, [])
            ):
                if child not in seen:
                    queue.append(child)
        
        return matches, root_kind

    # -------------------------------------------------------------------------
    # Deduplication Helper
    # -------------------------------------------------------------------------

    def _dedupe_by_store_id_or_name(
        self, 
        objects: list[tuple[Any, ExtractedObject]]
    ) -> tuple[dict[str, tuple[Any, ExtractedObject]], dict[str, set[Any]]]:
        """
        Deduplicate objects by storeID or normalized name.
        Returns (key_to_canonical, key_to_object_ids).
        """
        key_to_canonical: dict[str, tuple[Any, ExtractedObject]] = {}
        key_to_ids: dict[str, set[Any]] = defaultdict(set)
        
        for oid, obj in objects:
            props = (obj.properties or {}) if isinstance(obj.properties, dict) else {}
            store_id = props.get("storeID")
            if store_id is not None and str(store_id).strip():
                key = ("storeID:" + str(store_id).strip()).lower()
            else:
                name = (obj.name or "").strip()
                key = ("name:" + name.lower()) if name else ("_id_:" + str(oid))
            key_to_ids[key].add(oid)
            if key not in key_to_canonical:
                key_to_canonical[key] = (oid, obj)
        
        return key_to_canonical, key_to_ids

    # -------------------------------------------------------------------------
    # Parent Module Finding (for measures/dimensions)
    # -------------------------------------------------------------------------

    def _build_parent_map(
        self, 
        relationships: list[ObjectRelationship], 
        id_to_obj: dict[Any, ExtractedObject]
    ) -> dict[Any, Any]:
        """Build parent_map from relationships and object properties."""
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
        return parent_map

    def _get_parent_module_id(
        self, 
        oid: Any, 
        parent_map: dict[Any, Any], 
        id_to_obj: dict[Any, ExtractedObject]
    ) -> tuple[Any, Any]:
        """Walk up parent_map to find parent data_module."""
        current = oid
        seen: set[Any] = set()
        while current and current not in seen:
            seen.add(current)
            obj = id_to_obj.get(current)
            if obj and _normalize_object_type(obj.object_type) == "data_module":
                return current, (obj.name or "").strip() or None
            current = parent_map.get(current)
        return None, None

    # -------------------------------------------------------------------------
    # Generic Breakdown Method
    # -------------------------------------------------------------------------

    def _get_generic_breakdown(
        self,
        objects: list[ExtractedObject],
        tree: ContainmentTree,
        relationships: list[ObjectRelationship],
        object_type: str,
        id_field: str,
        count_key: str,
        complexity_fn: Callable[[ExtractedObject, dict[str, Any]], str],
        prop_keys: list[str],
        preview_len: Optional[int] = None,
        item_builder: Optional[Callable[[ExtractedObject, dict[str, Any], int, int], dict[str, Any]]] = None,
        default_complexity: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Generic breakdown method that handles the common pattern for many object types.
        """
        file_container = self._file_to_container_type(objects)
        relationships_by_target: dict[Any, list[Any]] = defaultdict(list)
        for rel in relationships:
            tgt = rel.target_object_id
            relationships_by_target[tgt].append(rel.source_object_id)
            if str(tgt) != tgt:
                relationships_by_target[str(tgt)] = relationships_by_target[tgt]
        
        tracker = ComplexityTracker()
        items: list[dict[str, Any]] = []
        
        for obj in objects:
            if _normalize_object_type(obj.object_type) != object_type:
                continue
            
            props = (obj.properties or {}) if isinstance(obj.properties, dict) else {}
            extra = self._safe_props(props, prop_keys, preview_len=preview_len)
            
            dashboards_count, reports_count, dash_root_key, report_root_key = self._resolve_containment_root(
                obj, tree, file_container, relationships_by_target
            )
            
            if complexity_fn:
                complexity = complexity_fn(obj, props)
            else:
                complexity = default_complexity or "Medium"
            
            tracker.add(complexity, dash_root_key, report_root_key)
            
            item = {
                id_field: str(obj.id),
                "name": (obj.name or "").strip() or "<unnamed>",
                **extra,
                "complexity": complexity,
                "dashboards_containing_count": dashboards_count,
                "reports_containing_count": reports_count,
            }
            
            if item_builder:
                item.update(item_builder(obj, props, dashboards_count, reports_count))
            
            items.append(item)
        
        stats = _build_complexity_stats(items)
        by_complexity = tracker.build_by_complexity(count_key)
        
        total_key = f"total_{object_type}s"
        return {
            total_key: len(items),
            "stats": stats,
            f"{object_type}s": items,
            "by_complexity": by_complexity,
        }

    # -------------------------------------------------------------------------
    # Visualization Details
    # -------------------------------------------------------------------------

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
            if c_key in COMPLEXITY_LEVELS:
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
            if c in stats:
                stats[c] += cnt
        
        overall_complexity = None
        # By complexity: visualization count and distinct dashboards/reports containing that complexity
        by_complexity = _build_by_complexity(
            "visualization_count", 
            stats, 
            complexity_to_dash_roots, 
            complexity_to_report_roots
        )

        return {
            "total_visualization": total,
            "overall_complexity": overall_complexity,
            "stats": stats,
            "by_complexity": by_complexity,
            "breakdown": breakdown,
        }

    # -------------------------------------------------------------------------
    # Challenges (per-visualization)
    # -------------------------------------------------------------------------

    def _get_challenges(
        self,
        objects: list[ExtractedObject],
        tree: ContainmentTree,
    ) -> dict[str, Any]:
        """
        Build challenges: dict with 'visualization' key containing list of per-viz entries
        (visualization name, visualization_type, complexity, description, recommended, dashboard/report name).
        """
        id_to_obj = tree.id_to_obj
        file_container = self._file_to_container_type(objects)
        viz_complexity_lookup = self._get_visualization_complexity_lookup()
        challenges: list[dict[str, Any]] = []

        for obj in objects:
            if not self._is_visualization_object(obj):
                continue
            viz_type = self._get_visualization_type_for_object(obj)
            key = (viz_type or "").strip().lower()
            info = viz_complexity_lookup.get(key) or {}
            complexity = (info.get("complexity") or "") or "Unknown"
            description = info.get("description")
            recommended = info.get("recommended")

            root_id, root_kind = tree.get_root(obj.id)
            dashboard_or_report_name: Optional[str] = None
            if root_kind == "dashboard" and root_id is not None:
                root_obj = id_to_obj.get(root_id)
                dashboard_or_report_name = (root_obj.name if root_obj else None) or str(root_id)
            elif root_kind == "report" and root_id is not None:
                root_obj = id_to_obj.get(root_id)
                dashboard_or_report_name = (root_obj.name if root_obj else None) or str(root_id)
            else:
                container = file_container.get(obj.file_id) if obj.file_id else None
                if container:
                    dashboard_or_report_name = f"Unknown ({container})"
                else:
                    dashboard_or_report_name = None

            challenges.append({
                "visualization": (obj.name or "").strip() or str(obj.id),
                "visualization_type": viz_type or "Unknown",
                "complexity": complexity,
                "description": description,
                "recommended": recommended,
                "dashboard_or_report_name": dashboard_or_report_name,
            })

        # Filter: only critical, high, medium (exclude low and Unknown)
        allowed_complexity = frozenset({"critical", "high", "medium"})
        filtered = [c for c in challenges if (c.get("complexity") or "").strip().lower() in allowed_complexity]

        # Order by complexity: critical first, then high, then medium
        complexity_order = {"critical": 0, "high": 1, "medium": 2}
        filtered.sort(key=lambda c: complexity_order.get((c.get("complexity") or "").strip().lower(), 99))

        return {
            "visualization": filtered,
        }

    # -------------------------------------------------------------------------
    # Dashboards Breakdown
    # -------------------------------------------------------------------------

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
                    if c in COMPLEXITY_LEVELS:
                        counts[f"visualizations_{c}"] += 1
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
            
            viz_by_complexity = {level: counts[f"visualizations_{level}"] for level in COMPLEXITY_LEVELS}
            dashboard_complexity = self._derive_complexity_from_viz(viz_by_complexity)
            
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
        dashboard_stats = _build_complexity_stats(dashboards_list)
        
        return {
            "total_dashboards": total_dashboards,
            "stats": dashboard_stats,
            "dashboards": dashboards_list,
        }
    
    # -------------------------------------------------------------------------
    # Reports Breakdown
    # -------------------------------------------------------------------------

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

    def _get_owner(self, obj: Optional[ExtractedObject]) -> str:
        """Extract owner from object properties (owner, Owner, etc.)."""
        if not obj or not obj.properties or not isinstance(obj.properties, dict):
            return ""
        return str(obj.properties.get("owner") or obj.properties.get("Owner") or "").strip()

    def _get_appendix(
        self,
        objects: list[ExtractedObject],
        tree: ContainmentTree,
        relationships: list[ObjectRelationship],
    ) -> dict[str, Any]:
        """
        Appendix: list of dashboards and reports with name, package(s), data module(s), and owner.
        Packages and data modules are resolved from containment and USES/REFERENCES/CONNECTS_TO.
        """
        id_to_obj = tree.id_to_obj

        # --- Dashboards: group by dashboard root ---
        dashboard_to_ids: dict[Any, set[Any]] = defaultdict(set)
        for obj in objects:
            root_id, root_kind = tree.get_root(obj.id)
            if root_kind == "dashboard" and root_id is not None:
                dashboard_to_ids[root_id].add(obj.id)
        for oid, obj in id_to_obj.items():
            ot = _normalize_object_type(obj.object_type)
            if ot == "dashboard":
                dashboard_to_ids[oid].add(oid)

        # Usage graph: source -> [targets] for USES/REFERENCES/CONNECTS_TO
        usage_children: dict[Any, list[Any]] = defaultdict(list)
        for rel in relationships:
            rt = _normalize_rel_type(rel.relationship_type)
            if rt not in USAGE_REL_TYPES:
                continue
            src, tgt = rel.source_object_id, rel.target_object_id
            if src in id_to_obj and tgt in id_to_obj:
                usage_children[src].append(tgt)

        def add_by_type(
            root_id: Any,
            oid: Any,
            ot: str,
            to_packages: dict[Any, set[Any]],
            to_data_modules: dict[Any, set[Any]],
        ) -> None:
            if ot == "package":
                to_packages[root_id].add(oid)
            elif ot == "data_module":
                to_data_modules[root_id].add(oid)

        dashboard_to_packages: dict[Any, set[Any]] = defaultdict(set)
        dashboard_to_data_modules: dict[Any, set[Any]] = defaultdict(set)

        # Dashboard: from containment
        for dash_id, member_ids in dashboard_to_ids.items():
            for oid in member_ids:
                obj = id_to_obj.get(oid)
                if not obj:
                    continue
                ot = _normalize_object_type(obj.object_type)
                add_by_type(dash_id, oid, ot, dashboard_to_packages, dashboard_to_data_modules)

        # Dashboard: bottom-to-top (USES from dashboard member -> package/data_module)
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
            if root_kind == "dashboard" and root_id is not None:
                add_by_type(root_id, tgt, tgt_ot, dashboard_to_packages, dashboard_to_data_modules)

        # Dashboard: top-down BFS from dashboard following usage_children
        for dash_id in dashboard_to_ids:
            seen: set[Any] = set()
            queue: list[Any] = [dash_id]
            while queue:
                node = queue.pop(0)
                if node in seen:
                    continue
                seen.add(node)
                obj = id_to_obj.get(node)
                if obj:
                    ot = _normalize_object_type(obj.object_type)
                    add_by_type(dash_id, node, ot, dashboard_to_packages, dashboard_to_data_modules)
                for child in usage_children.get(node, []):
                    if child not in seen:
                        queue.append(child)

        dashboards_list: list[dict[str, Any]] = []
        for dash_id in dashboard_to_ids:
            dash_obj = id_to_obj.get(dash_id)
            name = (dash_obj.name if dash_obj else None) or str(dash_id)
            owner = self._get_owner(dash_obj)
            package_names = sorted(
                set(
                    (id_to_obj.get(pid).name or str(pid)).strip() or str(pid)
                    for pid in dashboard_to_packages.get(dash_id, set())
                    if id_to_obj.get(pid)
                )
            )
            data_module_names = sorted(
                set(
                    (id_to_obj.get(mid).name or str(mid)).strip() or str(mid)
                    for mid in dashboard_to_data_modules.get(dash_id, set())
                    if id_to_obj.get(mid)
                )
            )
            dashboards_list.append({
                "name": name,
                "package": package_names,
                "data_module": data_module_names,
                "owner": owner,
            })

        # --- Reports: group by report root ---
        report_to_ids: dict[Any, set[Any]] = defaultdict(set)
        for obj in objects:
            root_id, root_kind = tree.get_root(obj.id)
            if root_kind == "report" and root_id is not None:
                report_to_ids[root_id].add(obj.id)
        for oid, obj in id_to_obj.items():
            ot = _normalize_object_type(obj.object_type)
            if ot == "report":
                report_to_ids[oid].add(oid)

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

        report_to_packages: dict[Any, set[Any]] = defaultdict(set)
        report_to_data_modules: dict[Any, set[Any]] = defaultdict(set)

        # Report: bottom-to-top
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
            add_by_type(root_id, tgt, tgt_ot, report_to_packages, report_to_data_modules)

        # Report: top-down from report
        for report_id in report_to_ids:
            seen_r: set[Any] = set()
            queue_r: list[Any] = [report_id]
            while queue_r:
                node = queue_r.pop(0)
                if node in seen_r:
                    continue
                seen_r.add(node)
                obj = id_to_obj.get(node)
                if obj:
                    ot = _normalize_object_type(obj.object_type)
                    add_by_type(report_id, node, ot, report_to_packages, report_to_data_modules)
                for child in usage_children.get(node, []):
                    if child not in seen_r:
                        queue_r.append(child)

        # Report: containment
        for report_id, member_ids in report_to_ids.items():
            for oid in member_ids:
                obj = id_to_obj.get(oid)
                if not obj:
                    continue
                ot = _normalize_object_type(obj.object_type)
                add_by_type(report_id, oid, ot, report_to_packages, report_to_data_modules)

        reports_list: list[dict[str, Any]] = []
        for report_id in report_to_ids:
            report_obj = id_to_obj.get(report_id)
            name = (report_obj.name if report_obj else None) or str(report_id)
            owner = self._get_owner(report_obj)
            package_names = sorted(
                set(
                    (id_to_obj.get(pid).name or str(pid)).strip() or str(pid)
                    for pid in report_to_packages.get(report_id, set())
                    if id_to_obj.get(pid)
                )
            )
            data_module_names = sorted(
                set(
                    (id_to_obj.get(mid).name or str(mid)).strip() or str(mid)
                    for mid in report_to_data_modules.get(report_id, set())
                    if id_to_obj.get(mid)
                )
            )
            reports_list.append({
                "name": name,
                "package": package_names,
                "data_module": data_module_names,
                "owner": owner,
            })

        return {
            "dashboards": dashboards_list,
            "reports": reports_list,
        }

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

        # Relationship fallback: objects not in CONTAINS chain
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

        # Data modules, packages, data sources: distinct counts
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

        # (1) Bottom-to-top
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

        # (2) Top-down from report
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

        # (3) Containment
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
                    if c in COMPLEXITY_LEVELS:
                        counts[f"visualizations_{c}"] += 1
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
                    if cf_c in COMPLEXITY_LEVELS:
                        counts[f"calculated_fields_{cf_c}"] += 1
                elif ot == "measure":
                    counts["measures"] += 1
                elif ot == "dimension":
                    counts["dimensions"] += 1
                elif ot == "table":
                    counts["tables"] += 1
                elif ot == "column":
                    counts["columns"] += 1
            
            # Tables/columns also from data modules used by this report
            for mid in report_to_data_modules.get(report_id, set()):
                mobj = id_to_obj.get(mid)
                if mobj and isinstance(mobj.properties, dict):
                    counts["tables"] += int(mobj.properties.get("table_count") or 0)
                    counts["columns"] += int(mobj.properties.get("column_count") or 0)
            
            viz_by_complexity = {level: counts[f"visualizations_{level}"] for level in COMPLEXITY_LEVELS}
            calculated_fields_by_complexity = {level: counts[f"calculated_fields_{level}"] for level in COMPLEXITY_LEVELS}

            # Report complexity
            if (report_type or "").strip().lower() == "interactivereport":
                report_complexity = "Critical"
            else:
                report_complexity = self._derive_complexity_from_viz(viz_by_complexity)

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
        report_stats = _build_complexity_stats(reports_list)
        
        return {
            "total_reports": total_reports,
            "stats": report_stats,
            "reports": reports_list,
        }

    # -------------------------------------------------------------------------
    # Packages Breakdown
    # -------------------------------------------------------------------------

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
        relationships: list[ObjectRelationship],
    ) -> dict[str, Any]:
        """
        Per-package breakdown: total packages, and for each package the counts of
        data modules (including by type: smartsModule, dataModule, module), tables, columns,
        and dashboards/reports using the package.
        """
        id_to_obj = tree.id_to_obj
        dashboard_roots, report_roots = self._collect_dashboard_report_roots(objects, tree)
        
        node_id_to_dashboard_roots: dict[Any, set[Any]] = defaultdict(set)
        node_id_to_report_roots: dict[Any, set[Any]] = defaultdict(set)
        contains_children = tree.contains_children
        contains_parent = tree.contains_parent

        def bfs_reach_nodes(root_id: Any, root_kind: str) -> None:
            seen: set[Any] = set()
            queue: list[Any] = [root_id]
            while queue:
                node = queue.pop(0)
                if node in seen:
                    continue
                seen.add(node)
                if root_kind == "dashboard":
                    node_id_to_dashboard_roots[node].add(root_id)
                else:
                    node_id_to_report_roots[node].add(root_id)
                for child in contains_children.get(node, []):
                    if child not in seen:
                        queue.append(child)
                parent = contains_parent.get(node)
                if parent is not None and parent not in seen:
                    queue.append(parent)

        for rid in dashboard_roots:
            bfs_reach_nodes(rid, "dashboard")
        for rid in report_roots:
            bfs_reach_nodes(rid, "report")

        def dashboards_and_reports_using(member_ids: set[Any]) -> tuple[int, int]:
            dash: set[Any] = set()
            rep: set[Any] = set()
            for mid in member_ids:
                dash.update(node_id_to_dashboard_roots.get(mid, set()) or node_id_to_dashboard_roots.get(str(mid), set()))
                rep.update(node_id_to_report_roots.get(mid, set()) or node_id_to_report_roots.get(str(mid), set()))
            return len(dash), len(rep)

        # Key by str(pkg_id)
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

        # Deduplicate by normalized name
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
            dash_count, report_count = dashboards_and_reports_using(member_ids)
            packages_list.append({
                "package_id": str(pkg_id),
                "package_name": name,
                "complexity": complexity,
                "total_data_modules": counts["data_modules"],
                "main_data_modules": counts["main_data_modules"],
                "data_modules_by_type": dict(data_module_types),
                "total_tables": counts["tables"],
                "total_columns": counts["columns"],
                "dashboards_using_count": dash_count,
                "reports_using_count": report_count,
            })
        
        total_packages = len(packages_list)
        _stats = _build_complexity_stats(packages_list)
        
        return {
            "total_packages": total_packages,
            "stats": _stats,
            "packages": packages_list,
        }

    # -------------------------------------------------------------------------
    # Data Source Connections Breakdown
    # -------------------------------------------------------------------------

    def _get_connection_properties(self, obj: ExtractedObject) -> dict[str, Any]:
        """Extract display-safe properties for a data source/connection."""
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
        reports that use it.
        """
        id_to_obj = tree.id_to_obj
        
        # Collect all data_source and data_source_connection objects
        connection_objects: list[tuple[Any, ExtractedObject]] = []
        for obj in objects:
            ot = _normalize_object_type(obj.object_type)
            if _is_connection_object_type(ot) or ot in ("data_source", "data_source_connection"):
                connection_objects.append((obj.id, obj))

        # Build usage graphs
        usage_children, usage_parents, has_column_parents = self._build_usage_graphs(relationships, id_to_obj)

        # Collect roots
        dashboard_roots, report_roots = self._collect_dashboard_report_roots(objects, tree)

        # Top-down BFS from each root
        connection_id_to_dashboard_roots: dict[Any, set[Any]] = defaultdict(set)
        connection_id_to_report_roots: dict[Any, set[Any]] = defaultdict(set)
        contains_children = tree.contains_children

        def _get_obj(oid: Any):
            obj = id_to_obj.get(oid)
            if obj is not None:
                return obj
            if oid is not None:
                return id_to_obj.get(str(oid))
            return None

        def bfs_reach_connections(root_id: Any, root_kind: str) -> None:
            seen: set[Any] = set()
            queue: list[Any] = [root_id]
            while queue:
                node = queue.pop(0)
                if node in seen:
                    continue
                seen.add(node)
                obj = _get_obj(node)
                if obj:
                    ot = _normalize_object_type(obj.object_type)
                    if _is_connection_object_type(ot) or ot in ("data_source", "data_source_connection"):
                        if root_kind == "dashboard":
                            connection_id_to_dashboard_roots[node].add(root_id)
                        else:
                            connection_id_to_report_roots[node].add(root_id)
                for child in (
                    contains_children.get(node, [])
                    + usage_children.get(node, [])
                    + usage_parents.get(node, [])
                    + has_column_parents.get(node, [])
                ):
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

        # Deduplicate
        key_to_canonical, key_to_connection_ids = self._dedupe_by_store_id_or_name(connection_objects)

        # Summary totals
        def _conn_is_data_source(otype: Any) -> bool:
            ot = _normalize_object_type(otype)
            return ot == "data_source" or (ot and ot.replace("_", "") == "datasource")
        def _conn_is_data_source_connection(otype: Any) -> bool:
            ot = _normalize_object_type(otype)
            return ot == "data_source_connection" or (ot and ot.replace("_", "") == "datasourceconnection")
        
        total_data_sources = sum(1 for _oid, o in connection_objects if _conn_is_data_source(o.object_type))
        total_data_source_connections = sum(1 for _oid, o in connection_objects if _conn_is_data_source_connection(o.object_type))
        total_data_modules = sum(1 for o in objects if _normalize_object_type(o.object_type) == "data_module")
        total_packages = sum(1 for o in objects if _normalize_object_type(o.object_type) == "package")
        total_unique_connections = len(key_to_canonical)

        connections_list: list[dict[str, Any]] = []
        for key, (conn_id, obj) in key_to_canonical.items():
            connection_ids_in_key = key_to_connection_ids.get(key, {conn_id})
            dash_count, report_count = dashboards_and_reports_using(connection_ids_in_key)
            extra = self._get_connection_properties(obj)
            name = (obj.name or "").strip() or f"<unnamed {obj.object_type}>"
            connections_list.append({
                "connection_id": str(conn_id),
                "connection_name": name,
                "object_type": _normalize_object_type(obj.object_type),
                "complexity": "Medium",
                "dashboards_using_count": dash_count,
                "reports_using_count": report_count,
                **extra,
            })

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

    # -------------------------------------------------------------------------
    # Calculated Fields Breakdown
    # -------------------------------------------------------------------------

    def _get_calculated_fields_breakdown(
        self,
        objects: list[ExtractedObject],
        tree: Optional[ContainmentTree] = None,
        relationships: Optional[list[ObjectRelationship]] = None,
    ) -> dict[str, Any]:
        """Total calculated fields and per-field details (expression, type, etc.)."""
        def complexity_fn(obj: ExtractedObject, props: dict[str, Any]) -> str:
            calc_type = (self._get_prop_any_case(props, "calculation_type") or "").strip() or "expression"
            expr_raw = self._get_prop_any_case(props, "expression", "formula", "calculation")
            return self._calculated_field_complexity(calc_type, expr_raw)
        
        result = self._get_generic_breakdown(
            objects=objects,
            tree=tree,
            relationships=relationships or [],
            object_type="calculated_field",
            id_field="calculated_field_id",
            count_key="calculated_field_count",
            complexity_fn=complexity_fn,
            prop_keys=["expression", "calculation_type", "cognosClass"],
            preview_len=500,
        )
        
        # Adjust key names for backward compatibility
        result["total_calculated_fields"] = result.pop("total_calculated_fields")
        result["calculated_fields"] = result.pop("calculated_fields")
        return result

    # -------------------------------------------------------------------------
    # Filters Breakdown
    # -------------------------------------------------------------------------

    def _get_filters_breakdown(
        self,
        objects: list[ExtractedObject],
        tree: Optional[ContainmentTree] = None,
        relationships: Optional[list[ObjectRelationship]] = None,
    ) -> dict[str, Any]:
        """Total filters and per-filter details."""
        id_to_obj = tree.id_to_obj if tree else {}
        
        def complexity_fn(obj: ExtractedObject, props: dict[str, Any]) -> str:
            is_complex = props.get("is_complex") is True
            return "Medium" if is_complex else "Low"
        
        def item_builder(obj: ExtractedObject, props: dict[str, Any], dash_count: int, rep_count: int) -> dict[str, Any]:
            extra_fields: dict[str, Any] = {}
            parent_id = props.get("parent_id") or getattr(obj, "parent_id", None)
            if parent_id and id_to_obj:
                for pid in (parent_id, str(parent_id)):
                    parent_obj = id_to_obj.get(pid)
                    if parent_obj:
                        extra_fields["parent_name"] = (parent_obj.name or "").strip() or str(pid)
                        extra_fields["parent_id"] = str(pid)
                        ot = _normalize_object_type(parent_obj.object_type)
                        extra_fields["associated_container_type"] = ot
                        break
            return extra_fields
        
        result = self._get_generic_breakdown(
            objects=objects,
            tree=tree,
            relationships=relationships or [],
            object_type="filter",
            id_field="filter_id",
            count_key="filter_count",
            complexity_fn=complexity_fn,
            prop_keys=[
                "expression", "filter_type", "filter_scope", "filter_style",
                "is_simple", "is_complex", "ref_data_item", "filter_definition_summary",
                "postAutoAggregation", "referenced_columns", "parameter_references", "cognosClass",
            ],
            preview_len=500,
            item_builder=item_builder,
        )
        
        result["total_filters"] = result.pop("total_filters")
        result["filters"] = result.pop("filters")
        return result

    # -------------------------------------------------------------------------
    # Parameters Breakdown
    # -------------------------------------------------------------------------

    def _get_parameters_breakdown(
        self,
        objects: list[ExtractedObject],
        tree: ContainmentTree,
        relationships: list[ObjectRelationship],
    ) -> dict[str, Any]:
        """Total parameters and per-parameter details."""
        result = self._get_generic_breakdown(
            objects=objects,
            tree=tree,
            relationships=relationships,
            object_type="parameter",
            id_field="parameter_id",
            count_key="parameter_count",
            complexity_fn=None,
            default_complexity="Medium",
            prop_keys=["parameter_type", "variable_type", "cognosClass"],
        )
        
        result["total_parameters"] = result.pop("total_parameters")
        result["parameters"] = result.pop("parameters")
        return result

    # -------------------------------------------------------------------------
    # Sorts Breakdown
    # -------------------------------------------------------------------------

    def _get_sorts_breakdown(
        self,
        objects: list[ExtractedObject],
        tree: ContainmentTree,
        relationships: list[ObjectRelationship],
    ) -> dict[str, Any]:
        """Total sorts and per-sort details."""
        result = self._get_generic_breakdown(
            objects=objects,
            tree=tree,
            relationships=relationships,
            object_type="sort",
            id_field="sort_id",
            count_key="sort_count",
            complexity_fn=None,
            default_complexity="Low",
            prop_keys=["direction", "sorted_column", "sort_items", "cognosClass"],
        )
        
        result["total_sorts"] = result.pop("total_sorts")
        result["sorts"] = result.pop("sorts")
        return result

    # -------------------------------------------------------------------------
    # Prompts Breakdown
    # -------------------------------------------------------------------------

    def _get_prompts_breakdown(
        self,
        objects: list[ExtractedObject],
        tree: ContainmentTree,
        relationships: list[ObjectRelationship],
    ) -> dict[str, Any]:
        """Total prompts and per-prompt details."""
        result = self._get_generic_breakdown(
            objects=objects,
            tree=tree,
            relationships=relationships,
            object_type="prompt",
            id_field="prompt_id",
            count_key="prompt_count",
            complexity_fn=None,
            default_complexity="Medium",
            prop_keys=["prompt_type", "value", "cognosClass"],
            preview_len=500,
        )
        
        result["total_prompts"] = result.pop("total_prompts")
        result["prompts"] = result.pop("prompts")
        return result

    # -------------------------------------------------------------------------
    # Data Modules Breakdown
    # -------------------------------------------------------------------------

    def _get_data_module_properties(self, obj: ExtractedObject) -> dict[str, Any]:
        """Extract display-safe properties for a data module."""
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
        dashboards/reports that use it, and parser details.
        """
        id_to_obj = tree.id_to_obj
        module_objects: list[tuple[Any, ExtractedObject]] = []
        for obj in objects:
            if _normalize_object_type(obj.object_type) != "data_module":
                continue
            module_objects.append((obj.id, obj))

        usage_children, usage_parents, has_column_parents = self._build_usage_graphs(relationships, id_to_obj)
        dashboard_roots, report_roots = self._collect_dashboard_report_roots(objects, tree)

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
                for child in (
                    contains_children.get(node, [])
                    + usage_children.get(node, [])
                    + usage_parents.get(node, [])
                    + has_column_parents.get(node, [])
                ):
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

        key_to_canonical, key_to_module_ids = self._dedupe_by_store_id_or_name(module_objects)

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

    # -------------------------------------------------------------------------
    # Queries Breakdown
    # -------------------------------------------------------------------------

    def _get_queries_breakdown(
        self,
        objects: list[ExtractedObject],
        tree: ContainmentTree,
        relationships: list[ObjectRelationship],
    ) -> dict[str, Any]:
        """
        Per-query breakdown: name, source_type (model / query_ref / sql), simple vs complex,
        report that contains the query, dashboard/report counts, and by_complexity.
        """
        id_to_obj = tree.id_to_obj
        file_container = self._file_to_container_type(objects)
        relationships_by_target: dict[Any, list[Any]] = defaultdict(list)
        for rel in relationships:
            tgt = rel.target_object_id
            relationships_by_target[tgt].append(rel.source_object_id)
            if str(tgt) != tgt:
                relationships_by_target[str(tgt)] = relationships_by_target[tgt]
        
        tracker = ComplexityTracker()
        items: list[dict[str, Any]] = []
        
        for obj in objects:
            if _normalize_object_type(obj.object_type) != "query":
                continue
            props = (obj.properties or {}) if isinstance(obj.properties, dict) else {}
            source_type = props.get("source_type") or "unknown"
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
            
            dashboards_count, reports_count, dash_root_key, report_root_key = self._resolve_containment_root(
                obj, tree, file_container, relationships_by_target
            )
            tracker.add(complexity, dash_root_key, report_root_key)
            
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
                "dashboards_containing_count": dashboards_count,
                "reports_containing_count": reports_count,
            })
        
        _stats = _build_complexity_stats(items)
        by_complexity = tracker.build_by_complexity("query_count")
        
        return {"total_queries": len(items), "stats": _stats, "queries": items, "by_complexity": by_complexity}

    # -------------------------------------------------------------------------
    # Measures Breakdown
    # -------------------------------------------------------------------------

    def _get_measures_breakdown(
        self,
        objects: list[ExtractedObject],
        tree: ContainmentTree,
        relationships: list[ObjectRelationship],
    ) -> dict[str, Any]:
        """
        Per-measure breakdown: name, aggregation type, parent data module, and other properties.
        Complexity is derived from the expression property.
        """
        id_to_obj = tree.id_to_obj
        parent_map = self._build_parent_map(relationships, id_to_obj)
        file_container = self._file_to_container_type(objects)
        relationships_by_target: dict[Any, list[Any]] = defaultdict(list)
        for rel in relationships:
            tgt = rel.target_object_id
            relationships_by_target[tgt].append(rel.source_object_id)
            if str(tgt) != tgt:
                relationships_by_target[str(tgt)] = relationships_by_target[tgt]
        
        tracker = ComplexityTracker()
        items: list[dict[str, Any]] = []
        
        for obj in objects:
            if _normalize_object_type(obj.object_type) != "measure":
                continue
            props = (obj.properties or {}) if isinstance(obj.properties, dict) else {}
            agg = props.get("regularAggregate") or props.get("aggregation") or ""
            is_simple = (agg or "").lower() in ("", "none", "none ")
            is_complex = not is_simple
            module_id, module_name = self._get_parent_module_id(obj.id, parent_map, id_to_obj)
            
            dashboards_count, reports_count, dash_root_key, report_root_key = self._resolve_containment_root(
                obj, tree, file_container, relationships_by_target
            )
            
            extra = self._safe_props(props, ["cognosClass", "regularAggregate", "datatype", "usage", "expression"], preview_len=300)
            expression_raw = self._get_prop_any_case(props, "expression", "formula", "calculation")
            complexity = self._calculated_field_complexity("expression", expression_raw)
            tracker.add(complexity, dash_root_key, report_root_key)
            
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
                "dashboards_containing_count": dashboards_count,
                "reports_containing_count": reports_count,
            })
        
        by_complexity = tracker.build_by_complexity("measure_count")
        return {"total_measures": len(items), "measures": items, "by_complexity": by_complexity}

    # -------------------------------------------------------------------------
    # Dimensions Breakdown
    # -------------------------------------------------------------------------

    def _get_dimensions_breakdown(
        self,
        objects: list[ExtractedObject],
        tree: ContainmentTree,
        relationships: list[ObjectRelationship],
    ) -> dict[str, Any]:
        """
        Per-dimension breakdown: name, parent data module, and other properties.
        Complexity is derived from the expression property.
        """
        id_to_obj = tree.id_to_obj
        parent_map = self._build_parent_map(relationships, id_to_obj)
        file_container = self._file_to_container_type(objects)
        relationships_by_target: dict[Any, list[Any]] = defaultdict(list)
        for rel in relationships:
            tgt = rel.target_object_id
            relationships_by_target[tgt].append(rel.source_object_id)
            if str(tgt) != tgt:
                relationships_by_target[str(tgt)] = relationships_by_target[tgt]
        
        tracker = ComplexityTracker()
        items: list[dict[str, Any]] = []
        
        for obj in objects:
            if _normalize_object_type(obj.object_type) != "dimension":
                continue
            props = (obj.properties or {}) if isinstance(obj.properties, dict) else {}
            usage = props.get("usage") or props.get("data_usage") or ""
            is_simple = (usage or "").lower() in ("attribute", "dimension", "")
            is_complex = not is_simple
            module_id, module_name = self._get_parent_module_id(obj.id, parent_map, id_to_obj)
            
            dashboards_count, reports_count, dash_root_key, report_root_key = self._resolve_containment_root(
                obj, tree, file_container, relationships_by_target
            )
            
            extra = self._safe_props(props, ["cognosClass", "usage", "datatype", "expression"], preview_len=300)
            expression_raw = self._get_prop_any_case(props, "expression", "formula", "calculation")
            complexity = self._calculated_field_complexity("expression", expression_raw)
            tracker.add(complexity, dash_root_key, report_root_key)
            
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
                "dashboards_containing_count": dashboards_count,
                "reports_containing_count": reports_count,
            })
        
        by_complexity = tracker.build_by_complexity("dimension_count")
        return {"total_dimensions": len(items), "dimensions": items, "by_complexity": by_complexity}
