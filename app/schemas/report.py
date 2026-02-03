"""Schemas for assessment report API responses."""
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class VisualizationBreakdownItem(BaseModel):
    visualization: str
    count: int
    complexity: str = "Unknown"
    dashboards_containing_count: int = 0
    reports_containing_count: int = 0
    """Count of distinct queries that contain/use this visualization type."""
    queries_using_count: int = 0


class VisualizationComplexityStats(BaseModel):
    """Counts of visualizations by complexity (low, medium, high, critical)."""
    low: int = 0
    medium: int = 0
    high: int = 0
    critical: int = 0


class VisualizationByComplexityItem(BaseModel):
    """Per-complexity: visualization count and distinct dashboards/reports containing that complexity."""
    complexity: str = "low"  # low | medium | high | critical
    visualization_count: int = 0
    dashboards_containing_count: int = 0
    reports_containing_count: int = 0
    """Feature from BigQuery Complex_Analysis_Feature (matched by feature_area & complexity)."""
    feature: Optional[str] = None


class DashboardByComplexityItem(BaseModel):
    """Per-complexity: distinct dashboards containing that complexity."""
    complexity: str = "low"  # low | medium | high | critical
    dashboards_containing_count: int = 0
    """Feature from BigQuery Complex_Analysis_Feature (matched by feature_area & complexity)."""
    feature: Optional[str] = None


class ReportByComplexityItem(BaseModel):
    """Per-complexity: distinct reports containing that complexity."""
    complexity: str = "low"  # low | medium | high | critical
    reports_containing_count: int = 0
    """Feature from BigQuery Complex_Analysis_Feature (matched by feature_area & complexity)."""
    feature: Optional[str] = None


class CalculatedFieldByComplexityItem(BaseModel):
    """Per-complexity: calculated field count and distinct dashboards/reports containing that complexity."""
    complexity: str = "low"  # low | medium | high | critical
    calculated_field_count: int = 0
    dashboards_containing_count: int = 0
    reports_containing_count: int = 0
    """Feature from BigQuery Complex_Analysis_Feature (matched by feature_area & complexity)."""
    feature: Optional[str] = None


class FilterByComplexityItem(BaseModel):
    """Per-complexity: filter count and distinct dashboards/reports containing that complexity."""
    complexity: str = "low"  # low | medium | high | critical
    filter_count: int = 0
    dashboards_containing_count: int = 0
    reports_containing_count: int = 0
    """Feature from BigQuery Complex_Analysis_Feature (matched by feature_area & complexity)."""
    feature: Optional[str] = None


class MeasureByComplexityItem(BaseModel):
    """Per-complexity: measure count and dashboards/reports containing."""
    complexity: str = "low"  # low | medium | high | critical
    measure_count: int = 0
    dashboards_containing_count: int = 0
    reports_containing_count: int = 0
    """Feature from BigQuery Complex_Analysis_Feature (matched by feature_area & complexity)."""
    feature: Optional[str] = None


class DimensionByComplexityItem(BaseModel):
    """Per-complexity: dimension count and dashboards/reports containing."""
    complexity: str = "low"  # low | medium | high | critical
    dimension_count: int = 0
    dashboards_containing_count: int = 0
    reports_containing_count: int = 0
    """Feature from BigQuery Complex_Analysis_Feature (matched by feature_area & complexity)."""
    feature: Optional[str] = None


class VisualizationDetails(BaseModel):
    total_visualization: int
    """Overall complexity from linear weighted model (low=1, medium=2, high=3, critical=4)."""
    overall_complexity: Optional[str] = None
    stats: VisualizationComplexityStats = Field(default_factory=lambda: VisualizationComplexityStats())
    by_complexity: Dict[str, VisualizationByComplexityItem] = Field(default_factory=dict)
    breakdown: list[VisualizationBreakdownItem]


class DashboardBreakdownItem(BaseModel):
    dashboard_id: str
    dashboard_name: str
    """Derived from visualizations_by_complexity: worst level present (Critical > High > Medium > Low)."""
    complexity: str = "Unknown"
    total_visualizations: int = 0
    visualizations_by_complexity: VisualizationComplexityStats = Field(
        default_factory=lambda: VisualizationComplexityStats()
    )
    """Weighted average of visualization complexities in this dashboard (linear: low=1, medium=2, high=3, critical=4)."""
    visualization_overall_complexity: Optional[str] = None
    """Viz type names used in this dashboard (e.g. Pie, Bar, Line)."""
    visualization_type_names: List[str] = Field(default_factory=list)
    total_tabs: int = 0
    total_measures: int = 0
    total_dimensions: int = 0
    total_calculated_fields: int = 0
    total_data_modules: int = 0
    total_packages: int = 0
    total_data_sources: int = 0


class DashboardsBreakdown(BaseModel):
    total_dashboards: int
    """Overall complexity from linear weighted model (low=1, medium=2, high=3, critical=4)."""
    overall_complexity: Optional[str] = None
    """Count of dashboards by derived complexity (low, medium, high, critical)."""
    stats: VisualizationComplexityStats = Field(default_factory=lambda: VisualizationComplexityStats())
    dashboards: list[DashboardBreakdownItem]


class ReportBreakdownItem(BaseModel):
    report_id: str
    report_name: str
    report_type: str = "report"  # report, interactiveReport, reportView, dataSet2, reportVersion
    """Derived from visualizations_by_complexity: worst level present (Critical > High > Medium > Low). Not affected by calculated_fields_by_complexity."""
    complexity: str = "Unknown"
    total_visualizations: int = 0
    visualizations_by_complexity: VisualizationComplexityStats = Field(
        default_factory=lambda: VisualizationComplexityStats()
    )
    """Counts of calculated fields in this report by complexity (informational only; does not affect report complexity)."""
    calculated_fields_by_complexity: VisualizationComplexityStats = Field(
        default_factory=lambda: VisualizationComplexityStats()
    )
    """Viz type names used in this report (e.g. Pie, Bar, Line)."""
    visualization_type_names: List[str] = Field(default_factory=list)
    total_pages: int = 0
    total_data_modules: int = 0
    total_packages: int = 0
    total_data_sources: int = 0
    total_tables: int = 0
    total_columns: int = 0
    total_filters: int = 0
    total_parameters: int = 0
    total_sorts: int = 0
    total_prompts: int = 0
    total_calculated_fields: int = 0
    total_measures: int = 0
    total_dimensions: int = 0


class ReportsBreakdown(BaseModel):
    total_reports: int
    """Overall complexity from linear weighted model (low=1, medium=2, high=3, critical=4)."""
    overall_complexity: Optional[str] = None
    """Count of reports by derived complexity (low, medium, high, critical)."""
    stats: VisualizationComplexityStats = Field(default_factory=lambda: VisualizationComplexityStats())
    reports: list[ReportBreakdownItem]


class PackageBreakdownItem(BaseModel):
    package_id: str
    package_name: str
    """Derived from data_modules count: > 2 → Medium, else Low."""
    complexity: str = "Low"
    total_data_modules: int = 0
    main_data_modules: int = 0  # module, dataModule, model only (excludes smartsModule, modelView, dataSet2)
    data_modules_by_type: Dict[str, int] = Field(default_factory=dict)  # smartsModule, dataModule, module, etc.
    total_tables: int = 0
    total_columns: int = 0
    """Distinct dashboards that use this package (BFS from roots via containment, usage, has_column reverse)."""
    dashboards_using_count: int = 0
    """Distinct reports that use this package."""
    reports_using_count: int = 0


class PackagesBreakdown(BaseModel):
    total_packages: int
    """Overall complexity from linear weighted model (low=1, medium=2, high=3, critical=4)."""
    overall_complexity: Optional[str] = None
    stats: VisualizationComplexityStats = Field(default_factory=lambda: VisualizationComplexityStats())
    packages: list[PackageBreakdownItem]


class DataSourceConnectionBreakdownItem(BaseModel):
    connection_id: str
    connection_name: str
    object_type: str  # data_source | data_source_connection
    """All data source connections: Medium."""
    complexity: str = "Medium"
    dashboards_using_count: int = 0
    reports_using_count: int = 0
    identifier: Optional[str] = None
    connection_type: Optional[str] = None
    cognos_class: Optional[str] = None
    connection_string_preview: Optional[str] = None
    # Allow extra keys from _get_connection_properties
    class Config:
        extra = "allow"


class DataSourceConnectionsBreakdown(BaseModel):
    total_data_sources: int
    total_data_source_connections: int
    total_unique_connections: int
    total_data_modules: int
    total_packages: int
    """Overall complexity from linear weighted model (low=1, medium=2, high=3, critical=4)."""
    overall_complexity: Optional[str] = None
    stats: VisualizationComplexityStats = Field(default_factory=lambda: VisualizationComplexityStats())
    connections: list[DataSourceConnectionBreakdownItem]


class CalculatedFieldBreakdownItem(BaseModel):
    calculated_field_id: str
    name: str
    """Derived from calculation_type and expression (embeddedCalculation→Medium; expression scanned for critical/medium terms)."""
    complexity: str = "Low"
    expression: Optional[str] = None
    calculation_type: Optional[str] = None
    cognos_class: Optional[str] = None
    """Count of dashboards containing this calculated field (0 or 1 per containment root)."""
    dashboards_containing_count: int = 0
    """Count of reports containing this calculated field (0 or 1 per containment root)."""
    reports_containing_count: int = 0
    class Config:
        extra = "allow"


class CalculatedFieldsBreakdown(BaseModel):
    total_calculated_fields: int
    """Overall complexity from linear weighted model (low=1, medium=2, high=3, critical=4)."""
    overall_complexity: Optional[str] = None
    calculated_fields: list[CalculatedFieldBreakdownItem]
    """Per-complexity: calculated field count and distinct dashboards/reports containing that complexity (for complex_analysis.calculated_field)."""
    by_complexity: Dict[str, CalculatedFieldByComplexityItem] = Field(default_factory=dict)


class FilterBreakdownItem(BaseModel):
    filter_id: str
    name: str
    """Derived from is_complex: True → Medium, else Low."""
    complexity: str = "Low"
    expression: Optional[str] = None
    filter_type: Optional[str] = None  # detail, summary
    filter_scope: Optional[str] = None  # query_level, report_level, data_module, data_set
    filter_style: Optional[str] = None  # expression, definition
    is_simple: Optional[bool] = None
    is_complex: Optional[bool] = None
    ref_data_item: Optional[str] = None
    filter_definition_summary: Optional[str] = None
    postAutoAggregation: Optional[str] = None
    parent_id: Optional[str] = None
    parent_name: Optional[str] = None
    associated_container_type: Optional[str] = None  # report, query, data_module
    """Count of dashboards containing this filter (0 or 1 per containment root)."""
    dashboards_containing_count: int = 0
    """Count of reports containing this filter (0 or 1 per containment root)."""
    reports_containing_count: int = 0
    referenced_columns: Optional[List[str]] = None
    parameter_references: Optional[List[str]] = None
    cognos_class: Optional[str] = None
    class Config:
        extra = "allow"


class FiltersBreakdown(BaseModel):
    total_filters: int
    """Overall complexity from linear weighted model (low=1, medium=2, high=3, critical=4)."""
    overall_complexity: Optional[str] = None
    stats: VisualizationComplexityStats = Field(default_factory=lambda: VisualizationComplexityStats())
    filters: list[FilterBreakdownItem]
    """Per-complexity: filter count and distinct dashboards/reports containing that complexity (for complex_analysis.filter)."""
    by_complexity: Dict[str, FilterByComplexityItem] = Field(default_factory=dict)


class ParameterBreakdownItem(BaseModel):
    parameter_id: str
    name: str
    """All parameters: Medium."""
    complexity: str = "Medium"
    parameter_type: Optional[str] = None
    variable_type: Optional[str] = None
    cognos_class: Optional[str] = None
    dashboards_containing_count: int = 0
    reports_containing_count: int = 0
    class Config:
        extra = "allow"


class ParameterByComplexityItem(BaseModel):
    """Per-complexity: parameter count and distinct dashboards/reports containing that complexity."""
    complexity: str = "low"
    parameter_count: int = 0
    dashboards_containing_count: int = 0
    reports_containing_count: int = 0
    """Feature from BigQuery Complex_Analysis_Feature (matched by feature_area & complexity)."""
    feature: Optional[str] = None


class ParametersBreakdown(BaseModel):
    total_parameters: int
    """Overall complexity from linear weighted model (low=1, medium=2, high=3, critical=4)."""
    overall_complexity: Optional[str] = None
    stats: VisualizationComplexityStats = Field(default_factory=lambda: VisualizationComplexityStats())
    parameters: list[ParameterBreakdownItem]
    by_complexity: Dict[str, ParameterByComplexityItem] = Field(default_factory=dict)


class SortBreakdownItem(BaseModel):
    sort_id: str
    name: str
    """All sorts: Low."""
    complexity: str = "Low"
    direction: Optional[str] = None
    sorted_column: Optional[str] = None
    sort_items: Optional[List[Any]] = None
    cognos_class: Optional[str] = None
    dashboards_containing_count: int = 0
    reports_containing_count: int = 0
    class Config:
        extra = "allow"


class SortByComplexityItem(BaseModel):
    """Per-complexity: sort count and distinct dashboards/reports containing that complexity."""
    complexity: str = "low"
    sort_count: int = 0
    dashboards_containing_count: int = 0
    reports_containing_count: int = 0
    """Feature from BigQuery Complex_Analysis_Feature (matched by feature_area & complexity)."""
    feature: Optional[str] = None


class SortsBreakdown(BaseModel):
    total_sorts: int
    """Overall complexity from linear weighted model (low=1, medium=2, high=3, critical=4)."""
    overall_complexity: Optional[str] = None
    stats: VisualizationComplexityStats = Field(default_factory=lambda: VisualizationComplexityStats())
    sorts: list[SortBreakdownItem]
    by_complexity: Dict[str, SortByComplexityItem] = Field(default_factory=dict)


class PromptBreakdownItem(BaseModel):
    prompt_id: str
    name: str
    """All prompts: Medium."""
    complexity: str = "Medium"
    prompt_type: Optional[str] = None
    value: Optional[str] = None
    cognos_class: Optional[str] = None
    dashboards_containing_count: int = 0
    reports_containing_count: int = 0
    class Config:
        extra = "allow"


class PromptByComplexityItem(BaseModel):
    """Per-complexity: prompt count and distinct dashboards/reports containing that complexity."""
    complexity: str = "low"
    prompt_count: int = 0
    dashboards_containing_count: int = 0
    reports_containing_count: int = 0
    """Feature from BigQuery Complex_Analysis_Feature (matched by feature_area & complexity)."""
    feature: Optional[str] = None


class PromptsBreakdown(BaseModel):
    total_prompts: int
    """Overall complexity from linear weighted model (low=1, medium=2, high=3, critical=4)."""
    overall_complexity: Optional[str] = None
    stats: VisualizationComplexityStats = Field(default_factory=lambda: VisualizationComplexityStats())
    prompts: list[PromptBreakdownItem]
    by_complexity: Dict[str, PromptByComplexityItem] = Field(default_factory=dict)


class QueryBreakdownItem(BaseModel):
    query_id: str
    name: str
    """Derived from is_complex: true → Medium, else Low."""
    complexity: str = "Low"
    source_type: Optional[str] = None  # model, query_ref, sql
    is_simple: bool = False
    is_complex: bool = False
    report_id: Optional[str] = None
    report_name: Optional[str] = None
    cognos_class: Optional[str] = None
    sql_content: Optional[str] = None
    dashboards_containing_count: int = 0
    reports_containing_count: int = 0
    class Config:
        extra = "allow"


class QueryByComplexityItem(BaseModel):
    """Per-complexity: query count and distinct dashboards/reports containing that complexity."""
    complexity: str = "low"
    query_count: int = 0
    dashboards_containing_count: int = 0
    reports_containing_count: int = 0
    """Feature from BigQuery Complex_Analysis_Feature (matched by feature_area & complexity)."""
    feature: Optional[str] = None


class QueriesBreakdown(BaseModel):
    total_queries: int
    """Overall complexity from linear weighted model (low=1, medium=2, high=3, critical=4)."""
    overall_complexity: Optional[str] = None
    stats: VisualizationComplexityStats = Field(default_factory=lambda: VisualizationComplexityStats())
    queries: list[QueryBreakdownItem]
    by_complexity: Dict[str, QueryByComplexityItem] = Field(default_factory=dict)


class MeasureBreakdownItem(BaseModel):
    measure_id: str
    name: str
    """Derived from expression (same rules as calculated fields: critical/medium/low terms)."""
    complexity: str = "Low"
    aggregation: Optional[str] = None
    is_simple: bool = False
    is_complex: bool = False
    parent_module_id: Optional[str] = None
    parent_module_name: Optional[str] = None
    cognos_class: Optional[str] = None
    datatype: Optional[str] = None
    usage: Optional[str] = None
    expression: Optional[str] = None
    class Config:
        extra = "allow"


class MeasuresBreakdown(BaseModel):
    total_measures: int
    """Overall complexity from linear weighted model (low=1, medium=2, high=3, critical=4)."""
    overall_complexity: Optional[str] = None
    measures: list[MeasureBreakdownItem]


class DimensionBreakdownItem(BaseModel):
    dimension_id: str
    name: str
    """Derived from expression (same rules as calculated fields/measures: critical/medium/low terms)."""
    complexity: str = "Low"
    usage: Optional[str] = None
    is_simple: bool = False
    is_complex: bool = False
    parent_module_id: Optional[str] = None
    parent_module_name: Optional[str] = None
    cognos_class: Optional[str] = None
    datatype: Optional[str] = None
    expression: Optional[str] = None
    class Config:
        extra = "allow"


class DimensionsBreakdown(BaseModel):
    total_dimensions: int
    """Overall complexity from linear weighted model (low=1, medium=2, high=3, critical=4)."""
    overall_complexity: Optional[str] = None
    dimensions: list[DimensionBreakdownItem]


class DataModuleBreakdownItem(BaseModel):
    data_module_id: str
    name: str
    """All data modules: Medium."""
    complexity: str = "Medium"
    dashboards_using_count: int = 0
    reports_using_count: int = 0
    is_main_module: Optional[bool] = None  # True for module/dataModule/model; False for smartsModule/modelView/dataSet2
    storeID: Optional[str] = None
    cognosClass: Optional[str] = None
    table_count: Optional[int] = None
    column_count: Optional[int] = None
    calculated_field_count: Optional[int] = None
    filter_count: Optional[int] = None
    creationTime: Optional[str] = None
    modificationTime: Optional[str] = None
    owner: Optional[str] = None
    displaySequence: Optional[int] = None
    hidden: Optional[bool] = None
    tenantID: Optional[str] = None
    class Config:
        extra = "allow"


class DataModulesBreakdown(BaseModel):
    total_data_modules: int
    total_main_data_modules: int = 0  # module, dataModule, model only (excludes smartsModule, modelView, dataSet2)
    total_unique_modules: int
    """Overall complexity from linear weighted model (low=1, medium=2, high=3, critical=4)."""
    overall_complexity: Optional[str] = None
    stats: VisualizationComplexityStats = Field(default_factory=lambda: VisualizationComplexityStats())
    data_modules: list[DataModuleBreakdownItem]
    main_data_modules: list[DataModuleBreakdownItem] = Field(default_factory=list)  # main-only list (module, dataModule, model)


class ReportSections(BaseModel):
    visualization_details: VisualizationDetails
    dashboards_breakdown: DashboardsBreakdown
    reports_breakdown: ReportsBreakdown
    packages_breakdown: PackagesBreakdown
    data_source_connections_breakdown: DataSourceConnectionsBreakdown
    calculated_fields_breakdown: CalculatedFieldsBreakdown
    filters_breakdown: FiltersBreakdown
    parameters_breakdown: ParametersBreakdown
    sorts_breakdown: SortsBreakdown
    prompts_breakdown: PromptsBreakdown
    data_modules_breakdown: DataModulesBreakdown
    queries_breakdown: QueriesBreakdown
    measures_breakdown: MeasuresBreakdown
    dimensions_breakdown: DimensionsBreakdown


class ComplexAnalysis(BaseModel):
    """Array of per-complexity stats: visualization count and dashboards/reports containing that complexity."""
    visualization: List[VisualizationByComplexityItem] = Field(default_factory=list)
    dashboard: List[DashboardByComplexityItem] = Field(default_factory=list)
    report: List[ReportByComplexityItem] = Field(default_factory=list)
    calculated_field: List[CalculatedFieldByComplexityItem] = Field(default_factory=list)
    filter: List[FilterByComplexityItem] = Field(default_factory=list)
    measure: List[MeasureByComplexityItem] = Field(default_factory=list)
    dimension: List[DimensionByComplexityItem] = Field(default_factory=list)
    parameter: List[ParameterByComplexityItem] = Field(default_factory=list)
    sort: List[SortByComplexityItem] = Field(default_factory=list)
    prompt: List[PromptByComplexityItem] = Field(default_factory=list)
    query: List[QueryByComplexityItem] = Field(default_factory=list)


class KeyFinding(BaseModel):
    """Key finding per feature area: representative complexity, count, and usage in dashboards/reports."""
    feature_area: str
    complexity: str
    count: int
    dashboards_summary: str
    reports_summary: str
    dashboards_percent: float = 0.0
    reports_percent: float = 0.0


class HighLevelComplexityOverviewItem(BaseModel):
    """Per-complexity level: counts for Visualization, Dashboard, Report."""
    complexity: str
    visualization_count: int = 0
    dashboard_count: int = 0
    report_count: int = 0


class InventoryItem(BaseModel):
    """Total count for an asset type (Dashboard, Report, Visualization, etc.)."""
    asset_type: str
    count: int


class Summary(BaseModel):
    """Report summary with key findings, high-level complexity overview, and inventory."""
    key_findings: List[KeyFinding] = Field(default_factory=list)
    """Per feature area: overall_complexity (linear), total count, and % of dashboards/reports containing any (union)."""
    overall_key_findings: List[KeyFinding] = Field(default_factory=list)
    """Final overall complexity: linear weighted average by count across all sections (Option 1)."""
    overall_complexity: Optional[str] = None
    high_level_complexity_overview: List[HighLevelComplexityOverviewItem] = Field(default_factory=list)
    inventory: List[InventoryItem] = Field(default_factory=list)


class ChallengeItem(BaseModel):
    """Per-visualization challenge: visualization name, type, complexity, description, recommended, and container name."""
    visualization: str
    visualization_type: str
    complexity: str
    description: Optional[str] = None
    recommended: Optional[str] = None
    dashboard_or_report_name: Optional[str] = None


class ChallengesResponse(BaseModel):
    """Challenges keyed by category; 'visualization' contains per-visualization challenge items."""
    visualization: List[ChallengeItem] = Field(default_factory=list)


class AppendixItem(BaseModel):
    """Appendix row: name, package(s), data module(s), owner (for dashboard or report)."""
    name: str
    package: List[str] = Field(default_factory=list)
    data_module: List[str] = Field(default_factory=list)
    owner: str = ""


class AppendixResponse(BaseModel):
    """Appendix: dashboards and reports with name, package(s), data module(s), owner."""
    dashboards: List[AppendixItem] = Field(default_factory=list)
    reports: List[AppendixItem] = Field(default_factory=list)


class AssessmentReportResponse(BaseModel):
    assessment_id: str
    sections: ReportSections
    complex_analysis: ComplexAnalysis = Field(default_factory=lambda: ComplexAnalysis())
    summary: Optional[Summary] = None
    challenges: Optional[ChallengesResponse] = None
    appendix: Optional[AppendixResponse] = None
    """Optional usage stats from usage_stats.json upload (usage_stats, content_creation, user_stats, performance, quick_wins, pilot_recommendations)."""
    usage_stats: Optional[Dict[str, Any]] = None
