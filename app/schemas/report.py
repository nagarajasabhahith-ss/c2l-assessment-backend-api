"""Schemas for assessment report API responses."""
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class VisualizationBreakdownItem(BaseModel):
    visualization: str
    count: int
    dashboards_containing_count: int = 0
    reports_containing_count: int = 0


class VisualizationDetails(BaseModel):
    total_visualization: int
    breakdown: list[VisualizationBreakdownItem]


class DashboardBreakdownItem(BaseModel):
    dashboard_id: str
    dashboard_name: str
    total_visualizations: int = 0
    total_tabs: int = 0
    total_measures: int = 0
    total_dimensions: int = 0
    total_calculated_fields: int = 0
    total_data_modules: int = 0
    total_packages: int = 0
    total_data_sources: int = 0


class DashboardsBreakdown(BaseModel):
    total_dashboards: int
    dashboards: list[DashboardBreakdownItem]


class ReportBreakdownItem(BaseModel):
    report_id: str
    report_name: str
    report_type: str = "report"  # report, interactiveReport, reportView, dataSet2, reportVersion
    total_visualizations: int = 0
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
    reports: list[ReportBreakdownItem]


class PackageBreakdownItem(BaseModel):
    package_id: str
    package_name: str
    total_data_modules: int = 0
    main_data_modules: int = 0  # module, dataModule, model only (excludes smartsModule, modelView, dataSet2)
    data_modules_by_type: Dict[str, int] = Field(default_factory=dict)  # smartsModule, dataModule, module, etc.
    total_tables: int = 0
    total_columns: int = 0


class PackagesBreakdown(BaseModel):
    total_packages: int
    packages: list[PackageBreakdownItem]


class DataSourceConnectionBreakdownItem(BaseModel):
    connection_id: str
    connection_name: str
    object_type: str  # data_source | data_source_connection
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
    connections: list[DataSourceConnectionBreakdownItem]


class CalculatedFieldBreakdownItem(BaseModel):
    calculated_field_id: str
    name: str
    expression: Optional[str] = None
    calculation_type: Optional[str] = None
    cognos_class: Optional[str] = None
    class Config:
        extra = "allow"


class CalculatedFieldsBreakdown(BaseModel):
    total_calculated_fields: int
    calculated_fields: list[CalculatedFieldBreakdownItem]


class FilterBreakdownItem(BaseModel):
    filter_id: str
    name: str
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
    referenced_columns: Optional[List[str]] = None
    parameter_references: Optional[List[str]] = None
    cognos_class: Optional[str] = None
    class Config:
        extra = "allow"


class FiltersBreakdown(BaseModel):
    total_filters: int
    filters: list[FilterBreakdownItem]


class ParameterBreakdownItem(BaseModel):
    parameter_id: str
    name: str
    parameter_type: Optional[str] = None
    variable_type: Optional[str] = None
    cognos_class: Optional[str] = None
    class Config:
        extra = "allow"


class ParametersBreakdown(BaseModel):
    total_parameters: int
    parameters: list[ParameterBreakdownItem]


class SortBreakdownItem(BaseModel):
    sort_id: str
    name: str
    direction: Optional[str] = None
    sorted_column: Optional[str] = None
    sort_items: Optional[List[Any]] = None
    cognos_class: Optional[str] = None
    class Config:
        extra = "allow"


class SortsBreakdown(BaseModel):
    total_sorts: int
    sorts: list[SortBreakdownItem]


class PromptBreakdownItem(BaseModel):
    prompt_id: str
    name: str
    prompt_type: Optional[str] = None
    value: Optional[str] = None
    cognos_class: Optional[str] = None
    class Config:
        extra = "allow"


class PromptsBreakdown(BaseModel):
    total_prompts: int
    prompts: list[PromptBreakdownItem]


class QueryBreakdownItem(BaseModel):
    query_id: str
    name: str
    source_type: Optional[str] = None  # model, query_ref, sql
    is_simple: bool = False
    is_complex: bool = False
    report_id: Optional[str] = None
    report_name: Optional[str] = None
    cognos_class: Optional[str] = None
    sql_content: Optional[str] = None
    class Config:
        extra = "allow"


class QueriesBreakdown(BaseModel):
    total_queries: int
    queries: list[QueryBreakdownItem]


class MeasureBreakdownItem(BaseModel):
    measure_id: str
    name: str
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
    measures: list[MeasureBreakdownItem]


class DimensionBreakdownItem(BaseModel):
    dimension_id: str
    name: str
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
    dimensions: list[DimensionBreakdownItem]


class DataModuleBreakdownItem(BaseModel):
    data_module_id: str
    name: str
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


class AssessmentReportResponse(BaseModel):
    assessment_id: str
    sections: ReportSections
