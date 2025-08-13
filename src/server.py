"""Main MCP server implementation."""

import asyncio
from typing import Any, Optional, List, Dict
from fastmcp import FastMCP
import structlog

from .config import settings
# from .core.cache_manager import cache_manager
from .tools import (
    SchemaDiscoveryTool,
    FeatureAnalysisTool,
    QueryBuilderTool,
    ComplianceCheckerTool
)

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.dev.ConsoleRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Initialize FastMCP server
mcp = FastMCP(
    name="zeotap-feature-analysis",
    version="1.0.0"
    # description="MCP server for Zeotap data feature analysis and AI/ML readiness assessment"
)


# Server lifecycle hooks
# @mcp.on_startup()
# async def startup():
#     """Initialize server resources."""
#     logger.info("Starting Zeotap Feature Analysis MCP Server")
#
#     try:
#         # Connect to cache
#         await cache_manager.connect()
#         logger.info("Cache manager connected")
#
#         # Log configuration
#         logger.info(
#             "Server configuration",
#             use_mock_api=settings.use_mock_api,
#             cache_enabled=cache_manager._connected,
#             bigquery_project=settings.bigquery_project
#         )
#
#     except Exception as e:
#         logger.error(f"Startup failed: {e}")
#         raise


# @mcp.on_shutdown()
# async def shutdown():
#     """Cleanup server resources."""
#     logger.info("Shutting down MCP server")
#
#     try:
#         # Disconnect cache
#         await cache_manager.disconnect()
#         logger.info("Cache manager disconnected")
#
#     except Exception as e:
#         logger.error(f"Shutdown error: {e}")


# Register tools
@mcp.tool()
async def schema_discovery(
    org_id: str,
    operation: str = "overview",
    store_type: Optional[str] = None,
    columns: Optional[List[str]] = None,
    search_query: Optional[str] = None,
    force_refresh: bool = False
) -> Any:
    """
    Discover and explore data schema.
    
    Operations:
    - overview: Get schema overview with store distribution
    - store: Get schema for specific store (requires store_type)
    - columns: Get details for specific columns (requires columns list)
    - search: Search for columns (requires search_query)
    - pii: Get PII columns analysis
    
    Args:
        org_id: Organization ID (required)
        operation: Operation type
        store_type: Store type (profile_store, event_store, etc.)
        columns: List of column names
        search_query: Search query
        force_refresh: Force refresh cached data
        
    Returns:
        Schema discovery results
    """
    # Validate org_id
    if not org_id:
        return {
            "error": "org_id is required",
            "message": "Please provide an organization ID to discover schema"
        }
    
    tool = SchemaDiscoveryTool()
    return await tool.run(
        org_id=org_id,
        operation=operation,
        store_type=store_type,
        columns=columns,
        search_query=search_query,
        force_refresh=force_refresh
    )


@mcp.tool()
async def feature_analysis(
    org_id: str,
    use_case: str = "collaborative_filtering",
    columns: Optional[List[str]] = None,
    dataset_id: Optional[str] = None,
    table_id: Optional[str] = None,
    include_statistics: bool = True,
    include_quality: bool = True,
    include_correlations: bool = False
) -> Any:
    """
    Analyze features for AI/ML readiness.
    
    Use cases:
    - collaborative_filtering: Product recommendations
    - churn_prediction: Customer churn analysis
    - segmentation: Customer segmentation
    
    Args:
        org_id: Organization ID
        use_case: ML use case
        columns: Specific columns to analyze
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        include_statistics: Include statistical analysis
        include_quality: Include data quality metrics
        include_correlations: Include correlation analysis
        
    Returns:
        Feature analysis and readiness assessment
    """
    tool = FeatureAnalysisTool()
    return await tool.run(
        org_id=org_id,
        use_case=use_case,
        columns=columns,
        dataset_id=dataset_id,
        table_id=table_id,
        include_statistics=include_statistics,
        include_quality=include_quality,
        include_correlations=include_correlations
    )


@mcp.tool()
async def query_builder(
    org_id: str,
    query_type: str = "feature_extraction",
    use_case: str = "collaborative_filtering",
    dataset_id: Optional[str] = None,
    table_id: Optional[str] = None,
    features: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    execute: bool = False
) -> Any:
    """
    Build queries for ML dataset preparation.
    
    Query types:
    - feature_extraction: Extract features for training
    - aggregation: Create aggregated features
    - sampling: Generate data samples
    
    Args:
        org_id: Organization ID
        query_type: Type of query to build
        use_case: ML use case
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        features: List of features to include
        filters: Query filters
        limit: Result limit
        execute: Execute the query
        
    Returns:
        Generated query and optionally results
    """
    tool = QueryBuilderTool()
    return await tool.run(
        org_id=org_id,
        query_type=query_type,
        use_case=use_case,
        dataset_id=dataset_id,
        table_id=table_id,
        features=features,
        filters=filters,
        limit=limit,
        execute=execute
    )


@mcp.tool()
async def compliance_checker(
    org_id: str,
    check_type: str = "full",
    regulations: Optional[List[str]] = None,
    columns: Optional[List[str]] = None,
    generate_report: bool = True
) -> Any:
    """
    Check data compliance for privacy regulations.
    
    Check types:
    - full: Complete compliance check
    - pii: PII compliance only
    - consent: Consent management only
    - retention: Data retention only
    - regulations: Specific regulation checks
    
    Args:
        org_id: Organization ID
        check_type: Type of compliance check
        regulations: List of regulations (GDPR, CCPA, HIPAA)
        columns: Specific columns to check
        generate_report: Generate detailed report
        
    Returns:
        Compliance check results and report
    """
    tool = ComplianceCheckerTool()
    return await tool.run(
        org_id=org_id,
        check_type=check_type,
        regulations=regulations,
        columns=columns,
        generate_report=generate_report
    )


# Health check endpoint
@mcp.tool()
async def health_check() -> dict:
    """
    Check server health and connectivity.
    
    Returns:
        Health status of server components
    """
    health = {
        "status": "healthy",
        "version": "1.0.0",
        "components": {}
    }
    
    # Check cache
    # cache_stats = cache_manager.get_cache_stats()
    # health["components"]["cache"] = {
    #     "connected": cache_stats["redis_connected"],
    #     "memory_items": cache_stats["memory_cache_size"]
    # }
    
    # Check mock mode
    health["components"]["api_mode"] = "mock" if settings.use_mock_api else "live"
    
    return health


def create_app():
    """Create and configure the MCP server application."""
    return mcp


if __name__ == "__main__":
    # Run the server
    app = create_app()
    asyncio.run(app.run(transport='stdio'))
