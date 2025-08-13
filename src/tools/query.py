"""Query builder tool for generating ML-ready queries."""

from typing import Dict, Any, Optional, List
import structlog

from ..core.schema_manager import SchemaManager
from ..core.exceptions import ValidationError
from ..integrations import (
    CatalogAPI, MetadataAPI, BigQueryClient,
    MockCatalogAPI, MockMetadataAPI, MockBigQueryClient
)
from ..config import settings

logger = structlog.get_logger()


class QueryBuilderTool:
    """Tool for building queries to extract ML-ready datasets."""
    
    def __init__(self):
        # Initialize APIs based on mock setting
        if settings.use_mock_api:
            self.catalog_api = MockCatalogAPI()
            self.metadata_api = MockMetadataAPI()
            self.bigquery_client = MockBigQueryClient()
        else:
            self.catalog_api = CatalogAPI()
            self.metadata_api = MetadataAPI()
            self.bigquery_client = BigQueryClient()
            
        self.schema_manager = SchemaManager(self.catalog_api, self.metadata_api)
    
    async def run(
        self,
        org_id: str,
        query_type: str = "feature_extraction",
        use_case: str = "collaborative_filtering",
        dataset_id: Optional[str] = None,
        table_id: Optional[str] = None,
        features: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        execute: bool = False
    ) -> Dict[str, Any]:
        """
        Build and optionally execute ML queries.
        
        Args:
            org_id: Organization ID
            query_type: Type of query (feature_extraction, aggregation, sampling)
            use_case: ML use case
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            features: List of features to include
            filters: Query filters
            limit: Result limit
            execute: Whether to execute the query
            
        Returns:
            Query and optionally results
        """
        try:
            # Connect to services
            await self.catalog_api.connect()
            await self.metadata_api.connect()
            await self.bigquery_client.connect()
            
            # Get schema for validation
            schema = await self.schema_manager.get_schema(org_id)
            
            # Build query based on type
            if query_type == "feature_extraction":
                query_result = await self._build_feature_extraction_query(
                    schema, use_case, dataset_id, table_id, features, filters, limit
                )
            elif query_type == "aggregation":
                query_result = await self._build_aggregation_query(
                    schema, use_case, dataset_id, table_id, features, filters
                )
            elif query_type == "sampling":
                query_result = await self._build_sampling_query(
                    schema, dataset_id, table_id, features, limit or 10000
                )
            else:
                raise ValidationError(f"Unknown query_type: {query_type}")
            
            # Execute if requested
            if execute and dataset_id and table_id:
                try:
                    results = await self.bigquery_client.run_custom_query(
                        query_result["query"]
                    )
                    query_result["execution_results"] = {
                        "rows_returned": len(results),
                        "sample_data": results[:5] if results else []
                    }
                except Exception as e:
                    query_result["execution_error"] = str(e)
            
            return query_result
            
        except Exception as e:
            logger.error(f"Query builder failed: {e}")
            return {
                "error": str(e),
                "org_id": org_id,
                "query_type": query_type
            }
        finally:
            # Cleanup
            await self.catalog_api.disconnect()
            await self.metadata_api.disconnect()
            await self.bigquery_client.disconnect()
    
    async def _build_feature_extraction_query(
        self,
        schema: Dict[str, Any],
        use_case: str,
        dataset_id: Optional[str],
        table_id: Optional[str],
        features: Optional[List[str]],
        filters: Optional[Dict[str, Any]],
        limit: Optional[int]
    ) -> Dict[str, Any]:
        """Build feature extraction query."""
        if not dataset_id or not table_id:
            return {
                "error": "dataset_id and table_id required for query generation",
                "query_type": "feature_extraction"
            }
        
        # Determine features if not provided
        if not features:
            features = await self._get_default_features(schema, use_case)
        
        # Validate features exist
        all_columns = {col["name"] for col in schema["raw_attributes"]}
        valid_features = [f for f in features if f in all_columns]
        invalid_features = [f for f in features if f not in all_columns]
        
        # Build SELECT clause
        select_clause = ",\n    ".join(valid_features)
        
        # Build WHERE clause
        where_conditions = []
        if filters:
            for field, value in filters.items():
                if isinstance(value, str):
                    where_conditions.append(f"{field} = '{value}'")
                elif isinstance(value, (int, float)):
                    where_conditions.append(f"{field} = {value}")
                elif isinstance(value, dict):
                    # Handle range filters
                    if "min" in value:
                        where_conditions.append(f"{field} >= {value['min']}")
                    if "max" in value:
                        where_conditions.append(f"{field} <= {value['max']}")
        
        where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
        
        # Build LIMIT clause
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        # Construct query
        query = f"""
SELECT 
    {select_clause}
FROM `{settings.bigquery_project}.{dataset_id}.{table_id}`
{where_clause}
{limit_clause}
        """.strip()
        
        return {
            "query_type": "feature_extraction",
            "use_case": use_case,
            "query": query,
            "features_requested": features,
            "features_included": valid_features,
            "features_not_found": invalid_features,
            "filters_applied": filters or {},
            "recommendations": self._get_query_recommendations(use_case, valid_features)
        }
    
    async def _build_aggregation_query(
        self,
        schema: Dict[str, Any],
        use_case: str,
        dataset_id: Optional[str],
        table_id: Optional[str],
        features: Optional[List[str]],
        filters: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Build aggregation query for feature engineering."""
        if not dataset_id or not table_id:
            return {
                "error": "dataset_id and table_id required for query generation",
                "query_type": "aggregation"
            }
        
        # Determine aggregation based on use case
        if use_case == "collaborative_filtering":
            # User-item interaction aggregations
            query = f"""
WITH user_item_stats AS (
    SELECT 
        user_id,
        COUNT(DISTINCT product_id) as products_interacted,
        COUNT(*) as total_interactions,
        AVG(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_rate,
        MAX(event_timestamp) as last_interaction
    FROM `{settings.bigquery_project}.{dataset_id}.{table_id}`
    GROUP BY user_id
),
item_stats AS (
    SELECT 
        product_id,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(*) as total_interactions,
        AVG(CASE WHEN event_type = 'purchase' THEN purchase_amount ELSE 0 END) as avg_purchase_amount
    FROM `{settings.bigquery_project}.{dataset_id}.{table_id}`
    GROUP BY product_id
)
SELECT * FROM user_item_stats
UNION ALL
SELECT * FROM item_stats
            """
            
        elif use_case == "churn_prediction":
            # User behavior aggregations
            query = f"""
SELECT 
    user_id,
    COUNT(*) as total_events,
    COUNT(DISTINCT DATE(event_timestamp)) as active_days,
    DATE_DIFF(CURRENT_DATE(), MAX(DATE(event_timestamp)), DAY) as days_since_last_activity,
    AVG(session_duration) as avg_session_duration,
    SUM(purchase_amount) as total_purchase_amount,
    COUNT(DISTINCT product_id) as unique_products
FROM `{settings.bigquery_project}.{dataset_id}.{table_id}`
GROUP BY user_id
            """
        else:
            # Generic aggregation
            group_by_col = features[0] if features else "user_id"
            query = f"""
SELECT 
    {group_by_col},
    COUNT(*) as count,
    MIN(event_timestamp) as first_seen,
    MAX(event_timestamp) as last_seen
FROM `{settings.bigquery_project}.{dataset_id}.{table_id}`
GROUP BY {group_by_col}
            """
        
        return {
            "query_type": "aggregation",
            "use_case": use_case,
            "query": query.strip(),
            "aggregation_level": "user" if use_case in ["collaborative_filtering", "churn_prediction"] else "custom",
            "features_created": self._get_aggregated_features(use_case)
        }
    
    async def _build_sampling_query(
        self,
        schema: Dict[str, Any],
        dataset_id: Optional[str],
        table_id: Optional[str],
        features: Optional[List[str]],
        sample_size: int
    ) -> Dict[str, Any]:
        """Build sampling query for model training."""
        if not dataset_id or not table_id:
            return {
                "error": "dataset_id and table_id required for query generation",
                "query_type": "sampling"
            }
        
        # Use all features if not specified
        if not features:
            features = [col["name"] for col in schema["raw_attributes"][:20]]
        
        select_clause = ",\n    ".join(features)
        
        # Multiple sampling strategies
        queries = {
            "random_sample": f"""
SELECT 
    {select_clause}
FROM `{settings.bigquery_project}.{dataset_id}.{table_id}`
WHERE RAND() < {sample_size / 1000000}  -- Assuming ~1M rows
LIMIT {sample_size}
            """,
            
            "stratified_sample": f"""
WITH stratified AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY user_segment ORDER BY RAND()) as rn
    FROM `{settings.bigquery_project}.{dataset_id}.{table_id}`
)
SELECT 
    {select_clause}
FROM stratified
WHERE rn <= {sample_size / 10}  -- Assuming ~10 segments
            """,
            
            "time_based_sample": f"""
SELECT 
    {select_clause}
FROM `{settings.bigquery_project}.{dataset_id}.{table_id}`
WHERE DATE(event_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY RAND()
LIMIT {sample_size}
            """
        }
        
        return {
            "query_type": "sampling",
            "sample_size": sample_size,
            "sampling_strategies": queries,
            "recommended_strategy": "random_sample",
            "features_included": features,
            "notes": [
                "Random sampling is fastest but may miss rare classes",
                "Stratified sampling ensures balanced representation",
                "Time-based sampling focuses on recent data"
            ]
        }
    
    async def _get_default_features(
        self, 
        schema: Dict[str, Any], 
        use_case: str
    ) -> List[str]:
        """Get default features for use case."""
        all_columns = schema["raw_attributes"]
        
        if use_case == "collaborative_filtering":
            # Look for user, item, and interaction columns
            features = []
            for col in all_columns:
                col_name = col["name"].lower()
                if any(term in col_name for term in ["user", "product", "item", "purchase", "rating", "view"]):
                    features.append(col["name"])
            return features[:20]
            
        elif use_case == "churn_prediction":
            # Look for user behavior columns
            features = []
            for col in all_columns:
                col_name = col["name"].lower()
                if any(term in col_name for term in ["user", "event", "activity", "engagement", "last", "count"]):
                    features.append(col["name"])
            return features[:20]
            
        else:
            # Return mix of columns
            return [col["name"] for col in all_columns[:15]]
    
    def _get_query_recommendations(
        self, 
        use_case: str, 
        features: List[str]
    ) -> List[str]:
        """Get query optimization recommendations."""
        recommendations = []
        
        # General recommendations
        recommendations.append("Consider adding appropriate indexes for better query performance")
        recommendations.append("Use partitioning on timestamp columns if available")
        
        # Use case specific
        if use_case == "collaborative_filtering":
            if not any("user" in f.lower() for f in features):
                recommendations.append("Add user identifier column for collaborative filtering")
            if not any("product" in f.lower() or "item" in f.lower() for f in features):
                recommendations.append("Add item/product identifier column")
                
        elif use_case == "churn_prediction":
            if not any("timestamp" in f.lower() or "date" in f.lower() for f in features):
                recommendations.append("Add timestamp columns to calculate recency features")
                
        # Data quality
        recommendations.append("Filter out records with null values in key columns")
        recommendations.append("Consider data validation before model training")
        
        return recommendations
    
    def _get_aggregated_features(self, use_case: str) -> List[str]:
        """Get list of aggregated features created."""
        if use_case == "collaborative_filtering":
            return [
                "products_interacted",
                "total_interactions", 
                "purchase_rate",
                "last_interaction",
                "unique_users",
                "avg_purchase_amount"
            ]
        elif use_case == "churn_prediction":
            return [
                "total_events",
                "active_days",
                "days_since_last_activity",
                "avg_session_duration",
                "total_purchase_amount",
                "unique_products"
            ]
        else:
            return ["count", "first_seen", "last_seen"]