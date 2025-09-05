"""Zeotap Metadata API client."""

from typing import Dict, Any, List, Optional
import structlog

from ..core.api_client import APIClient
from ..core.exceptions import APIError, ValidationError
from ..config import settings
from .bigquery_client import BigQueryClient

logger = structlog.get_logger()


class MetadataAPI(APIClient):
    """Client for Zeotap Metadata API."""
    
    def __init__(self, use_mock: bool = False):
        super().__init__(settings.metadata_api_url, use_mock)
        
    async def health_check(self) -> bool:
        """Check if Metadata API is reachable."""
        try:
            if not self.session:
                await self.connect()
            return True
        except Exception as e:
            logger.error(f"Metadata API health check failed: {e}")
            return False
    
    async def get_column_metadata(
        self,
        org_id: str,
        columns: List[str],
        sample_size: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get metadata for specific columns.
        
        Args:
            org_id: Organization ID
            columns: List of column names
            sample_size: Number of sample values to return
            
        Returns:
            List of metadata for each column
        """
        if not org_id:
            raise ValidationError("Organization ID is required", field="org_id")
        
        if not columns:
            raise ValidationError("At least one column is required", field="columns")
            
        # Prepare request payload
        payload = {"columns": columns}
        
        try:
            response = await self._make_request(
                method="POST",
                url=self.base_url+f"?org={org_id}",
                json_data=payload
            )
            
            # Response should be a list of column metadata
            metadata = response if isinstance(response, list) else []
            
            logger.info(
                f"Retrieved metadata for {len(metadata)} columns"
            )
            
            return metadata
            
        except APIError:
            raise
        except Exception as e:
            raise APIError(
                f"Failed to get column metadata: {str(e)}",
                endpoint=self.base_url
            )
    
    async def get_column_statistics(
        self,
        org_id: str,
        columns: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get statistical information for columns.
        
        Args:
            org_id: Organization ID
            columns: List of column names
            
        Returns:
            Dictionary mapping column names to their statistics
        """
        metadata = await self.get_column_metadata(org_id, columns)
        
        stats = {}
        for item in metadata:
            column_name = item.get("column")
            if column_name:
                stats[column_name] = {
                    "count": item.get("count", 0),
                    "unique_values": len(item.get("values", [])),
                    "sample_values": item.get("values", [])[:10],  # First 10 values
                    "null_count": item.get("nullCount", 0),
                    "null_percentage": self._calculate_null_percentage(
                        item.get("nullCount", 0),
                        item.get("totalCount", 1)
                    )
                }
                
        return stats
    
    def _calculate_null_percentage(self, null_count: int, total_count: int) -> float:
        """Calculate null percentage."""
        if total_count == 0:
            return 0.0
        return round((null_count / total_count) * 100, 2)
    
    async def get_sample_data(
        self,
        org_id: str,
        columns: List[str],
        limit: int = 10
    ) -> Dict[str, List[Any]]:
        """
        Get sample data for columns.
        
        Args:
            org_id: Organization ID
            columns: List of column names
            limit: Number of samples per column
            
        Returns:
            Dictionary mapping column names to sample values
        """
        metadata = await self.get_column_metadata(org_id, columns, sample_size=limit)
        
        samples = {}
        for item in metadata:
            column_name = item.get("column")
            if column_name:
                values = item.get("values", [])
                samples[column_name] = values[:limit]
                
        return samples
    
    async def get_column_statistics_from_bigquery(
        self,
        org_id: str,
        store_type: str,
        columns: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get comprehensive column statistics from BigQuery unified_feature_analysis table.
        
        Args:
            org_id: Organization ID
            store_type: Store type (e.g., 'profile_store')
            columns: List of column names
            
        Returns:
            Dictionary mapping column names to their statistics from BigQuery
        """
        if not org_id:
            raise ValidationError("Organization ID is required", field="org_id")
        
        if not columns:
            raise ValidationError("At least one column is required", field="columns")
        
        if not store_type:
            raise ValidationError("Store type is required", field="store_type")
            
        try:
            # Initialize BigQuery client
            bq_client = BigQueryClient(use_mock=self.use_mock)
            await bq_client.connect()
            
            # Build the column names condition for the query
            columns_condition = ", ".join([f"'{col}'" for col in columns])
            
            # Query to fetch column statistics from BigQuery
            query = f"""
            SELECT 
                column_name,
                fill_rate,
                fill_rate_total_rows,
                fill_rate_non_null_rows,
                data_type,
                data_mode,
                percentiles,
                percentiles_sample_size,
                distinct_count,
                uniqueness_ratio,
                low_cardinality,
                cardinality_category,
                sample_values,
                distinct_value_counts_unique_users_by_value,
                unique_users_by_string_column_data,
                event_frequency_stats,
                event_type_distribution,
                category_canon,
                group_canon,
                isRawPII_canon,
                isPivot_canon,
                cardinalityType_canon,
                displayName_canon,
                groupDisplayName_canon,
                last_updated,
                analysis_runtime_ms,
                row_count_analyzed
            FROM `zeotap-dev-datascience.audience_recommendation.unified_feature_analysis`
            WHERE org_id = {org_id} 
                AND store_type = '{store_type}'
                AND column_name IN ({columns_condition})
            """
            
            # Execute the query
            results = await bq_client.run_custom_query(query)
            
            # Transform results into a dictionary mapping column names to statistics
            statistics = {}
            for row in results:
                column_name = row.get('column_name')
                if column_name:
                    statistics[column_name] = {
                        'fill_rate': row.get('fill_rate'),
                        'fill_rate_total_rows': row.get('fill_rate_total_rows'),
                        'fill_rate_non_null_rows': row.get('fill_rate_non_null_rows'),
                        'data_type': row.get('data_type'),
                        'data_mode': row.get('data_mode'),
                        'percentiles': row.get('percentiles'),
                        'percentiles_sample_size': row.get('percentiles_sample_size'),
                        'distinct_count': row.get('distinct_count'),
                        'uniqueness_ratio': row.get('uniqueness_ratio'),
                        'low_cardinality': row.get('low_cardinality'),
                        'cardinality_category': row.get('cardinality_category'),
                        'sample_values': row.get('sample_values'),
                        'distinct_value_counts': row.get('distinct_value_counts_unique_users_by_value'),
                        'unique_users_by_column': row.get('unique_users_by_string_column_data'),
                        'event_frequency_stats': row.get('event_frequency_stats'),
                        'event_type_distribution': row.get('event_type_distribution'),
                        'category': row.get('category_canon'),
                        'group': row.get('group_canon'),
                        'is_pii': row.get('isRawPII_canon'),
                        'is_pivot': row.get('isPivot_canon'),
                        'cardinality_type': row.get('cardinalityType_canon'),
                        'display_name': row.get('displayName_canon'),
                        'group_display_name': row.get('groupDisplayName_canon'),
                        'last_updated': row.get('last_updated'),
                        'analysis_runtime_ms': row.get('analysis_runtime_ms'),
                        'row_count_analyzed': row.get('row_count_analyzed')
                    }
            
            logger.info(
                f"Retrieved BigQuery statistics for {len(statistics)} columns from {store_type}"
            )
            
            return statistics
            
        except Exception as e:
            logger.error(f"Failed to get column statistics from BigQuery: {e}")
            raise APIError(
                f"Failed to get column statistics from BigQuery: {str(e)}",
                endpoint="bigquery://unified_feature_analysis"
            )
        finally:
            # Cleanup
            if 'bq_client' in locals():
                await bq_client.disconnect()
