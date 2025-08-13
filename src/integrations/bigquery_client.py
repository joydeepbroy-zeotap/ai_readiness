"""BigQuery client for statistical analysis."""

from typing import Dict, Any, List, Optional, Union
import structlog
from google.cloud import bigquery
from google.oauth2 import service_account

from ..config import settings
from ..core.exceptions import APIError, ValidationError

logger = structlog.get_logger()


class BigQueryClient:
    """Client for BigQuery operations."""
    
    def __init__(self, use_mock: bool = False):
        self.use_mock = use_mock or settings.use_mock_api
        self.client: Optional[bigquery.Client] = None
        
    async def connect(self):
        """Initialize BigQuery client."""
        if self.use_mock:
            return
            
        try:
            # If credentials path is provided, use service account
            if settings.bigquery_credentials_path:
                credentials = service_account.Credentials.from_service_account_file(
                    settings.bigquery_credentials_path
                )
                self.client = bigquery.Client(
                    credentials=credentials,
                    project=settings.bigquery_project,
                    location=settings.bigquery_location
                )
            else:
                # Use default credentials (ADC)
                self.client = bigquery.Client(
                    project=settings.bigquery_project,
                    location=settings.bigquery_location
                )
                
            logger.info(f"Connected to BigQuery project: {settings.bigquery_project}")
            
        except Exception as e:
            raise APIError(f"Failed to connect to BigQuery: {str(e)}")
    
    async def disconnect(self):
        """Close BigQuery client."""
        if self.client:
            self.client.close()
            self.client = None
    
    async def health_check(self) -> bool:
        """Check if BigQuery is accessible."""
        try:
            if not self.client:
                await self.connect()
                
            # Run a simple query to test connection
            query = "SELECT 1"
            self.client.query(query).result()
            return True
            
        except Exception as e:
            logger.error(f"BigQuery health check failed: {e}")
            return False
    
    async def analyze_table_statistics(
        self,
        dataset_id: str,
        table_id: str
    ) -> Dict[str, Any]:
        """
        Get comprehensive statistics for a table.
        
        Args:
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            
        Returns:
            Dictionary with table statistics
        """
        if not self.client:
            await self.connect()
            
        try:
            # Get table reference
            table_ref = f"{settings.bigquery_project}.{dataset_id}.{table_id}"
            table = self.client.get_table(table_ref)
            
            # Get basic table info
            stats = {
                "table_id": table_id,
                "dataset_id": dataset_id,
                "row_count": table.num_rows,
                "size_bytes": table.num_bytes,
                "created": table.created.isoformat() if table.created else None,
                "modified": table.modified.isoformat() if table.modified else None,
                "schema": [
                    {
                        "name": field.name,
                        "type": field.field_type,
                        "mode": field.mode,
                        "description": field.description
                    }
                    for field in table.schema
                ]
            }
            
            return stats
            
        except Exception as e:
            raise APIError(
                f"Failed to analyze table {table_ref}: {str(e)}",
                endpoint=f"bigquery://{table_ref}"
            )
    
    async def get_column_distribution(
        self,
        dataset_id: str,
        table_id: str,
        column_name: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Get value distribution for a column.
        
        Args:
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            column_name: Column name to analyze
            limit: Maximum number of distinct values
            
        Returns:
            Distribution statistics
        """
        if not self.client:
            await self.connect()
            
        table_ref = f"{settings.bigquery_project}.{dataset_id}.{table_id}"
        
        # Query for value distribution
        query = f"""
        SELECT 
            {column_name} as value,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
        FROM `{table_ref}`
        GROUP BY {column_name}
        ORDER BY count DESC
        LIMIT {limit}
        """
        
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            
            distribution = []
            for row in results:
                distribution.append({
                    "value": row.value,
                    "count": row.count,
                    "percentage": float(row.percentage)
                })
                
            return {
                "column": column_name,
                "distribution": distribution,
                "unique_values": len(distribution)
            }
            
        except Exception as e:
            raise APIError(
                f"Failed to get distribution for {column_name}: {str(e)}",
                endpoint=f"bigquery://{table_ref}"
            )
    
    async def calculate_correlation_matrix(
        self,
        dataset_id: str,
        table_id: str,
        numeric_columns: List[str]
    ) -> Dict[str, Dict[str, float]]:
        """
        Calculate correlation matrix for numeric columns.
        
        Args:
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            numeric_columns: List of numeric column names
            
        Returns:
            Correlation matrix
        """
        if not numeric_columns or len(numeric_columns) < 2:
            raise ValidationError(
                "At least 2 numeric columns required for correlation",
                field="numeric_columns"
            )
            
        if not self.client:
            await self.connect()
            
        table_ref = f"{settings.bigquery_project}.{dataset_id}.{table_id}"
        
        # Build correlation query
        correlations = {}
        
        for i, col1 in enumerate(numeric_columns):
            correlations[col1] = {}
            for col2 in numeric_columns:
                if col1 == col2:
                    correlations[col1][col2] = 1.0
                else:
                    query = f"""
                    SELECT CORR({col1}, {col2}) as correlation
                    FROM `{table_ref}`
                    WHERE {col1} IS NOT NULL AND {col2} IS NOT NULL
                    """
                    
                    try:
                        query_job = self.client.query(query)
                        results = list(query_job.result())
                        
                        if results and results[0].correlation is not None:
                            correlations[col1][col2] = round(float(results[0].correlation), 4)
                        else:
                            correlations[col1][col2] = None
                            
                    except Exception as e:
                        logger.error(f"Failed to calculate correlation for {col1}, {col2}: {e}")
                        correlations[col1][col2] = None
                        
        return correlations
    
    async def get_data_quality_metrics(
        self,
        dataset_id: str,
        table_id: str,
        columns: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get data quality metrics for columns.
        
        Args:
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            columns: List of column names
            
        Returns:
            Data quality metrics per column
        """
        if not self.client:
            await self.connect()
            
        table_ref = f"{settings.bigquery_project}.{dataset_id}.{table_id}"
        metrics = {}
        
        for column in columns:
            query = f"""
            SELECT
                COUNT(*) as total_rows,
                COUNT({column}) as non_null_count,
                COUNT(DISTINCT {column}) as unique_count,
                MIN(LENGTH(CAST({column} AS STRING))) as min_length,
                MAX(LENGTH(CAST({column} AS STRING))) as max_length,
                AVG(LENGTH(CAST({column} AS STRING))) as avg_length
            FROM `{table_ref}`
            """
            
            try:
                query_job = self.client.query(query)
                results = list(query_job.result())
                
                if results:
                    row = results[0]
                    total_rows = row.total_rows or 1
                    
                    metrics[column] = {
                        "completeness": round((row.non_null_count / total_rows) * 100, 2),
                        "uniqueness": round((row.unique_count / row.non_null_count) * 100, 2) if row.non_null_count else 0,
                        "null_count": total_rows - row.non_null_count,
                        "null_percentage": round(((total_rows - row.non_null_count) / total_rows) * 100, 2),
                        "unique_count": row.unique_count,
                        "length_stats": {
                            "min": row.min_length,
                            "max": row.max_length,
                            "avg": round(float(row.avg_length), 2) if row.avg_length else None
                        }
                    }
                    
            except Exception as e:
                logger.error(f"Failed to get quality metrics for {column}: {e}")
                metrics[column] = {"error": str(e)}
                
        return metrics
    
    async def run_custom_query(
        self,
        query: str,
        parameters: Optional[List[bigquery.ScalarQueryParameter]] = None
    ) -> List[Dict[str, Any]]:
        """
        Run a custom BigQuery query.
        
        Args:
            query: SQL query to execute
            parameters: Optional query parameters
            
        Returns:
            Query results as list of dictionaries
        """
        if not self.client:
            await self.connect()
            
        try:
            # Configure query
            job_config = bigquery.QueryJobConfig()
            if parameters:
                job_config.query_parameters = parameters
                
            # Run query
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            
            # Convert to list of dicts
            rows = []
            for row in results:
                rows.append(dict(row))
                
            logger.info(f"Query returned {len(rows)} rows")
            return rows
            
        except Exception as e:
            raise APIError(
                f"Failed to execute query: {str(e)}",
                endpoint="bigquery://custom_query"
            )