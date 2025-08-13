"""Zeotap Metadata API client."""

from typing import Dict, Any, List, Optional
import structlog

from ..core.api_client import APIClient
from ..core.exceptions import APIError, ValidationError
from ..config import settings

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
        payload = {
            "orgId": org_id,
            "columns": columns,
            "sampleSize": sample_size
        }
        
        try:
            response = await self._make_request(
                method="POST",
                url=self.base_url,
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
    
    async def analyze_cardinality(
        self,
        org_id: str,
        columns: List[str]
    ) -> Dict[str, str]:
        """
        Analyze cardinality for columns.
        
        Args:
            org_id: Organization ID
            columns: List of column names
            
        Returns:
            Dictionary mapping column names to cardinality levels
        """
        metadata = await self.get_column_metadata(org_id, columns)
        
        cardinality = {}
        for item in metadata:
            column_name = item.get("column")
            if column_name:
                unique_count = len(item.get("values", []))
                total_count = item.get("count", 0)
                
                # Determine cardinality level
                if unique_count <= settings.LOW_CARDINALITY_THRESHOLD:
                    level = "LOW"
                elif unique_count / max(total_count, 1) > 0.95:
                    level = "VERY_HIGH"  # Almost unique
                else:
                    level = "HIGH"
                    
                cardinality[column_name] = level
                
        return cardinality
    
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