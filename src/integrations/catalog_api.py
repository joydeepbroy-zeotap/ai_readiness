"""Zeotap Catalog API client."""

from typing import Dict, Any, List, Optional
import structlog

from ..core.api_client import APIClient
from ..core.exceptions import APIError, ValidationError
from ..config import settings

logger = structlog.get_logger()


class CatalogAPI(APIClient):
    """Client for Zeotap Catalog API."""
    
    def __init__(self, use_mock: bool = False):
        super().__init__(settings.catalog_api_url, use_mock)
        
    async def health_check(self) -> bool:
        """Check if Catalog API is reachable."""
        try:
            # Use a lightweight endpoint for health check
            # Since we need org_id, we'll just check if we can connect
            if not self.session:
                await self.connect()
            return True
        except Exception as e:
            logger.error(f"Catalog API health check failed: {e}")
            return False
    
    async def get_catalog_schema(
        self, 
        org_id: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Get catalog schema for organization.
        
        Args:
            org_id: Organization ID
            filters: Optional filters for search
            
        Returns:
            Catalog schema with attributes
        """
        if not org_id:
            raise ValidationError("Organization ID is required", field="org_id")
            
        # Prepare search payload
        payload = {
            "filters": filters or {},
            "from": 0,
            "size": 10000  # Get all attributes
        }
        json_body = {
            "fetchGroup": "COMPLETE_CATALOG_ATTR",
            "sortField": "ATTRIBUTE_NAME",
            "sortOrder": "ASC",
            "filters": {
                "excludeCategory": ["CALCULATED_ATTRIBUTE", "INTERNAL"]
            }
        }
        
        # Format URL with org_id
        url = self.base_url.format(org_id=org_id)
        
        try:
            response = await self._make_request(
                method="POST",
                url=url,
                json_data=json_body
            )
            
            # Extract relevant data
            result = {
                "orgId": org_id,
                "attributes": response.get("attributes", []),
                "totalCount": response.get("count", 0)
            }
            
            logger.info(
                f"Retrieved {result['totalCount']} attributes for org {org_id}"
            )
            
            return result
            
        except APIError:
            raise
        except Exception as e:
            raise APIError(
                f"Failed to get catalog schema: {str(e)}",
                endpoint=url
            )
    
    async def search_attributes(
        self,
        org_id: str,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        size: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Search for specific attributes.
        
        Args:
            org_id: Organization ID
            query: Search query
            filters: Additional filters
            size: Number of results to return
            
        Returns:
            List of matching attributes
        """
        if not org_id:
            raise ValidationError("Organization ID is required", field="org_id")
            
        # Build search filters
        search_filters = filters or {}
        
        # Add query to filters if provided
        if query:
            search_filters["query"] = {
                "multi_match": {
                    "query": query,
                    "fields": ["name", "displayName", "description"]
                }
            }
        
        payload = {
            "filters": search_filters,
            "from": 0,
            "size": size
        }
        
        url = self.base_url.format(org_id=org_id)
        
        try:
            response = await self._make_request(
                method="POST",
                url=url,
                json_data=payload
            )
            
            attributes = response.get("documents", [])
            logger.info(
                f"Found {len(attributes)} attributes matching query '{query}'"
            )
            
            return attributes
            
        except APIError:
            raise
        except Exception as e:
            raise APIError(
                f"Failed to search attributes: {str(e)}",
                endpoint=url
            )
    
    async def get_attributes_by_type(
        self,
        org_id: str,
        attribute_type: str
    ) -> List[Dict[str, Any]]:
        """
        Get all attributes of a specific type.
        
        Args:
            org_id: Organization ID
            attribute_type: Attribute type (IDENTITY, DEMOGRAPHIC, etc.)
            
        Returns:
            List of attributes of the specified type
        """
        filters = {
            "attributeType": attribute_type.upper()
        }
        
        schema = await self.get_catalog_schema(org_id, filters)
        return schema.get("attributes", [])
    
    async def get_pii_attributes(self, org_id: str) -> List[Dict[str, Any]]:
        """
        Get all PII-marked attributes.
        
        Args:
            org_id: Organization ID
            
        Returns:
            List of PII attributes
        """
        filters = {
            "isRawPII": True
        }
        
        schema = await self.get_catalog_schema(org_id, filters)
        return schema.get("attributes", [])
