"""Schema management with caching and categorization."""

from typing import Dict, List, Any, Optional
import structlog

from ..config import settings, STORE_MAPPING, PII_PATTERNS
from .cache_manager import cache_manager
from .exceptions import ValidationError

logger = structlog.get_logger()


class SchemaManager:
    """Manages schema operations with caching."""
    
    def __init__(self, catalog_api, metadata_api):
        self.catalog_api = catalog_api
        self.metadata_api = metadata_api
        
    async def get_schema(
        self, 
        org_id: str, 
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """Get complete schema for organization."""
        if not org_id:
            raise ValidationError("Organization ID is required", field="org_id")
            
        cache_key = f"schema:{org_id}"
        
        # Check cache first
        if not force_refresh:
            cached = await cache_manager.get(cache_key)
            if cached:
                return cached
                
        # Fetch from API
        logger.info(f"Fetching schema for org {org_id}")
        raw_schema = await self.catalog_api.get_catalog_schema(org_id)
        
        # Categorize by store
        categorized = self._categorize_schema(raw_schema)
        
        # Cache result
        await cache_manager.set(cache_key, categorized, ttl=settings.cache_ttl_medium)
        
        return categorized
    
    def _categorize_schema(self, raw_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Categorize columns by store type."""
        stores = {
            "profile_store": [],
            "event_store": [],
            "calculated_attribute": [],
            "consent_store": []
        }
        
        attributes = raw_schema.get("attributes", [])
        
        for attr in attributes:
            store = self._determine_store(attr)
            stores[store].append(attr)
            
        return {
            "org_id": raw_schema.get("orgId"),
            "total_columns": len(attributes),
            "stores": stores,
            "raw_attributes": attributes
        }
    
    def _determine_store(self, attribute: Dict[str, Any]) -> str:
        """Determine which store a column belongs to."""
        attr_type = attribute.get("attributeType", "").upper()
        attr_name = attribute.get("name", "").lower()
        
        # Check each store's rules
        for store_name, rules in STORE_MAPPING.items():
            # Check attribute type
            if attr_type in rules["attribute_types"]:
                return store_name
                
            # Check keywords in name
            for keyword in rules["keywords"]:
                if keyword in attr_name:
                    return store_name
        
        # Default to event store
        return "event_store"
    
    async def get_column_metadata(
        self, 
        org_id: str, 
        columns: List[str]
    ) -> Dict[str, Any]:
        """Get metadata for specific columns."""
        if not columns:
            return {}
            
        cache_key = f"metadata:{org_id}:{','.join(sorted(columns))}"
        
        # Check cache
        cached = await cache_manager.get(cache_key)
        if cached:
            return cached
            
        # Fetch from API
        logger.info(f"Fetching metadata for {len(columns)} columns")
        metadata = await self.metadata_api.get_column_metadata(org_id, columns)
        
        # Process and cache
        processed = self._process_metadata(metadata)
        await cache_manager.set(cache_key, processed, ttl=settings.cache_ttl_long)
        
        return processed
    
    def _process_metadata(self, raw_metadata: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process column metadata response."""
        result = {}
        
        for item in raw_metadata:
            column = item.get("column")
            if column:
                result[column] = {
                    "values": item.get("values", []),
                    "count": item.get("count", 0),
                    "cardinality": self._determine_cardinality(item.get("count", 0))
                }
                
        return result
    
    def _determine_cardinality(self, count: int) -> str:
        """Determine if column has low or high cardinality."""
        if count <= settings.LOW_CARDINALITY_THRESHOLD:
            return "LOW"
        return "HIGH"
    
    def detect_pii_columns(self, schema: Dict[str, Any]) -> Dict[str, List[str]]:
        """Detect potential PII columns in schema."""
        pii_columns = {
            "high": [],
            "medium": [],
            "low": []
        }
        
        all_attributes = schema.get("raw_attributes", [])
        
        for attr in all_attributes:
            # Skip if already marked as PII
            if attr.get("isRawPII"):
                pii_columns["high"].append(attr["name"])
                continue
                
            # Check patterns
            col_name = attr.get("name", "").lower()
            for sensitivity, patterns in PII_PATTERNS.items():
                for pattern in patterns:
                    if pattern in col_name:
                        pii_columns[sensitivity].append(attr["name"])
                        break
                        
        return pii_columns
    
    def get_schema_summary(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Generate summary statistics for schema."""
        stores = schema.get("stores", {})
        all_attributes = schema.get("raw_attributes", [])
        
        # Count by data type
        type_counts = {}
        for attr in all_attributes:
            data_type = attr.get("dataType", "UNKNOWN")
            type_counts[data_type] = type_counts.get(data_type, 0) + 1
            
        # Count by store
        store_counts = {
            store: len(columns) 
            for store, columns in stores.items()
        }
        
        return {
            "total_columns": schema.get("total_columns", 0),
            "by_store": store_counts,
            "by_data_type": type_counts
        }