"""Schema discovery tool for MCP server."""

from typing import Dict, Any, Optional, List
import structlog

from ..core.schema_manager import SchemaManager
from ..core.exceptions import ValidationError
from ..integrations import CatalogAPI, MetadataAPI, MockCatalogAPI, MockMetadataAPI
from ..config import settings

logger = structlog.get_logger()


class SchemaDiscoveryTool:
    """Tool for discovering and exploring schema information."""
    
    def __init__(self):
        # Initialize APIs based on mock setting
        if settings.use_mock_api:
            self.catalog_api = MockCatalogAPI()
            self.metadata_api = MockMetadataAPI()
        else:
            self.catalog_api = CatalogAPI()
            self.metadata_api = MetadataAPI()
            
        self.schema_manager = SchemaManager(self.catalog_api, self.metadata_api)
    
    async def run(
        self,
        org_id: str,
        operation: str = "overview",
        store_type: Optional[str] = None,
        columns: Optional[List[str]] = None,
        search_query: Optional[str] = None,
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        Run schema discovery operations.
        
        Args:
            org_id: Organization ID
            operation: Operation to perform (overview, store, columns, search, pii)
            store_type: Store type for filtering (profile_store, event_store, etc.)
            columns: Specific columns to analyze
            search_query: Search query for finding columns
            force_refresh: Force refresh cached data
            
        Returns:
            Discovery results based on operation
        """
        try:
            # Connect to APIs
            await self.catalog_api.connect()
            await self.metadata_api.connect()
            
            if operation == "overview":
                return await self._get_overview(org_id, force_refresh)
                
            elif operation == "store":
                if not store_type:
                    raise ValidationError("store_type required for store operation")
                return await self._get_store_schema(org_id, store_type, force_refresh)
                
            elif operation == "columns":
                if not columns:
                    raise ValidationError("columns required for columns operation")
                return await self._get_column_details(org_id, columns)
                
            elif operation == "search":
                if not search_query:
                    raise ValidationError("search_query required for search operation")
                return await self._search_columns(org_id, search_query)
                
            elif operation == "pii":
                return await self._get_pii_columns(org_id, force_refresh)
                
            else:
                raise ValidationError(f"Unknown operation: {operation}")
                
        except Exception as e:
            logger.error(f"Schema discovery failed: {e}")
            return {
                "error": str(e),
                "operation": operation,
                "org_id": org_id
            }
        finally:
            # Cleanup
            await self.catalog_api.disconnect()
            await self.metadata_api.disconnect()
    
    async def _get_overview(self, org_id: str, force_refresh: bool) -> Dict[str, Any]:
        """Get schema overview."""
        schema = await self.schema_manager.get_schema(org_id, force_refresh)
        summary = self.schema_manager.get_schema_summary(schema)
        
        return {
            "org_id": org_id,
            "total_columns": summary["total_columns"],
            "store_distribution": summary["by_store"],
            "data_type_distribution": summary["by_data_type"],
            "stores": {
                store: len(columns)
                for store, columns in schema["stores"].items()
            },
            "refresh_performed": force_refresh
        }
    
    async def _get_store_schema(
        self, 
        org_id: str, 
        store_type: str,
        force_refresh: bool
    ) -> Dict[str, Any]:
        """Get schema for specific store."""
        schema = await self.schema_manager.get_schema(org_id, force_refresh)
        
        if store_type not in schema["stores"]:
            raise ValidationError(
                f"Invalid store_type: {store_type}",
                field="store_type"
            )
        
        store_columns = schema["stores"][store_type]
        
        # Get metadata for first 10 columns
        column_names = [col["name"] for col in store_columns] #[:10]]
        # if column_names:
        #     metadata = await self.schema_manager.get_column_metadata(
        #         org_id,
        #         column_names
        #     )
        # else:
        #     metadata = {}
        
        return {
            "org_id": org_id,
            "store_type": store_type,
            "column_count": len(store_columns),
            "columns": [
                {
                    "name": col["name"],
                    "data_type": col.get("dataType"),
                    "attribute_type": col.get("attributeType"),
                    "is_pii": col.get("isRawPII", False),
                    # "metadata": metadata.get(col["name"], {})
                }
                for col in store_columns  # Limit to 50 columns
            ],
            # "has_more": len(store_columns) > 50
        }
    
    async def _get_column_details(
        self, 
        org_id: str, 
        columns: List[str]
    ) -> Dict[str, Any]:
        """Get detailed information for specific columns."""
        # Get schema to find column info
        schema = await self.schema_manager.get_schema(org_id)
        all_columns = schema["raw_attributes"]
        
        # Find requested columns
        column_map = {col["name"]: col for col in all_columns}
        found_columns = []
        missing_columns = []
        
        for col_name in columns:
            if col_name in column_map:
                found_columns.append(column_map[col_name])
            else:
                missing_columns.append(col_name)
        
        # Get metadata for found columns
        if found_columns:
            col_names = [col["name"] for col in found_columns]
            metadata = await self.schema_manager.get_column_metadata(
                org_id,
                col_names
            )
            
            # Get cardinality analysis
            cardinality = await self.metadata_api.analyze_cardinality(
                org_id,
                col_names
            )
        else:
            metadata = {}
            cardinality = {}
        
        return {
            "org_id": org_id,
            "requested_columns": columns,
            "found_columns": [
                {
                    "name": col["name"],
                    "data_type": col.get("dataType"),
                    "attribute_type": col.get("attributeType"),
                    "is_pii": col.get("isRawPII", False),
                    "cardinality": cardinality.get(col["name"], "UNKNOWN"),
                    "metadata": metadata.get(col["name"], {})
                }
                for col in found_columns
            ],
            "missing_columns": missing_columns
        }
    
    async def _search_columns(
        self, 
        org_id: str, 
        search_query: str
    ) -> Dict[str, Any]:
        """Search for columns matching query."""
        results = await self.catalog_api.search_attributes(
            org_id,
            search_query,
            size=50
        )
        
        # Categorize by store
        schema = await self.schema_manager.get_schema(org_id)
        categorized = {}
        
        for attr in results:
            store = self.schema_manager._determine_store(attr)
            if store not in categorized:
                categorized[store] = []
            categorized[store].append({
                "name": attr["name"],
                "data_type": attr.get("dataType"),
                "attribute_type": attr.get("attributeType"),
                "is_pii": attr.get("isRawPII", False)
            })
        
        return {
            "org_id": org_id,
            "search_query": search_query,
            "total_results": len(results),
            "results_by_store": categorized
        }
    
    async def _get_pii_columns(
        self, 
        org_id: str,
        force_refresh: bool
    ) -> Dict[str, Any]:
        """Get PII columns analysis."""
        schema = await self.schema_manager.get_schema(org_id, force_refresh)
        
        # Get marked PII columns
        marked_pii = await self.catalog_api.get_pii_attributes(org_id)
        
        # Detect potential PII
        detected_pii = self.schema_manager.detect_pii_columns(schema)
        
        return {
            "org_id": org_id,
            "marked_pii": {
                "count": len(marked_pii),
                "columns": [
                    {
                        "name": col["name"],
                        "data_type": col.get("dataType"),
                        "attribute_type": col.get("attributeType")
                    }
                    for col in marked_pii
                ]
            },
            "detected_pii": {
                "high_sensitivity": detected_pii["high"],
                "medium_sensitivity": detected_pii["medium"],
                "low_sensitivity": detected_pii["low"]
            },
            "compliance_notes": [
                "Ensure GDPR compliance for EU users",
                "CCPA compliance required for California residents",
                "Consider data minimization principles",
                "Implement proper access controls"
            ]
        }
