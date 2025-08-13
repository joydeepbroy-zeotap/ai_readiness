"""Tests for schema discovery tool."""

import pytest
from src.tools.discovery import SchemaDiscoveryTool
from src.core.exceptions import ValidationError


class TestSchemaDiscoveryTool:
    """Test schema discovery tool functionality."""
    
    @pytest.fixture
    async def discovery_tool(self):
        """Create discovery tool instance."""
        tool = SchemaDiscoveryTool()
        return tool
    
    @pytest.mark.asyncio
    async def test_overview_operation(self, discovery_tool):
        """Test schema overview operation."""
        result = await discovery_tool.run(
            org_id="test_org",
            operation="overview"
        )
        
        assert "org_id" in result
        assert "total_columns" in result
        assert "store_distribution" in result
        assert "stores" in result
        assert result["org_id"] == "test_org"
    
    @pytest.mark.asyncio
    async def test_store_operation(self, discovery_tool):
        """Test store-specific schema operation."""
        result = await discovery_tool.run(
            org_id="test_org",
            operation="store",
            store_type="profile_store"
        )
        
        assert "store_type" in result
        assert result["store_type"] == "profile_store"
        assert "columns" in result
        assert "column_count" in result
        assert isinstance(result["columns"], list)
        
        # Verify column structure
        if result["columns"]:
            col = result["columns"][0]
            assert "name" in col
            assert "data_type" in col
            assert "attribute_type" in col
            assert "is_pii" in col
    
    @pytest.mark.asyncio
    async def test_store_operation_validation(self, discovery_tool):
        """Test store operation validation."""
        # Missing store_type
        result = await discovery_tool.run(
            org_id="test_org",
            operation="store"
        )
        assert "error" in result
        assert "store_type required" in result["error"]
    
    @pytest.mark.asyncio
    async def test_columns_operation(self, discovery_tool):
        """Test specific columns operation."""
        result = await discovery_tool.run(
            org_id="test_org",
            operation="columns",
            columns=["user_id", "email", "age"]
        )
        
        assert "requested_columns" in result
        assert "found_columns" in result
        assert "missing_columns" in result
        assert len(result["requested_columns"]) == 3
        
        # Check found columns have metadata
        for col in result["found_columns"]:
            assert "name" in col
            assert "cardinality" in col
            assert "metadata" in col
    
    @pytest.mark.asyncio
    async def test_search_operation(self, discovery_tool):
        """Test column search operation."""
        result = await discovery_tool.run(
            org_id="test_org",
            operation="search",
            search_query="user"
        )
        
        assert "search_query" in result
        assert result["search_query"] == "user"
        assert "total_results" in result
        assert "results_by_store" in result
        
        # Verify results are categorized by store
        for store, columns in result["results_by_store"].items():
            assert isinstance(columns, list)
            for col in columns:
                assert "user" in col["name"].lower()
    
    @pytest.mark.asyncio
    async def test_pii_operation(self, discovery_tool):
        """Test PII detection operation."""
        result = await discovery_tool.run(
            org_id="test_org",
            operation="pii"
        )
        
        assert "marked_pii" in result
        assert "detected_pii" in result
        assert "compliance_notes" in result
        
        # Check marked PII structure
        assert "count" in result["marked_pii"]
        assert "columns" in result["marked_pii"]
        
        # Check detected PII structure
        assert "high_sensitivity" in result["detected_pii"]
        assert "medium_sensitivity" in result["detected_pii"]
        assert "low_sensitivity" in result["detected_pii"]
        
        # Verify compliance notes
        assert isinstance(result["compliance_notes"], list)
        assert len(result["compliance_notes"]) > 0
    
    @pytest.mark.asyncio
    async def test_force_refresh(self, discovery_tool):
        """Test force refresh functionality."""
        # First call without refresh
        result1 = await discovery_tool.run(
            org_id="test_org",
            operation="overview",
            force_refresh=False
        )
        
        # Second call with refresh
        result2 = await discovery_tool.run(
            org_id="test_org",
            operation="overview",
            force_refresh=True
        )
        
        assert result2["refresh_performed"] == True
        assert result1["org_id"] == result2["org_id"]
    
    @pytest.mark.asyncio
    async def test_invalid_operation(self, discovery_tool):
        """Test invalid operation handling."""
        result = await discovery_tool.run(
            org_id="test_org",
            operation="invalid_op"
        )
        
        assert "error" in result
        assert "Unknown operation" in result["error"]
    
    @pytest.mark.asyncio
    async def test_error_handling(self, discovery_tool):
        """Test error handling in discovery tool."""
        # Test with empty org_id
        result = await discovery_tool.run(
            org_id="",
            operation="overview"
        )
        
        assert "error" in result
        assert "org_id" in result
    
    @pytest.mark.asyncio
    async def test_metadata_enrichment(self, discovery_tool):
        """Test that column metadata is properly enriched."""
        result = await discovery_tool.run(
            org_id="test_org",
            operation="store",
            store_type="profile_store"
        )
        
        # Check that columns have metadata
        for col in result["columns"][:10]:  # Check first 10 columns
            if "metadata" in col and col["metadata"]:
                assert "values" in col["metadata"] or "count" in col["metadata"]