"""Integration tests for MCP server."""

import pytest
import asyncio
from src.server import create_app
from src.core.cache_manager import cache_manager


class TestServerIntegration:
    """Test server integration and lifecycle."""
    
    @pytest.fixture
    async def mcp_app(self):
        """Create MCP application."""
        app = create_app()
        # Manually trigger startup
        await app._startup_handler()
        yield app
        # Manually trigger shutdown
        await app._shutdown_handler()
    
    @pytest.mark.asyncio
    async def test_server_startup_shutdown(self, mcp_app):
        """Test server startup and shutdown."""
        # After startup, cache should be connected
        assert cache_manager._memory_cache is not None
        
        # Test that tools are registered
        tools = mcp_app.list_tools()
        expected_tools = [
            "schema_discovery",
            "feature_analysis",
            "query_builder",
            "compliance_checker",
            "health_check"
        ]
        
        tool_names = [tool.name for tool in tools]
        for expected in expected_tools:
            assert expected in tool_names
    
    @pytest.mark.asyncio
    async def test_health_check_tool(self, mcp_app):
        """Test health check tool."""
        # Get the health check tool
        health_tool = None
        for tool in mcp_app.list_tools():
            if tool.name == "health_check":
                health_tool = tool
                break
        
        assert health_tool is not None
        
        # Execute health check
        result = await health_tool.run()
        
        assert "status" in result
        assert result["status"] == "healthy"
        assert "version" in result
        assert "components" in result
        
        # Check components
        components = result["components"]
        assert "cache" in components
        assert "api_mode" in components
        assert components["api_mode"] == "mock"  # Should be in mock mode for tests
    
    @pytest.mark.asyncio
    async def test_tool_error_propagation(self, mcp_app):
        """Test that tool errors are properly propagated."""
        # Get schema discovery tool
        discovery_tool = None
        for tool in mcp_app.list_tools():
            if tool.name == "schema_discovery":
                discovery_tool = tool
                break
        
        # Call with invalid parameters
        result = await discovery_tool.run(
            org_id="",  # Invalid
            operation="overview"
        )
        
        # Should return error in result, not raise exception
        assert "error" in result
    
    @pytest.mark.asyncio
    async def test_end_to_end_workflow(self, mcp_app):
        """Test end-to-end workflow across multiple tools."""
        org_id = "test_org"
        
        # Step 1: Schema Discovery
        discovery_tool = next(t for t in mcp_app.list_tools() if t.name == "schema_discovery")
        schema_result = await discovery_tool.run(
            org_id=org_id,
            operation="overview"
        )
        
        assert "total_columns" in schema_result
        assert schema_result["total_columns"] > 0
        
        # Step 2: Feature Analysis
        analysis_tool = next(t for t in mcp_app.list_tools() if t.name == "feature_analysis")
        analysis_result = await analysis_tool.run(
            org_id=org_id,
            use_case="collaborative_filtering"
        )
        
        assert "readiness_assessment" in analysis_result
        assert "features" in analysis_result
        
        # Step 3: Query Builder
        query_tool = next(t for t in mcp_app.list_tools() if t.name == "query_builder")
        query_result = await query_tool.run(
            org_id=org_id,
            query_type="feature_extraction",
            dataset_id="test_dataset",
            table_id="test_table",
            use_case="collaborative_filtering"
        )
        
        assert "query" in query_result
        assert "features_included" in query_result
        
        # Step 4: Compliance Check
        compliance_tool = next(t for t in mcp_app.list_tools() if t.name == "compliance_checker")
        compliance_result = await compliance_tool.run(
            org_id=org_id,
            check_type="pii"
        )
        
        assert "pii_compliance" in compliance_result
        assert "summary" in compliance_result
    
    @pytest.mark.asyncio
    async def test_cache_persistence_across_tools(self, mcp_app):
        """Test that cache is shared across tools."""
        org_id = "cache_test_org"
        
        # First call to schema discovery
        discovery_tool = next(t for t in mcp_app.list_tools() if t.name == "schema_discovery")
        result1 = await discovery_tool.run(
            org_id=org_id,
            operation="overview"
        )
        
        # Second call should use cache
        result2 = await discovery_tool.run(
            org_id=org_id,
            operation="overview"
        )
        
        # Results should be identical
        assert result1["total_columns"] == result2["total_columns"]
        
        # Force refresh should update
        result3 = await discovery_tool.run(
            org_id=org_id,
            operation="overview",
            force_refresh=True
        )
        
        assert result3["refresh_performed"] == True
    
    @pytest.mark.asyncio
    async def test_tool_parameter_validation(self, mcp_app):
        """Test tool parameter validation."""
        # Test each tool with missing required parameters
        tools_params = {
            "schema_discovery": {"operation": "store"},  # Missing store_type
            "feature_analysis": {},  # Missing org_id
            "query_builder": {"query_type": "feature_extraction"},  # Missing dataset/table
            "compliance_checker": {}  # Missing org_id
        }
        
        for tool_name, params in tools_params.items():
            tool = next(t for t in mcp_app.list_tools() if t.name == tool_name)
            result = await tool.run(**params)
            
            # Should handle gracefully
            assert isinstance(result, dict)
            if "error" in result:
                assert result["error"] is not None
    
    @pytest.mark.asyncio
    async def test_concurrent_tool_execution(self, mcp_app):
        """Test concurrent execution of multiple tools."""
        org_id = "concurrent_test"
        
        # Get all analysis tools
        discovery = next(t for t in mcp_app.list_tools() if t.name == "schema_discovery")
        analysis = next(t for t in mcp_app.list_tools() if t.name == "feature_analysis")
        compliance = next(t for t in mcp_app.list_tools() if t.name == "compliance_checker")
        
        # Execute concurrently
        results = await asyncio.gather(
            discovery.run(org_id=org_id, operation="overview"),
            analysis.run(org_id=org_id, use_case="collaborative_filtering"),
            compliance.run(org_id=org_id, check_type="pii"),
            return_exceptions=True
        )
        
        # All should complete successfully
        for result in results:
            assert not isinstance(result, Exception)
            assert isinstance(result, dict)
    
    @pytest.mark.asyncio
    async def test_mock_vs_live_mode_configuration(self):
        """Test configuration for mock vs live mode."""
        # Current tests run in mock mode
        from src.config import settings
        assert settings.use_mock_api == True
        
        # Test that mock implementations are used
        from src.tools.discovery import SchemaDiscoveryTool
        tool = SchemaDiscoveryTool()
        
        assert tool.catalog_api.__class__.__name__ == "MockCatalogAPI"
        assert tool.metadata_api.__class__.__name__ == "MockMetadataAPI"