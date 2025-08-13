#!/usr/bin/env python3
"""Debug runner for testing MCP tools directly."""

import asyncio
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

# Set environment variables
os.environ["USE_MOCK_API"] = "false"  # Change to "false" for real API

# Import the tool classes directly
from src.tools import (
    SchemaDiscoveryTool,
    FeatureAnalysisTool,
    QueryBuilderTool,
    ComplianceCheckerTool
)


async def debug_schema_discovery():
    """Test schema discovery tool."""
    print("\n=== Testing Schema Discovery ===")
    tool = SchemaDiscoveryTool()
    
    # Test 1: Overview
    result = await tool.run(
        org_id="1914",
        operation="overview"
    )
    print(f"Overview result: {result}")
    
    # Test 2: Store schema
    result = await tool.run(
        org_id="1914",
        operation="store",
        store_type="profile_store"
    )
    print(f"Store schema result: {result}")
    
    # Test 3: Search columns
    result = await tool.run(
        org_id="test_org_123",
        operation="search",
        search_query="user"
    )
    print(f"Search result: {result}")


async def debug_feature_analysis():
    """Test feature analysis tool."""
    print("\n=== Testing Feature Analysis ===")
    tool = FeatureAnalysisTool()
    
    result = await tool.run(
        org_id="test_org_123",
        use_case="collaborative_filtering",
        columns=["user_id", "product_id", "rating"],
        include_statistics=True
    )
    print(f"Feature analysis result: {result}")


async def debug_query_builder():
    """Test query builder tool."""
    print("\n=== Testing Query Builder ===")
    tool = QueryBuilderTool()
    
    result = await tool.run(
        org_id="test_org_123",
        query_type="feature_extraction",
        use_case="collaborative_filtering",
        dataset_id="test_dataset",
        table_id="events",
        features=["user_id", "product_id", "timestamp"],
        limit=1000
    )
    print(f"Query builder result: {result}")


async def debug_compliance_checker():
    """Test compliance checker tool."""
    print("\n=== Testing Compliance Checker ===")
    tool = ComplianceCheckerTool()
    
    result = await tool.run(
        org_id="test_org_123",
        check_type="pii",
        regulations=["GDPR", "CCPA"],
        generate_report=True
    )
    print(f"Compliance result: {result}")


async def main():
    """Run all tests."""
    # Health check - simple dict return
    print("=== Testing Health Check ===")
    from src.config import settings
    health = {
        "status": "healthy",
        "version": "1.0.0",
        "api_mode": "mock" if settings.use_mock_api else "live"
    }
    print(f"Health: {health}")
    
    # Run specific test based on command line argument
    if len(sys.argv) > 1:
        test_name = sys.argv[1]
        if test_name == "schema":
            await debug_schema_discovery()
        elif test_name == "feature":
            await debug_feature_analysis()
        elif test_name == "query":
            await debug_query_builder()
        elif test_name == "compliance":
            await debug_compliance_checker()
        else:
            print(f"Unknown test: {test_name}")
            print("Available tests: schema, feature, query, compliance")
    else:
        # Run all tests
        await debug_schema_discovery()
        await debug_feature_analysis()
        await debug_query_builder()
        await debug_compliance_checker()


if __name__ == "__main__":
    # Set breakpoint here and debug in PyCharm
    asyncio.run(main())
