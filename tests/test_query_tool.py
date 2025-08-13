"""Tests for query builder tool."""

import pytest
from src.tools.query import QueryBuilderTool


class TestQueryBuilderTool:
    """Test query builder tool functionality."""
    
    @pytest.fixture
    async def query_tool(self):
        """Create query tool instance."""
        tool = QueryBuilderTool()
        return tool
    
    @pytest.mark.asyncio
    async def test_feature_extraction_query(self, query_tool):
        """Test feature extraction query generation."""
        result = await query_tool.run(
            org_id="test_org",
            query_type="feature_extraction",
            use_case="collaborative_filtering",
            dataset_id="test_dataset",
            table_id="test_table",
            features=["user_id", "product_id", "purchase_amount"],
            limit=1000
        )
        
        assert "query" in result
        assert "query_type" in result
        assert result["query_type"] == "feature_extraction"
        assert "features_included" in result
        assert "features_not_found" in result
        assert "recommendations" in result
        
        # Check query structure
        query = result["query"]
        assert "SELECT" in query
        assert "FROM" in query
        assert "test_dataset.test_table" in query
        assert "LIMIT 1000" in query
        
        # Check features are in query
        for feature in result["features_included"]:
            assert feature in query
    
    @pytest.mark.asyncio
    async def test_aggregation_query(self, query_tool):
        """Test aggregation query generation."""
        result = await query_tool.run(
            org_id="test_org",
            query_type="aggregation",
            use_case="churn_prediction",
            dataset_id="test_dataset",
            table_id="test_table"
        )
        
        assert result["query_type"] == "aggregation"
        assert "query" in result
        assert "aggregation_level" in result
        assert "features_created" in result
        
        # Check query has aggregation functions
        query = result["query"]
        assert "GROUP BY" in query
        assert "COUNT" in query or "SUM" in query or "AVG" in query
        
        # Check features created list
        assert len(result["features_created"]) > 0
    
    @pytest.mark.asyncio
    async def test_sampling_query(self, query_tool):
        """Test sampling query generation."""
        result = await query_tool.run(
            org_id="test_org",
            query_type="sampling",
            dataset_id="test_dataset",
            table_id="test_table",
            limit=10000
        )
        
        assert result["query_type"] == "sampling"
        assert "sampling_strategies" in result
        assert "recommended_strategy" in result
        assert "notes" in result
        
        # Check multiple sampling strategies
        strategies = result["sampling_strategies"]
        assert "random_sample" in strategies
        assert "stratified_sample" in strategies
        assert "time_based_sample" in strategies
        
        # Check each strategy is a valid query
        for strategy_name, query in strategies.items():
            assert "SELECT" in query
            assert "FROM" in query
            assert str(result["sample_size"]) in query or "RAND()" in query
    
    @pytest.mark.asyncio
    async def test_query_with_filters(self, query_tool):
        """Test query generation with filters."""
        result = await query_tool.run(
            org_id="test_org",
            query_type="feature_extraction",
            dataset_id="test_dataset",
            table_id="test_table",
            features=["user_id", "age", "purchase_amount"],
            filters={
                "age": {"min": 18, "max": 65},
                "country": "USA"
            }
        )
        
        query = result["query"]
        assert "WHERE" in query
        assert "age >= 18" in query
        assert "age <= 65" in query
        assert "country = 'USA'" in query
    
    @pytest.mark.asyncio
    async def test_query_execution(self, query_tool):
        """Test query execution functionality."""
        result = await query_tool.run(
            org_id="test_org",
            query_type="feature_extraction",
            dataset_id="test_dataset",
            table_id="test_table",
            features=["user_id", "product_id"],
            execute=True,
            limit=10
        )
        
        # With mock API, should get execution results
        if "execution_results" in result:
            assert "rows_returned" in result["execution_results"]
            assert "sample_data" in result["execution_results"]
    
    @pytest.mark.asyncio
    async def test_use_case_specific_queries(self, query_tool):
        """Test use case specific query generation."""
        # Collaborative filtering
        cf_result = await query_tool.run(
            org_id="test_org",
            query_type="aggregation",
            use_case="collaborative_filtering",
            dataset_id="test_dataset",
            table_id="test_table"
        )
        
        cf_query = cf_result["query"]
        assert "user_item_stats" in cf_query or "item_stats" in cf_query
        assert "products_interacted" in str(cf_result["features_created"])
        
        # Churn prediction
        churn_result = await query_tool.run(
            org_id="test_org",
            query_type="aggregation",
            use_case="churn_prediction",
            dataset_id="test_dataset",
            table_id="test_table"
        )
        
        churn_query = churn_result["query"]
        assert "days_since_last_activity" in churn_query or "active_days" in churn_query
        assert "total_events" in str(churn_result["features_created"])
    
    @pytest.mark.asyncio
    async def test_invalid_query_type(self, query_tool):
        """Test invalid query type handling."""
        result = await query_tool.run(
            org_id="test_org",
            query_type="invalid_type",
            dataset_id="test_dataset",
            table_id="test_table"
        )
        
        assert "error" in result
        assert "Unknown query_type" in result["error"]
    
    @pytest.mark.asyncio
    async def test_missing_required_params(self, query_tool):
        """Test handling of missing required parameters."""
        # Missing dataset_id and table_id
        result = await query_tool.run(
            org_id="test_org",
            query_type="feature_extraction"
        )
        
        assert "error" in result or "query" not in result
        if "error" in result:
            assert "dataset_id and table_id required" in result["error"]
    
    @pytest.mark.asyncio
    async def test_default_feature_selection(self, query_tool):
        """Test automatic feature selection."""
        result = await query_tool.run(
            org_id="test_org",
            query_type="feature_extraction",
            use_case="collaborative_filtering",
            dataset_id="test_dataset",
            table_id="test_table"
        )
        
        # Should automatically select relevant features
        assert "features_included" in result
        assert len(result["features_included"]) > 0
        
        # Check recommendations
        assert "recommendations" in result
        assert len(result["recommendations"]) > 0
    
    @pytest.mark.asyncio
    async def test_query_optimization_recommendations(self, query_tool):
        """Test query optimization recommendations."""
        result = await query_tool.run(
            org_id="test_org",
            query_type="feature_extraction",
            use_case="churn_prediction",
            dataset_id="test_dataset",
            table_id="test_table",
            features=["user_id", "event_type", "event_count"]
        )
        
        recommendations = result["recommendations"]
        
        # Should include general optimization tips
        assert any("index" in r.lower() for r in recommendations)
        assert any("partition" in r.lower() for r in recommendations)
        
        # Should include data quality recommendations
        assert any("null" in r.lower() or "validation" in r.lower() for r in recommendations)