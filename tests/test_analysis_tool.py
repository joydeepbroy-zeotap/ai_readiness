"""Tests for feature analysis tool."""

import pytest
from src.tools.analysis import FeatureAnalysisTool


class TestFeatureAnalysisTool:
    """Test feature analysis tool functionality."""
    
    @pytest.fixture
    async def analysis_tool(self):
        """Create analysis tool instance."""
        tool = FeatureAnalysisTool()
        return tool
    
    @pytest.mark.asyncio
    async def test_collaborative_filtering_analysis(self, analysis_tool):
        """Test collaborative filtering use case analysis."""
        result = await analysis_tool.run(
            org_id="test_org",
            use_case="collaborative_filtering",
            include_statistics=True,
            include_quality=False,
            include_correlations=False
        )
        
        assert "org_id" in result
        assert "use_case" in result
        assert result["use_case"] == "collaborative_filtering"
        assert "features" in result
        assert "readiness_assessment" in result
        
        # Check readiness assessment
        assessment = result["readiness_assessment"]
        assert "score" in assessment
        assert "status" in assessment
        assert "strengths" in assessment
        assert "gaps" in assessment
        assert "recommendations" in assessment
    
    @pytest.mark.asyncio
    async def test_churn_prediction_analysis(self, analysis_tool):
        """Test churn prediction use case analysis."""
        result = await analysis_tool.run(
            org_id="test_org",
            use_case="churn_prediction",
            columns=["user_id", "last_activity", "engagement_score"]
        )
        
        assert result["use_case"] == "churn_prediction"
        assert "features" in result
        assert len(result["features"]) <= 3  # Should only analyze requested columns
    
    @pytest.mark.asyncio
    async def test_feature_analysis_details(self, analysis_tool):
        """Test detailed feature analysis."""
        result = await analysis_tool.run(
            org_id="test_org",
            use_case="segmentation",
            columns=["age", "gender", "income_bracket"],
            include_statistics=True
        )
        
        # Check feature details
        for feature_name, feature_info in result["features"].items():
            assert "name" in feature_info
            assert "data_type" in feature_info
            assert "attribute_type" in feature_info
            assert "is_pii" in feature_info
            assert "store" in feature_info
            assert "ml_feature_type" in feature_info
            assert "engineering_suggestions" in feature_info
            
            # Check statistics if included
            if "statistics" in feature_info:
                stats = feature_info["statistics"]
                assert "unique_values" in stats
                assert "sample_values" in stats
                assert "cardinality" in stats
    
    @pytest.mark.asyncio
    async def test_ml_feature_type_determination(self, analysis_tool):
        """Test ML feature type determination."""
        # Test through actual analysis
        result = await analysis_tool.run(
            org_id="test_org",
            use_case="collaborative_filtering",
            columns=["age", "gender", "user_id", "purchase_amount", "event_timestamp"]
        )
        
        features = result["features"]
        
        # Check expected ML feature types
        if "age" in features:
            assert features["age"]["ml_feature_type"] == "NUMERICAL"
        if "gender" in features:
            assert features["gender"]["ml_feature_type"] == "CATEGORICAL"
        if "user_id" in features:
            assert features["user_id"]["ml_feature_type"] in ["TEXT", "CATEGORICAL"]
        if "purchase_amount" in features:
            assert features["purchase_amount"]["ml_feature_type"] == "NUMERICAL"
        if "event_timestamp" in features:
            assert features["event_timestamp"]["ml_feature_type"] == "TEMPORAL"
    
    @pytest.mark.asyncio
    async def test_engineering_suggestions(self, analysis_tool):
        """Test feature engineering suggestions."""
        result = await analysis_tool.run(
            org_id="test_org",
            use_case="collaborative_filtering",
            columns=["age", "event_timestamp", "email"]
        )
        
        features = result["features"]
        
        # Check suggestions for different feature types
        if "age" in features:
            suggestions = features["age"]["engineering_suggestions"]
            assert any("normalization" in s for s in suggestions)
        
        if "event_timestamp" in features:
            suggestions = features["event_timestamp"]["engineering_suggestions"]
            assert any("date components" in s for s in suggestions)
        
        if "email" in features and features["email"]["is_pii"]:
            suggestions = features["email"]["engineering_suggestions"]
            assert any("privacy" in s for s in suggestions)
    
    @pytest.mark.asyncio
    async def test_data_quality_metrics(self, analysis_tool):
        """Test data quality metrics inclusion."""
        result = await analysis_tool.run(
            org_id="test_org",
            use_case="collaborative_filtering",
            dataset_id="test_dataset",
            table_id="test_table",
            include_quality=True,
            include_statistics=False,
            include_correlations=False
        )
        
        # Check if quality metrics are included
        if "data_quality" in result:
            quality = result["data_quality"]
            for col, metrics in quality.items():
                if "completeness" in metrics:
                    assert isinstance(metrics["completeness"], (int, float))
                    assert 0 <= metrics["completeness"] <= 100
    
    @pytest.mark.asyncio
    async def test_correlation_analysis(self, analysis_tool):
        """Test correlation analysis."""
        result = await analysis_tool.run(
            org_id="test_org",
            use_case="churn_prediction",
            dataset_id="test_dataset",
            table_id="test_table",
            include_correlations=True
        )
        
        # Check if correlations are included
        if "correlations" in result:
            correlations = result["correlations"]
            # Should be a square matrix
            for col1, col_data in correlations.items():
                for col2, corr_value in col_data.items():
                    if col1 == col2:
                        assert corr_value == 1.0  # Self-correlation
                    else:
                        assert -1 <= corr_value <= 1  # Valid correlation range
    
    @pytest.mark.asyncio
    async def test_readiness_assessment_scoring(self, analysis_tool):
        """Test readiness assessment scoring logic."""
        result = await analysis_tool.run(
            org_id="test_org",
            use_case="collaborative_filtering"
        )
        
        assessment = result["readiness_assessment"]
        
        # Check score bounds
        assert 0 <= assessment["score"] <= 100
        
        # Check status consistency
        if assessment["score"] >= 80:
            assert assessment["status"] == "READY"
        elif assessment["score"] >= 60:
            assert assessment["status"] == "PARTIALLY_READY"
        else:
            assert assessment["status"] == "NOT_READY"
        
        # Check recommendations exist for non-ready states
        if assessment["status"] != "READY":
            assert len(assessment["recommendations"]) > 0
    
    @pytest.mark.asyncio
    async def test_error_handling(self, analysis_tool):
        """Test error handling in analysis tool."""
        result = await analysis_tool.run(
            org_id="",  # Invalid org_id
            use_case="collaborative_filtering"
        )
        
        assert "error" in result
        assert "org_id" in result
        assert "use_case" in result
    
    @pytest.mark.asyncio
    async def test_default_column_selection(self, analysis_tool):
        """Test automatic column selection for use cases."""
        # Test without specifying columns
        result = await analysis_tool.run(
            org_id="test_org",
            use_case="collaborative_filtering"
        )
        
        # Should automatically select relevant columns
        assert "features" in result
        assert len(result["features"]) > 0
        
        # Check that selected columns are relevant
        feature_names = list(result["features"].keys())
        relevant_keywords = ["user", "product", "item", "purchase", "view", "rating"]
        assert any(
            any(keyword in fname.lower() for keyword in relevant_keywords)
            for fname in feature_names
        )