"""Tests for schema manager."""

import pytest
from unittest.mock import AsyncMock, MagicMock
from src.core.schema_manager import SchemaManager
from src.core.exceptions import ValidationError


class TestSchemaManager:
    """Test schema manager functionality."""
    
    @pytest.mark.asyncio
    async def test_get_schema(self, schema_manager, sample_schema):
        """Test getting and categorizing schema."""
        # Mock the catalog API response
        schema_manager.catalog_api.get_catalog_schema = AsyncMock(
            return_value={
                "orgId": "test_org",
                "attributes": sample_schema["raw_attributes"],
                "totalCount": len(sample_schema["raw_attributes"])
            }
        )
        
        result = await schema_manager.get_schema("test_org")
        
        assert result["org_id"] == "test_org"
        assert "stores" in result
        assert all(store in result["stores"] for store in [
            "profile_store", "event_store", "calculated_attribute", "consent_store"
        ])
    
    @pytest.mark.asyncio
    async def test_get_schema_validation(self, schema_manager):
        """Test schema validation."""
        with pytest.raises(ValidationError) as exc_info:
            await schema_manager.get_schema("")
        
        assert "Organization ID is required" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_categorize_schema(self, schema_manager):
        """Test schema categorization logic."""
        raw_schema = {
            "orgId": "test_org",
            "attributes": [
                {"name": "user_id", "attributeType": "IDENTITY", "dataType": "STRING"},
                {"name": "email", "attributeType": "IDENTITY", "dataType": "STRING", "isRawPII": True},
                {"name": "event_timestamp", "attributeType": "EVENT", "dataType": "TIMESTAMP"},
                {"name": "ltv_score", "attributeType": "CALCULATED", "dataType": "FLOAT"},
                {"name": "gdpr_consent", "attributeType": "CONSENT", "dataType": "BOOLEAN"},
                {"name": "unknown_field", "attributeType": "UNKNOWN", "dataType": "STRING"}
            ]
        }
        
        categorized = schema_manager._categorize_schema(raw_schema)
        
        assert categorized["org_id"] == "test_org"
        assert categorized["total_columns"] == 6
        assert len(categorized["stores"]["profile_store"]) == 2  # user_id, email
        assert len(categorized["stores"]["event_store"]) == 2  # event_timestamp, unknown_field (default)
        assert len(categorized["stores"]["calculated_attribute"]) == 1  # ltv_score
        assert len(categorized["stores"]["consent_store"]) == 1  # gdpr_consent
    
    @pytest.mark.asyncio
    async def test_determine_store(self, schema_manager):
        """Test store determination logic."""
        # Test by attribute type
        assert schema_manager._determine_store(
            {"attributeType": "IDENTITY", "name": "test"}
        ) == "profile_store"
        
        assert schema_manager._determine_store(
            {"attributeType": "CALCULATED", "name": "test"}
        ) == "calculated_attribute"
        
        # Test by keywords
        assert schema_manager._determine_store(
            {"attributeType": "UNKNOWN", "name": "user_email"}
        ) == "profile_store"
        
        assert schema_manager._determine_store(
            {"attributeType": "UNKNOWN", "name": "event_count"}
        ) == "event_store"
        
        # Test default
        assert schema_manager._determine_store(
            {"attributeType": "UNKNOWN", "name": "random_field"}
        ) == "event_store"
    
    @pytest.mark.asyncio
    async def test_get_column_metadata(self, schema_manager, sample_metadata):
        """Test getting column metadata."""
        schema_manager.metadata_api.get_column_metadata = AsyncMock(
            return_value=sample_metadata
        )
        
        columns = ["user_id", "age"]
        result = await schema_manager.get_column_metadata("test_org", columns)
        
        assert "user_id" in result
        assert "age" in result
        assert result["user_id"]["cardinality"] == "HIGH"
        assert result["age"]["cardinality"] == "HIGH"
        assert len(result["user_id"]["values"]) > 0
    
    @pytest.mark.asyncio
    async def test_detect_pii_columns(self, schema_manager):
        """Test PII detection."""
        schema = {
            "raw_attributes": [
                {"name": "user_id", "isRawPII": False},
                {"name": "email", "isRawPII": True},
                {"name": "phone_number", "isRawPII": False},  # Should be detected
                {"name": "ssn", "isRawPII": False},  # Should be detected as high
                {"name": "age", "isRawPII": False},
                {"name": "ip_address", "isRawPII": False},  # Should be detected as medium
            ]
        }
        
        pii_columns = schema_manager.detect_pii_columns(schema)
        
        assert "email" in pii_columns["high"]  # Marked as PII
        assert "ssn" in pii_columns["high"]  # Detected by pattern
        assert "phone_number" in pii_columns["high"]  # Detected by pattern
        assert "ip_address" in pii_columns["medium"]  # Detected by pattern
        assert "age" not in pii_columns["high"]
        assert "age" not in pii_columns["medium"]
        assert "age" not in pii_columns["low"]
    
    @pytest.mark.asyncio
    async def test_get_schema_summary(self, schema_manager, sample_schema):
        """Test schema summary generation."""
        # Add raw_attributes to sample schema
        sample_schema["raw_attributes"] = [
            {"name": "col1", "dataType": "STRING"},
            {"name": "col2", "dataType": "STRING"},
            {"name": "col3", "dataType": "INTEGER"},
            {"name": "col4", "dataType": "FLOAT"},
            {"name": "col5", "dataType": "BOOLEAN"},
        ]
        
        summary = schema_manager.get_schema_summary(sample_schema)
        
        assert summary["total_columns"] == 25
        assert summary["by_store"]["profile_store"] == 3
        assert summary["by_store"]["event_store"] == 2
        assert summary["by_data_type"]["STRING"] == 2
        assert summary["by_data_type"]["INTEGER"] == 1
        assert summary["by_data_type"]["FLOAT"] == 1
        assert summary["by_data_type"]["BOOLEAN"] == 1
    
    @pytest.mark.asyncio
    async def test_cardinality_determination(self, schema_manager):
        """Test cardinality determination."""
        # Low cardinality
        assert schema_manager._determine_cardinality(10) == "LOW"
        assert schema_manager._determine_cardinality(50) == "LOW"
        
        # High cardinality
        assert schema_manager._determine_cardinality(500) == "HIGH"
        assert schema_manager._determine_cardinality(10000) == "HIGH"
    
    @pytest.mark.asyncio
    async def test_process_metadata(self, schema_manager, sample_metadata):
        """Test metadata processing."""
        processed = schema_manager._process_metadata(sample_metadata)
        
        assert "user_id" in processed
        assert "age" in processed
        
        user_id_meta = processed["user_id"]
        assert user_id_meta["count"] == 100000
        assert len(user_id_meta["values"]) == 100
        assert user_id_meta["cardinality"] == "HIGH"
        
        age_meta = processed["age"]
        assert age_meta["count"] == 100000
        assert age_meta["cardinality"] == "HIGH"