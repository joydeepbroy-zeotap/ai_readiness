"""Shared test fixtures and configuration."""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
import os

# Set test environment
os.environ["USE_MOCK_API"] = "true"
os.environ["BEARER_TOKEN"] = "test_token"


@pytest.fixture
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def mock_catalog_api():
    """Mock Catalog API fixture."""
    from src.integrations.mock_api import MockCatalogAPI
    api = MockCatalogAPI()
    await api.connect()
    yield api
    await api.disconnect()


@pytest.fixture
async def mock_metadata_api():
    """Mock Metadata API fixture."""
    from src.integrations.mock_api import MockMetadataAPI
    api = MockMetadataAPI()
    await api.connect()
    yield api
    await api.disconnect()


@pytest.fixture
async def mock_bigquery_client():
    """Mock BigQuery client fixture."""
    from src.integrations.mock_api import MockBigQueryClient
    client = MockBigQueryClient()
    await client.connect()
    yield client
    await client.disconnect()


@pytest.fixture
async def schema_manager(mock_catalog_api, mock_metadata_api):
    """Schema manager fixture."""
    from src.core.schema_manager import SchemaManager
    return SchemaManager(mock_catalog_api, mock_metadata_api)


@pytest.fixture
async def cache_manager():
    """Cache manager fixture."""
    from src.core.cache_manager import CacheManager
    manager = CacheManager()
    await manager.connect()
    yield manager
    await manager.disconnect()


@pytest.fixture
def sample_schema():
    """Sample schema data."""
    return {
        "org_id": "test_org",
        "total_columns": 25,
        "stores": {
            "profile_store": [
                {"name": "user_id", "dataType": "STRING", "attributeType": "IDENTITY"},
                {"name": "email", "dataType": "STRING", "attributeType": "IDENTITY", "isRawPII": True},
                {"name": "age", "dataType": "INTEGER", "attributeType": "DEMOGRAPHIC"}
            ],
            "event_store": [
                {"name": "event_timestamp", "dataType": "TIMESTAMP", "attributeType": "EVENT"},
                {"name": "product_viewed", "dataType": "STRING", "attributeType": "BEHAVIORAL"}
            ],
            "calculated_attribute": [
                {"name": "ltv_score", "dataType": "FLOAT", "attributeType": "CALCULATED"}
            ],
            "consent_store": [
                {"name": "gdpr_consent", "dataType": "BOOLEAN", "attributeType": "CONSENT"}
            ]
        },
        "raw_attributes": []
    }


@pytest.fixture
def sample_metadata():
    """Sample metadata response."""
    return [
        {
            "column": "user_id",
            "values": [f"ID_{i:05d}" for i in range(1, 101)],
            "count": 100000,
            "nullCount": 0,
            "totalCount": 100000
        },
        {
            "column": "age",
            "values": list(range(18, 80)),
            "count": 100000,
            "nullCount": 5000,
            "totalCount": 105000
        }
    ]