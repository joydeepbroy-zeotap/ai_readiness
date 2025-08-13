"""Mock implementations for testing and development."""

import random
from typing import Dict, Any, List, Optional
import structlog

from ..config import STORE_MAPPING, PII_PATTERNS

logger = structlog.get_logger()


class MockCatalogAPI:
    """Mock implementation of Catalog API."""
    
    def __init__(self):
        self.base_url = "mock://catalog"
        
    async def connect(self):
        """Mock connect."""
        pass
        
    async def disconnect(self):
        """Mock disconnect."""
        pass
        
    async def health_check(self) -> bool:
        """Mock health check."""
        return True
    
    async def get_catalog_schema(
        self, 
        org_id: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Return mock catalog schema."""
        # Generate mock attributes based on store types
        attributes = []
        
        # Profile store attributes
        profile_attrs = [
            {"name": "user_id", "dataType": "STRING", "attributeType": "IDENTITY", "isRawPII": False},
            {"name": "email", "dataType": "STRING", "attributeType": "IDENTITY", "isRawPII": True},
            {"name": "phone_number", "dataType": "STRING", "attributeType": "IDENTITY", "isRawPII": True},
            {"name": "age", "dataType": "INTEGER", "attributeType": "DEMOGRAPHIC", "isRawPII": False},
            {"name": "gender", "dataType": "STRING", "attributeType": "DEMOGRAPHIC", "isRawPII": False},
            {"name": "city", "dataType": "STRING", "attributeType": "LOCATION", "isRawPII": False},
            {"name": "country", "dataType": "STRING", "attributeType": "LOCATION", "isRawPII": False},
            {"name": "income_bracket", "dataType": "STRING", "attributeType": "DEMOGRAPHIC", "isRawPII": False},
        ]
        
        # Event store attributes
        event_attrs = [
            {"name": "event_timestamp", "dataType": "TIMESTAMP", "attributeType": "EVENT", "isRawPII": False},
            {"name": "event_type", "dataType": "STRING", "attributeType": "EVENT", "isRawPII": False},
            {"name": "product_viewed", "dataType": "STRING", "attributeType": "BEHAVIORAL", "isRawPII": False},
            {"name": "product_purchased", "dataType": "STRING", "attributeType": "BEHAVIORAL", "isRawPII": False},
            {"name": "purchase_amount", "dataType": "FLOAT", "attributeType": "BEHAVIORAL", "isRawPII": False},
            {"name": "session_duration", "dataType": "INTEGER", "attributeType": "BEHAVIORAL", "isRawPII": False},
            {"name": "page_views", "dataType": "INTEGER", "attributeType": "BEHAVIORAL", "isRawPII": False},
        ]
        
        # Calculated attributes
        calc_attrs = [
            {"name": "ltv_score", "dataType": "FLOAT", "attributeType": "CALCULATED", "isRawPII": False},
            {"name": "churn_probability", "dataType": "FLOAT", "attributeType": "CALCULATED", "isRawPII": False},
            {"name": "engagement_score", "dataType": "FLOAT", "attributeType": "CALCULATED", "isRawPII": False},
            {"name": "preferred_category", "dataType": "STRING", "attributeType": "CALCULATED", "isRawPII": False},
        ]
        
        # Consent store attributes
        consent_attrs = [
            {"name": "gdpr_consent", "dataType": "BOOLEAN", "attributeType": "CONSENT", "isRawPII": False},
            {"name": "marketing_consent", "dataType": "BOOLEAN", "attributeType": "CONSENT", "isRawPII": False},
            {"name": "consent_timestamp", "dataType": "TIMESTAMP", "attributeType": "CONSENT", "isRawPII": False},
        ]
        
        # Combine all attributes
        all_attrs = profile_attrs + event_attrs + calc_attrs + consent_attrs
        
        # Apply filters if provided
        if filters:
            if "attributeType" in filters:
                filter_type = filters["attributeType"]
                all_attrs = [a for a in all_attrs if a["attributeType"] == filter_type]
            if "isRawPII" in filters:
                filter_pii = filters["isRawPII"]
                all_attrs = [a for a in all_attrs if a["isRawPII"] == filter_pii]
        
        return {
            "orgId": org_id,
            "attributes": all_attrs,
            "totalCount": len(all_attrs)
        }
    
    async def search_attributes(
        self,
        org_id: str,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        size: int = 100
    ) -> List[Dict[str, Any]]:
        """Search mock attributes."""
        schema = await self.get_catalog_schema(org_id, filters)
        attributes = schema["attributes"]
        
        # Simple search by query
        if query:
            query_lower = query.lower()
            attributes = [
                a for a in attributes
                if query_lower in a["name"].lower()
            ]
        
        return attributes[:size]
    
    async def get_attributes_by_type(
        self,
        org_id: str,
        attribute_type: str
    ) -> List[Dict[str, Any]]:
        """Get attributes by type."""
        filters = {"attributeType": attribute_type.upper()}
        schema = await self.get_catalog_schema(org_id, filters)
        return schema["attributes"]
    
    async def get_pii_attributes(self, org_id: str) -> List[Dict[str, Any]]:
        """Get PII attributes."""
        filters = {"isRawPII": True}
        schema = await self.get_catalog_schema(org_id, filters)
        return schema["attributes"]


class MockMetadataAPI:
    """Mock implementation of Metadata API."""
    
    def __init__(self):
        self.base_url = "mock://metadata"
        
    async def connect(self):
        """Mock connect."""
        pass
        
    async def disconnect(self):
        """Mock disconnect."""
        pass
        
    async def health_check(self) -> bool:
        """Mock health check."""
        return True
    
    async def get_column_metadata(
        self,
        org_id: str,
        columns: List[str],
        sample_size: int = 100
    ) -> List[Dict[str, Any]]:
        """Return mock column metadata."""
        metadata = []
        
        for column in columns:
            # Generate mock data based on column name
            if "id" in column.lower():
                values = [f"ID_{i:05d}" for i in range(1, min(sample_size + 1, 1001))]
                count = 100000
            elif "email" in column.lower():
                values = [f"user{i}@example.com" for i in range(1, min(sample_size + 1, 101))]
                count = 50000
            elif "age" in column.lower():
                values = list(range(18, min(18 + sample_size, 81)))
                count = len(values) * 1000
            elif "gender" in column.lower():
                values = ["M", "F", "Other"]
                count = 100000
            elif "country" in column.lower():
                values = ["USA", "UK", "Canada", "Germany", "France", "Japan", "Australia"]
                count = 100000
            elif "score" in column.lower() or "amount" in column.lower():
                values = [round(random.uniform(0, 100), 2) for _ in range(sample_size)]
                count = 100000
            elif "timestamp" in column.lower():
                values = [f"2024-01-{i:02d}T12:00:00Z" for i in range(1, min(sample_size + 1, 32))]
                count = 100000
            elif "consent" in column.lower():
                values = [True, False]
                count = 100000
            else:
                # Generic string values
                values = [f"{column}_value_{i}" for i in range(1, min(sample_size + 1, 21))]
                count = 50000
            
            metadata.append({
                "column": column,
                "values": values[:sample_size],
                "count": count,
                "nullCount": int(count * 0.05),  # 5% nulls
                "totalCount": count + int(count * 0.05)
            })
        
        return metadata
    
    async def get_column_statistics(
        self,
        org_id: str,
        columns: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """Get mock column statistics."""
        metadata = await self.get_column_metadata(org_id, columns)
        
        stats = {}
        for item in metadata:
            column_name = item["column"]
            stats[column_name] = {
                "count": item["count"],
                "unique_values": len(item["values"]),
                "sample_values": item["values"][:10],
                "null_count": item["nullCount"],
                "null_percentage": round((item["nullCount"] / item["totalCount"]) * 100, 2)
            }
        
        return stats
    
    async def analyze_cardinality(
        self,
        org_id: str,
        columns: List[str]
    ) -> Dict[str, str]:
        """Analyze mock cardinality."""
        metadata = await self.get_column_metadata(org_id, columns)
        
        cardinality = {}
        for item in metadata:
            column_name = item["column"]
            unique_count = len(item["values"])
            
            # Determine cardinality based on column type
            if "id" in column_name.lower():
                level = "VERY_HIGH"
            elif any(x in column_name.lower() for x in ["gender", "country", "type", "consent"]):
                level = "LOW"
            else:
                level = "HIGH"
            
            cardinality[column_name] = level
        
        return cardinality
    
    async def get_sample_data(
        self,
        org_id: str,
        columns: List[str],
        limit: int = 10
    ) -> Dict[str, List[Any]]:
        """Get mock sample data."""
        metadata = await self.get_column_metadata(org_id, columns, sample_size=limit)
        
        samples = {}
        for item in metadata:
            samples[item["column"]] = item["values"][:limit]
        
        return samples


class MockBigQueryClient:
    """Mock implementation of BigQuery client."""
    
    def __init__(self):
        self.client = None
        
    async def connect(self):
        """Mock connect."""
        pass
        
    async def disconnect(self):
        """Mock disconnect."""
        pass
        
    async def health_check(self) -> bool:
        """Mock health check."""
        return True
    
    async def analyze_table_statistics(
        self,
        dataset_id: str,
        table_id: str
    ) -> Dict[str, Any]:
        """Return mock table statistics."""
        return {
            "table_id": table_id,
            "dataset_id": dataset_id,
            "row_count": 1000000,
            "size_bytes": 1024 * 1024 * 100,  # 100MB
            "created": "2024-01-01T00:00:00",
            "modified": "2024-01-15T12:00:00",
            "schema": [
                {"name": "user_id", "type": "STRING", "mode": "REQUIRED", "description": "User identifier"},
                {"name": "age", "type": "INTEGER", "mode": "NULLABLE", "description": "User age"},
                {"name": "purchase_amount", "type": "FLOAT", "mode": "NULLABLE", "description": "Purchase amount"},
                {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "Record creation time"},
            ]
        }
    
    async def get_column_distribution(
        self,
        dataset_id: str,
        table_id: str,
        column_name: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        """Return mock column distribution."""
        # Generate mock distribution based on column name
        if "age" in column_name.lower():
            distribution = [
                {"value": age, "count": random.randint(5000, 20000), "percentage": 0}
                for age in range(18, 65)
            ]
        elif "gender" in column_name.lower():
            distribution = [
                {"value": "M", "count": 450000, "percentage": 45.0},
                {"value": "F", "count": 480000, "percentage": 48.0},
                {"value": "Other", "count": 70000, "percentage": 7.0},
            ]
        elif "country" in column_name.lower():
            countries = ["USA", "UK", "Canada", "Germany", "France"]
            distribution = [
                {"value": country, "count": random.randint(100000, 300000), "percentage": 0}
                for country in countries
            ]
        else:
            # Generic distribution
            distribution = [
                {"value": f"Value_{i}", "count": random.randint(1000, 50000), "percentage": 0}
                for i in range(min(20, limit))
            ]
        
        # Calculate percentages
        total = sum(d["count"] for d in distribution)
        for d in distribution:
            d["percentage"] = round(d["count"] * 100.0 / total, 2)
        
        return {
            "column": column_name,
            "distribution": distribution[:limit],
            "unique_values": len(distribution)
        }
    
    async def calculate_correlation_matrix(
        self,
        dataset_id: str,
        table_id: str,
        numeric_columns: List[str]
    ) -> Dict[str, Dict[str, float]]:
        """Return mock correlation matrix."""
        correlations = {}
        
        for col1 in numeric_columns:
            correlations[col1] = {}
            for col2 in numeric_columns:
                if col1 == col2:
                    correlations[col1][col2] = 1.0
                else:
                    # Generate realistic correlations
                    if ("age" in col1 and "income" in col2) or ("income" in col1 and "age" in col2):
                        correlation = round(random.uniform(0.3, 0.6), 4)
                    elif ("purchase" in col1 and "ltv" in col2) or ("ltv" in col1 and "purchase" in col2):
                        correlation = round(random.uniform(0.6, 0.8), 4)
                    else:
                        correlation = round(random.uniform(-0.2, 0.2), 4)
                    correlations[col1][col2] = correlation
        
        return correlations
    
    async def get_data_quality_metrics(
        self,
        dataset_id: str,
        table_id: str,
        columns: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """Return mock data quality metrics."""
        metrics = {}
        
        for column in columns:
            # Generate realistic quality metrics
            if "id" in column.lower():
                completeness = 100.0
                uniqueness = 99.9
                null_percentage = 0.0
            elif "email" in column.lower():
                completeness = 95.0
                uniqueness = 98.0
                null_percentage = 5.0
            else:
                completeness = round(random.uniform(85, 98), 2)
                uniqueness = round(random.uniform(10, 90), 2)
                null_percentage = round(100 - completeness, 2)
            
            metrics[column] = {
                "completeness": completeness,
                "uniqueness": uniqueness,
                "null_count": int(1000000 * null_percentage / 100),
                "null_percentage": null_percentage,
                "unique_count": int(1000000 * uniqueness / 100),
                "length_stats": {
                    "min": random.randint(3, 10),
                    "max": random.randint(20, 50),
                    "avg": round(random.uniform(10, 25), 2)
                }
            }
        
        return metrics
    
    async def run_custom_query(
        self,
        query: str,
        parameters: Optional[List[Any]] = None
    ) -> List[Dict[str, Any]]:
        """Return mock query results."""
        # Simple mock response
        return [
            {"result": "Mock query executed successfully", "rows_returned": 10},
            {"sample_data": {"col1": "value1", "col2": 123, "col3": 45.67}}
        ]