"""Configuration management for MCP server."""

from typing import Optional
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Settings(BaseSettings):
    """Application settings."""

    # API Configuration
    catalog_api_url: str = Field(
        default="https://unity.zeotap.com/canon/api/v2/orgs/{org_id}/catalog/_search",
        alias="CATALOG_API_URL"
    )
    metadata_api_url: str = Field(
        default="https://unity.zeotap.com/datamanager/api/v2/catalog/column/metadata",
        alias="METADATA_API_URL"
    )
    bearer_token: Optional[str] = Field(default="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE2NjAxMTM0MTIsImV4cCI6MTY3NTY2NTQxMCwidCI6IjU1NDM4Y2I4ZWZlZjU5MzBjN2FkNGU1MGZhMTAwNjhhIn0.36M7_Ruq48AMay9acmOT95cl9Wx4_FNLzQxjwLBe7b8", alias="BEARER_TOKEN")
    
    # BigQuery Configuration
    bigquery_project: str = Field(default="zeotap-dev-datascience", alias="BIGQUERY_PROJECT")
    bigquery_location: str = Field(default="europe-west1", alias="BIGQUERY_LOCATION")
    bigquery_dataset: str = Field(default="schema_statistics", alias="BIGQUERY_DATASET")
    
    # Redis Configuration
    redis_host: str = Field(default="localhost", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")
    redis_db: int = Field(default=0, alias="REDIS_DB")
    redis_password: Optional[str] = Field(default=None, alias="REDIS_PASSWORD")
    
    # Cache Configuration
    cache_ttl_short: int = Field(default=300, alias="CACHE_TTL_SHORT")  # 5 minutes
    cache_ttl_medium: int = Field(default=3600, alias="CACHE_TTL_MEDIUM")  # 1 hour
    cache_ttl_long: int = Field(default=7200, alias="CACHE_TTL_LONG")  # 2 hours
    
    # API Configuration
    api_timeout: int = Field(default=30, alias="API_TIMEOUT")
    api_retry_count: int = Field(default=3, alias="API_RETRY_COUNT")
    api_retry_delay: int = Field(default=1, alias="API_RETRY_DELAY")
    
    # Feature Flags
    use_mock_api: bool = Field(default=False, alias="USE_MOCK_API")
    enable_monitoring: bool = Field(default=True, alias="ENABLE_MONITORING")
    
    # Logging
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        env_file_encoding='utf-8',
        populate_by_name=True
    )

# Global settings instance
settings = Settings()

# Store categorization rules
STORE_MAPPING = {
    "profile_store": {
        "attribute_types": ["USER", "PROFILE"],
        "keywords": ["age", "gender", "location", "preference", "demographic"]
    },
    "event_store": {
        "attribute_types": ["EVENT", "INTERACTION"],
        "keywords": ["event", "action", "click", "view", "purchase", "timestamp"]
    },
    "calculated_attribute": {
        "attribute_types": ["CALCULATED", "DERIVED", "AGGREGATE"],
        "keywords": ["score", "sum", "avg", "count", "last_", "total_", "CLV"]
    },
    "consent_store": {
        "attribute_types": ["CONSENT", "PRIVACY"],
        "keywords": ["consent", "opt", "gdpr", "privacy", "permission"]
    }
}

# PII detection patterns
PII_PATTERNS = {
    "high": ["email", "phone", "ssn", "credit_card", "password", "ip_address"],
    "medium": ["name", "address", "device_id", "cookie", "user_agent"],
    "low": ["country", "city", "state", "zip", "age", "gender"]
}

# Low cardinality threshold
LOW_CARDINALITY_THRESHOLD = 100
