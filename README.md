
# Zeotap Feature Analysis MCP Server

A Model Context Protocol (MCP) server for analyzing Zeotap data features and assessing AI/ML readiness.

## Features

- **Schema Discovery**: Explore data schemas across different stores
- **Feature Analysis**: Assess AI/ML readiness for various use cases
- **Query Builder**: Generate optimized queries for ML datasets
- **Compliance Checker**: Verify data privacy compliance (GDPR, CCPA, etc.)

## Quick Start

### Prerequisites

- Python 3.10+
- Redis (optional, for caching)
- BigQuery access (optional, for statistical analysis)

### Installation

1. Clone the repository and navigate to the MCP server directory:
```bash
cd /Users/joydeep_1/PycharmProjects/ai_readiness/mcp-server
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment:
```bash
cp .env.example .env
# Edit .env with your credentials
```

### Running the Server

#### Development Mode (with mock data):
```bash
USE_MOCK_API=true python main.py
```

#### Production Mode:
```bash
python main.py
```

## Tools

### 1. Schema Discovery

Discover and explore data schemas:

```python
# Get schema overview
await schema_discovery(
    org_id="org123",
    operation="overview"
)

# Get specific store schema
await schema_discovery(
    org_id="org123",
    operation="store",
    store_type="profile_store"
)

# Search for columns
await schema_discovery(
    org_id="org123",
    operation="search",
    search_query="user"
)
```

### 2. Feature Analysis

Analyze features for ML readiness:

```python
# Collaborative filtering analysis
await feature_analysis(
    org_id="org123",
    use_case="collaborative_filtering",
    dataset_id="my_dataset",
    table_id="user_interactions"
)

# Churn prediction analysis
await feature_analysis(
    org_id="org123",
    use_case="churn_prediction",
    include_correlations=True
)
```

### 3. Query Builder

Build ML-ready queries:

```python
# Feature extraction query
await query_builder(
    org_id="org123",
    query_type="feature_extraction",
    use_case="collaborative_filtering",
    dataset_id="my_dataset",
    table_id="events",
    limit=10000
)

# Aggregation query
await query_builder(
    org_id="org123",
    query_type="aggregation",
    use_case="churn_prediction",
    execute=True
)
```

### 4. Compliance Checker

Check data compliance:

```python
# Full compliance check
await compliance_checker(
    org_id="org123",
    check_type="full",
    regulations=["GDPR", "CCPA"]
)

# PII compliance only
await compliance_checker(
    org_id="org123",
    check_type="pii",
    generate_report=True
)
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `BEARER_TOKEN` | Zeotap API authentication token | Required |
| `USE_MOCK_API` | Use mock data for development | `false` |
| `BIGQUERY_PROJECT` | GCP project for BigQuery | `zeotap-dev-datascience` |
| `BIGQUERY_LOCATION` | BigQuery dataset location | `europe-west1` |
| `REDIS_HOST` | Redis server host | `localhost` |
| `REDIS_PORT` | Redis server port | `6379` |

### Cache Configuration

The server uses a multi-level caching strategy:
- **Memory Cache**: Fast L1 cache for frequently accessed data
- **Redis Cache**: Distributed L2 cache for sharing across instances
- **TTL Settings**: Configurable cache expiration times

## Architecture

```
mcp-server/
├── src/
│   ├── config.py           # Configuration and settings
│   ├── server.py           # Main server implementation
│   ├── core/               # Core functionality
│   │   ├── api_client.py   # Base API client with retry logic
│   │   ├── cache_manager.py # Multi-level cache management
│   │   ├── exceptions.py   # Custom exceptions
│   │   └── schema_manager.py # Schema operations
│   ├── integrations/       # External API integrations
│   │   ├── catalog_api.py  # Zeotap Catalog API
│   │   ├── metadata_api.py # Zeotap Metadata API
│   │   ├── bigquery_client.py # Google BigQuery
│   │   └── mock_api.py     # Mock implementations
│   └── tools/              # MCP tool implementations
│       ├── discovery.py    # Schema discovery tool
│       ├── analysis.py     # Feature analysis tool
│       ├── query.py        # Query builder tool
│       └── compliance.py   # Compliance checker tool
├── tests/                  # Test suite
├── main.py                 # Entry point
└── requirements.txt        # Dependencies
```

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src tests/

# Run specific test file
pytest tests/test_schema_discovery.py
```

### Adding New Tools

1. Create a new tool in `src/tools/`
2. Implement the `Tool` interface
3. Register in `src/server.py`
4. Add tests in `tests/`

### Mock Mode

Use `USE_MOCK_API=true` for development without external dependencies. Mock implementations provide realistic test data for all features.

## Monitoring

### Health Check

```python
await health_check()
# Returns server health status and component states
```

### Logging

Structured logging with multiple levels:
- `DEBUG`: Detailed debugging information
- `INFO`: General information
- `WARNING`: Warning messages
- `ERROR`: Error messages

## Security

- Bearer token authentication for Zeotap APIs
- PII detection and compliance checking
- Secure credential management via environment variables
- Optional BigQuery service account authentication

## Troubleshooting

### Common Issues

1. **Redis Connection Failed**: Server works with memory cache only
2. **BigQuery Access Denied**: Check project permissions and credentials
3. **API Rate Limits**: Implement appropriate retry logic and caching

### Debug Mode

Enable debug logging:
```bash
LOG_LEVEL=DEBUG python main.py
```

## License

Proprietary - Zeotap Internal Use Only
