#!/usr/bin/env python3
"""HTTP debug server for testing MCP tools via REST API."""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import uvicorn
import os

# Set environment
os.environ["USE_MOCK_API"] = "true"

from src.server import (
    schema_discovery,
    feature_analysis,
    query_builder,
    compliance_checker
)

app = FastAPI(title="MCP Debug Server")


class SchemaDiscoveryRequest(BaseModel):
    org_id: str
    operation: str = "overview"
    store_type: Optional[str] = None
    columns: Optional[List[str]] = None
    search_query: Optional[str] = None
    force_refresh: bool = False


class FeatureAnalysisRequest(BaseModel):
    org_id: str
    use_case: str = "collaborative_filtering"
    columns: Optional[List[str]] = None
    dataset_id: Optional[str] = None
    table_id: Optional[str] = None
    include_statistics: bool = True
    include_quality: bool = True
    include_correlations: bool = False


@app.post("/schema_discovery")
async def api_schema_discovery(request: SchemaDiscoveryRequest):
    """Test schema discovery endpoint."""
    try:
        result = await schema_discovery(
            org_id=request.org_id,
            operation=request.operation,
            store_type=request.store_type,
            columns=request.columns,
            search_query=request.search_query,
            force_refresh=request.force_refresh
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/feature_analysis")
async def api_feature_analysis(request: FeatureAnalysisRequest):
    """Test feature analysis endpoint."""
    try:
        result = await feature_analysis(
            org_id=request.org_id,
            use_case=request.use_case,
            columns=request.columns,
            dataset_id=request.dataset_id,
            table_id=request.table_id,
            include_statistics=request.include_statistics,
            include_quality=request.include_quality,
            include_correlations=request.include_correlations
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok", "debug_mode": True}


if __name__ == "__main__":
    # Run with: python debug_server.py
    # Then use curl or Postman to test:
    # curl -X POST http://localhost:8000/schema_discovery \
    #   -H "Content-Type: application/json" \
    #   -d '{"org_id": "test_org", "operation": "overview"}'
    
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)