#!/bin/bash

# Run tests for MCP server

echo "Running MCP Server Tests..."
echo "=========================="

# Set test environment
export USE_MOCK_API=true
export BEARER_TOKEN=test_token
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Install test dependencies if needed
pip install pytest pytest-asyncio pytest-cov pytest-mock

# Run tests with coverage
echo ""
echo "Running unit tests..."
pytest tests/ -v --cov=src --cov-report=html --cov-report=term

# Run specific test suites
echo ""
echo "Test Summary:"
echo "-------------"
pytest tests/ -q --tb=no

echo ""
echo "Coverage report generated in htmlcov/"
echo ""

# Check if all tests passed
if [ $? -eq 0 ]; then
    echo "✅ All tests passed!"
else
    echo "❌ Some tests failed. Please check the output above."
    exit 1
fi