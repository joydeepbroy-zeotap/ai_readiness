#!/usr/bin/env python3
"""Main entry point for the MCP server."""

import sys
import asyncio
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.server import create_app

if __name__ == "__main__":
    app = create_app()
    app.run(transport='stdio')
