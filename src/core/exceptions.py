"""Custom exceptions for MCP server."""

from typing import Optional, Dict, Any


class MCPServerError(Exception):
    """Base exception for MCP server."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class APIError(MCPServerError):
    """API related errors."""
    
    def __init__(
        self, 
        message: str, 
        status_code: Optional[int] = None,
        endpoint: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message, details)
        self.status_code = status_code
        self.endpoint = endpoint


class CacheError(MCPServerError):
    """Cache related errors."""
    pass


class ValidationError(MCPServerError):
    """Validation errors."""
    
    def __init__(self, message: str, field: Optional[str] = None, value: Any = None):
        details = {}
        if field:
            details["field"] = field
        if value is not None:
            details["value"] = value
        super().__init__(message, details)


class AuthenticationError(MCPServerError):
    """Authentication errors."""
    pass


class PermissionError(MCPServerError):
    """Permission/Authorization errors."""
    
    def __init__(self, message: str, required_permission: Optional[str] = None):
        details = {}
        if required_permission:
            details["required_permission"] = required_permission
        super().__init__(message, details)