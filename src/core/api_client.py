"""Abstract API client with retry logic and error handling."""

import asyncio
from typing import Dict, Any, Optional, Union
from abc import ABC, abstractmethod
import aiohttp
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)
import structlog

from ..config import settings
from .exceptions import APIError, AuthenticationError

logger = structlog.get_logger()


class APIClient(ABC):
    """Abstract base class for API clients."""
    
    def __init__(self, base_url: str, use_mock: bool = False):
        self.base_url = base_url
        self.use_mock = use_mock or settings.use_mock_api
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
        
    async def connect(self):
        """Create aiohttp session."""
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=settings.api_timeout)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers=self._get_default_headers()
            )
            
    async def disconnect(self):
        """Close aiohttp session."""
        if self.session:
            await self.session.close()
            self.session = None
            
    def _get_default_headers(self) -> Dict[str, str]:
        """Get default headers for requests."""
        return {
            "Authorization": f"Bearer {settings.bearer_token}",
            "Content-Type": "application/json"
        }
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def _make_request(
        self,
        method: str,
        url: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Make HTTP request with retry logic."""
        if not self.session:
            await self.connect()
            
        # Merge headers
        request_headers = self._get_default_headers()
        if headers:
            request_headers.update(headers)
            
        logger.debug(f"Making {method} request to {url}")
        
        try:
            async with self.session.request(
                method=method,
                url=url,
                json=json_data,
                params=params,
                headers=request_headers
            ) as response:
                # Check for auth errors
                if response.status == 401:
                    raise AuthenticationError("Invalid or expired token")
                elif response.status == 403:
                    raise AuthenticationError("Insufficient permissions")
                
                # Check for other HTTP errors
                if response.status >= 400:
                    error_text = await response.text()
                    raise APIError(
                        f"API request failed: {error_text}",
                        status_code=response.status,
                        endpoint=url
                    )
                
                # Parse JSON response
                data = await response.json()
                logger.debug(f"Request successful: {method} {url}")
                return data
                
        except aiohttp.ClientError as e:
            logger.error(f"Request failed: {method} {url} - {str(e)}")
            raise APIError(
                f"Request failed: {str(e)}",
                endpoint=url
            )
        except asyncio.TimeoutError:
            logger.error(f"Request timeout: {method} {url}")
            raise APIError(
                f"Request timeout after {settings.api_timeout}s",
                endpoint=url
            )
    
    @abstractmethod
    async def health_check(self) -> bool:
        """Check if API is reachable."""
        pass