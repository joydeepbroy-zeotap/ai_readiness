"""Cache management with Redis."""

import json
import asyncio
from typing import Optional, Dict, Any, Union
from datetime import timedelta
import redis.asyncio as redis
import structlog

from ..config import settings
from .exceptions import CacheError

logger = structlog.get_logger()


class CacheManager:
    """Multi-level cache manager with Redis backend."""
    
    def __init__(self):
        self._redis_client: Optional[redis.Redis] = None
        self._memory_cache: Dict[str, Any] = {}
        self._connected = False
        
    async def connect(self):
        """Connect to Redis."""
        try:
            self._redis_client = redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                password=settings.redis_password,
                decode_responses=True
            )
            # Test connection
            await self._redis_client.ping()
            self._connected = True
            logger.info("Redis connection established")
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}. Using memory cache only.")
            self._connected = False
    
    async def disconnect(self):
        """Disconnect from Redis."""
        if self._redis_client:
            await self._redis_client.close()
            self._connected = False
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        # Try memory cache first (L1)
        if key in self._memory_cache:
            logger.debug(f"Cache hit (memory): {key}")
            return self._memory_cache[key]
        
        # Try Redis (L2)
        if self._connected and self._redis_client:
            try:
                value = await self._redis_client.get(key)
                if value:
                    logger.debug(f"Cache hit (Redis): {key}")
                    # Deserialize JSON
                    deserialized = json.loads(value)
                    # Store in memory cache for faster access
                    self._memory_cache[key] = deserialized
                    return deserialized
            except Exception as e:
                logger.error(f"Redis get error: {e}")
        
        logger.debug(f"Cache miss: {key}")
        return None
    
    async def set(
        self, 
        key: str, 
        value: Any, 
        ttl: Optional[int] = None
    ) -> bool:
        """Set value in cache with optional TTL in seconds."""
        try:
            # Serialize to JSON
            serialized = json.dumps(value)
            
            # Store in memory cache (L1)
            self._memory_cache[key] = value
            
            # Store in Redis (L2) if connected
            if self._connected and self._redis_client:
                try:
                    if ttl:
                        await self._redis_client.setex(key, ttl, serialized)
                    else:
                        await self._redis_client.set(key, serialized)
                    logger.debug(f"Cache set: {key} (ttl={ttl})")
                except Exception as e:
                    logger.error(f"Redis set error: {e}")
                    return False
            
            return True
            
        except Exception as e:
            raise CacheError(f"Failed to cache value: {e}")
    
    async def delete(self, key: str) -> bool:
        """Delete key from cache."""
        # Remove from memory cache
        self._memory_cache.pop(key, None)
        
        # Remove from Redis if connected
        if self._connected and self._redis_client:
            try:
                await self._redis_client.delete(key)
                logger.debug(f"Cache delete: {key}")
                return True
            except Exception as e:
                logger.error(f"Redis delete error: {e}")
                return False
        
        return True
    
    async def clear_pattern(self, pattern: str) -> int:
        """Clear all keys matching pattern."""
        count = 0
        
        # Clear from memory cache
        keys_to_remove = [k for k in self._memory_cache if pattern in k]
        for key in keys_to_remove:
            self._memory_cache.pop(key, None)
            count += 1
        
        # Clear from Redis if connected
        if self._connected and self._redis_client:
            try:
                cursor = 0
                while True:
                    cursor, keys = await self._redis_client.scan(
                        cursor, 
                        match=f"*{pattern}*", 
                        count=100
                    )
                    if keys:
                        await self._redis_client.delete(*keys)
                        count += len(keys)
                    if cursor == 0:
                        break
            except Exception as e:
                logger.error(f"Redis pattern delete error: {e}")
        
        logger.info(f"Cleared {count} keys matching pattern: {pattern}")
        return count
    
    def clear_memory_cache(self):
        """Clear memory cache only."""
        self._memory_cache.clear()
        logger.debug("Memory cache cleared")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            "memory_cache_size": len(self._memory_cache),
            "redis_connected": self._connected
        }


# Global cache instance
cache_manager = CacheManager()