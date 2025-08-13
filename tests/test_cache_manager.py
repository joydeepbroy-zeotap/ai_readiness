"""Tests for cache manager."""

import pytest
import json
from src.core.cache_manager import CacheManager


class TestCacheManager:
    """Test cache manager functionality."""
    
    @pytest.mark.asyncio
    async def test_memory_cache_operations(self):
        """Test basic memory cache operations."""
        cache = CacheManager()
        
        # Test set and get
        key = "test_key"
        value = {"data": "test_value", "number": 42}
        
        assert await cache.set(key, value) is True
        retrieved = await cache.get(key)
        assert retrieved == value
        
        # Test cache miss
        assert await cache.get("nonexistent_key") is None
        
        # Test delete
        assert await cache.delete(key) is True
        assert await cache.get(key) is None
    
    @pytest.mark.asyncio
    async def test_cache_ttl(self):
        """Test cache with TTL."""
        cache = CacheManager()
        
        key = "ttl_key"
        value = "ttl_value"
        ttl = 1  # 1 second
        
        await cache.set(key, value, ttl=ttl)
        assert await cache.get(key) == value
        
        # Note: In real tests, we'd wait for TTL to expire
        # For now, just verify the set operation worked
    
    @pytest.mark.asyncio
    async def test_cache_json_serialization(self):
        """Test JSON serialization in cache."""
        cache = CacheManager()
        
        # Test various data types
        test_data = {
            "string": "test",
            "number": 42,
            "float": 3.14,
            "bool": True,
            "null": None,
            "list": [1, 2, 3],
            "dict": {"nested": "value"}
        }
        
        key = "json_test"
        await cache.set(key, test_data)
        retrieved = await cache.get(key)
        
        assert retrieved == test_data
        assert isinstance(retrieved["list"], list)
        assert isinstance(retrieved["dict"], dict)
    
    @pytest.mark.asyncio
    async def test_cache_pattern_clear(self):
        """Test clearing cache by pattern."""
        cache = CacheManager()
        
        # Set multiple keys
        await cache.set("user:1", {"id": 1})
        await cache.set("user:2", {"id": 2})
        await cache.set("product:1", {"id": 1})
        
        # Clear user keys
        count = await cache.clear_pattern("user:")
        assert count == 2
        
        # Verify user keys are gone but product key remains
        assert await cache.get("user:1") is None
        assert await cache.get("user:2") is None
        assert await cache.get("product:1") == {"id": 1}
    
    @pytest.mark.asyncio
    async def test_cache_stats(self):
        """Test cache statistics."""
        cache = CacheManager()
        
        # Initial stats
        stats = cache.get_cache_stats()
        assert stats["memory_cache_size"] == 0
        
        # Add some items
        await cache.set("key1", "value1")
        await cache.set("key2", "value2")
        
        stats = cache.get_cache_stats()
        assert stats["memory_cache_size"] == 2
        
        # Clear memory cache
        cache.clear_memory_cache()
        stats = cache.get_cache_stats()
        assert stats["memory_cache_size"] == 0
    
    @pytest.mark.asyncio
    async def test_cache_error_handling(self):
        """Test cache error handling."""
        cache = CacheManager()
        
        # Test setting non-serializable object
        class NonSerializable:
            def __init__(self):
                self.func = lambda x: x
        
        with pytest.raises(Exception):  # Should raise serialization error
            await cache.set("bad_key", NonSerializable())
    
    @pytest.mark.asyncio
    async def test_redis_fallback(self, cache_manager):
        """Test fallback to memory cache when Redis is unavailable."""
        # cache_manager fixture handles connection
        
        # Should work even without Redis
        key = "fallback_test"
        value = {"test": "data"}
        
        assert await cache_manager.set(key, value) is True
        assert await cache_manager.get(key) == value