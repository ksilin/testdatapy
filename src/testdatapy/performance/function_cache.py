"""Optimized caching system for transformation function operations.

This module provides caching for function results to improve transformation
performance, especially for expensive operations like Faker data generation.
"""

import hashlib
import json
import time
from typing import Any, Dict, Optional, Tuple, Callable, Union
import threading
from functools import wraps

from ..logging_config import get_schema_logger


class FunctionCache:
    """High-performance cache for transformation function results."""
    
    def __init__(self, max_size: int = 10000, ttl: int = 300):
        """
        Initialize the function cache.
        
        Args:
            max_size: Maximum number of cached results
            ttl: Time-to-live for cache entries in seconds
        """
        self._logger = get_schema_logger(__name__)
        self._max_size = max_size
        self._ttl = ttl
        
        # Cache storage
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._access_times: Dict[str, float] = {}
        self._cache_lock = threading.RLock()
        
        # Statistics
        self._stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "errors": 0
        }
        
        self._logger.info("FunctionCache initialized", 
                         max_size=max_size, ttl=ttl)
    
    def _generate_cache_key(self, function_name: str, args: Tuple, kwargs: Dict[str, Any]) -> str:
        """
        Generate a cache key for function call.
        
        Args:
            function_name: Name of the function
            args: Function arguments
            kwargs: Function keyword arguments
            
        Returns:
            Unique cache key
        """
        # Create a hashable representation of arguments
        try:
            # Convert args and kwargs to a hashable format
            args_str = str(args)
            kwargs_str = json.dumps(kwargs, sort_keys=True, default=str)
            
            # Create hash
            key_data = f"{function_name}:{args_str}:{kwargs_str}"
            cache_key = hashlib.sha256(key_data.encode()).hexdigest()[:32]
            
            return cache_key
        
        except Exception as e:
            # Fallback for non-serializable arguments
            self._logger.debug("Cache key generation failed", 
                             function_name=function_name, error=str(e))
            return f"{function_name}_{hash((args, tuple(sorted(kwargs.items()))))}"
    
    def get(self, function_name: str, args: Tuple, kwargs: Dict[str, Any]) -> Tuple[bool, Any]:
        """
        Get cached function result.
        
        Args:
            function_name: Name of the function
            args: Function arguments
            kwargs: Function keyword arguments
            
        Returns:
            Tuple of (found, result)
        """
        cache_key = self._generate_cache_key(function_name, args, kwargs)
        
        with self._cache_lock:
            if cache_key in self._cache:
                entry = self._cache[cache_key]
                current_time = time.time()
                
                # Check if entry is still valid
                if current_time - entry['timestamp'] < self._ttl:
                    self._access_times[cache_key] = current_time
                    self._stats["hits"] += 1
                    self._logger.debug("Function cache hit", 
                                     function_name=function_name, 
                                     cache_key=cache_key[:16])
                    return True, entry['result']
                else:
                    # Expired, remove from cache
                    del self._cache[cache_key]
                    del self._access_times[cache_key]
            
            self._stats["misses"] += 1
            self._logger.debug("Function cache miss", 
                             function_name=function_name, 
                             cache_key=cache_key[:16])
            return False, None
    
    def put(self, function_name: str, args: Tuple, kwargs: Dict[str, Any], result: Any) -> None:
        """
        Store function result in cache.
        
        Args:
            function_name: Name of the function
            args: Function arguments
            kwargs: Function keyword arguments
            result: Function result to cache
        """
        cache_key = self._generate_cache_key(function_name, args, kwargs)
        
        with self._cache_lock:
            current_time = time.time()
            
            # Store the result
            self._cache[cache_key] = {
                'result': result,
                'timestamp': current_time,
                'function_name': function_name
            }
            self._access_times[cache_key] = current_time
            
            # Ensure cache size limit
            self._ensure_cache_size()
            
            self._logger.debug("Function result cached", 
                             function_name=function_name,
                             cache_key=cache_key[:16])
    
    def _ensure_cache_size(self) -> None:
        """Ensure cache doesn't exceed maximum size."""
        while len(self._cache) > self._max_size:
            # Evict least recently used item
            oldest_key = min(self._access_times.keys(), 
                            key=lambda k: self._access_times[k])
            
            function_name = self._cache[oldest_key].get('function_name', 'unknown')
            del self._cache[oldest_key]
            del self._access_times[oldest_key]
            self._stats["evictions"] += 1
            
            self._logger.debug("Evicted cache entry", 
                             function_name=function_name,
                             cache_key=oldest_key[:16])
    
    def clear(self) -> None:
        """Clear all cached data."""
        with self._cache_lock:
            self._cache.clear()
            self._access_times.clear()
            self._logger.info("Function cache cleared")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._cache_lock:
            total_requests = self._stats["hits"] + self._stats["misses"]
            hit_rate = self._stats["hits"] / total_requests if total_requests > 0 else 0
            
            # Function distribution
            function_counts = {}
            for entry in self._cache.values():
                func_name = entry.get('function_name', 'unknown')
                function_counts[func_name] = function_counts.get(func_name, 0) + 1
            
            return {
                **self._stats,
                "hit_rate": hit_rate,
                "cache_size": len(self._cache),
                "function_distribution": function_counts
            }
    
    def cleanup_expired(self) -> int:
        """Clean up expired cache entries."""
        removed_count = 0
        current_time = time.time()
        
        with self._cache_lock:
            expired_keys = [
                key for key, entry in self._cache.items()
                if current_time - entry['timestamp'] >= self._ttl
            ]
            
            for key in expired_keys:
                del self._cache[key]
                del self._access_times[key]
                removed_count += 1
        
        self._logger.info("Function cache cleanup completed", 
                         removed_entries=removed_count)
        return removed_count


def cacheable(ttl: int = 300, max_size: int = 1000):
    """
    Decorator to make functions cacheable.
    
    Args:
        ttl: Time-to-live for cache entries
        max_size: Maximum cache size for this function
        
    Returns:
        Decorated function with caching
    """
    def decorator(func: Callable) -> Callable:
        # Create a dedicated cache for this function
        cache = FunctionCache(max_size=max_size, ttl=ttl)
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Try to get from cache
            found, result = cache.get(func.__name__, args, kwargs)
            if found:
                return result
            
            # Execute function and cache result
            try:
                result = func(*args, **kwargs)
                cache.put(func.__name__, args, kwargs, result)
                return result
            except Exception as e:
                cache._stats["errors"] += 1
                raise
        
        # Add cache management methods to the wrapper
        wrapper._cache = cache
        wrapper.clear_cache = cache.clear
        wrapper.get_cache_stats = cache.get_statistics
        wrapper.cleanup_cache = cache.cleanup_expired
        
        return wrapper
    
    return decorator


# Global function cache instance
_global_function_cache: Optional[FunctionCache] = None


def get_function_cache() -> FunctionCache:
    """Get the global function cache instance."""
    global _global_function_cache
    if _global_function_cache is None:
        _global_function_cache = FunctionCache()
    return _global_function_cache