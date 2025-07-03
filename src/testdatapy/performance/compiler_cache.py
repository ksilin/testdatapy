"""Optimized caching system for protobuf compilation operations.

This module provides intelligent caching to avoid repeated compilation
of identical proto files and improve overall performance.
"""

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
import threading

from ..logging_config import get_schema_logger


class CompilerCache:
    """High-performance cache for protobuf compilation results."""
    
    def __init__(self, cache_dir: Optional[str] = None, max_cache_size: int = 1000, ttl: int = 3600):
        """
        Initialize the compiler cache.
        
        Args:
            cache_dir: Directory for persistent cache storage
            max_cache_size: Maximum number of cached items
            ttl: Time-to-live for cache entries in seconds
        """
        self._logger = get_schema_logger(__name__)
        self._cache_dir = Path(cache_dir or Path.home() / ".testdatapy" / "cache" / "compiler")
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        
        self._max_cache_size = max_cache_size
        self._ttl = ttl
        
        # In-memory cache for hot data
        self._memory_cache: Dict[str, Dict[str, Any]] = {}
        self._access_times: Dict[str, float] = {}
        self._cache_lock = threading.RLock()
        
        # Statistics
        self._stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "disk_reads": 0,
            "disk_writes": 0
        }
        
        self._logger.info("CompilerCache initialized", 
                         cache_dir=str(self._cache_dir),
                         max_size=max_cache_size,
                         ttl=ttl)
    
    def get_cache_key(self, proto_file: str, include_paths: Optional[List[str]] = None) -> str:
        """
        Generate a cache key for a proto file and its dependencies.
        
        Args:
            proto_file: Path to proto file
            include_paths: Optional include paths
            
        Returns:
            Unique cache key
        """
        proto_path = Path(proto_file)
        
        # Get file content hash
        with open(proto_path, 'rb') as f:
            content_hash = hashlib.sha256(f.read()).hexdigest()
        
        # Include file modification time
        mtime = proto_path.stat().st_mtime
        
        # Include include paths in key
        include_key = ""
        if include_paths:
            include_key = hashlib.sha256(
                json.dumps(sorted(include_paths), sort_keys=True).encode()
            ).hexdigest()[:16]
        
        cache_key = f"{proto_path.name}_{content_hash[:16]}_{int(mtime)}_{include_key}"
        return cache_key
    
    def get(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """
        Get cached compilation result.
        
        Args:
            cache_key: Cache key
            
        Returns:
            Cached result or None if not found/expired
        """
        with self._cache_lock:
            # Check memory cache first
            if cache_key in self._memory_cache:
                entry = self._memory_cache[cache_key]
                if time.time() - entry['timestamp'] < self._ttl:
                    self._access_times[cache_key] = time.time()
                    self._stats["hits"] += 1
                    self._logger.debug("Cache hit (memory)", cache_key=cache_key)
                    return entry['data']
                else:
                    # Expired, remove from memory
                    del self._memory_cache[cache_key]
                    del self._access_times[cache_key]
            
            # Check disk cache
            cache_file = self._cache_dir / f"{cache_key}.json"
            if cache_file.exists():
                try:
                    with open(cache_file, 'r') as f:
                        entry = json.load(f)
                    
                    if time.time() - entry['timestamp'] < self._ttl:
                        # Load into memory cache
                        self._memory_cache[cache_key] = entry
                        self._access_times[cache_key] = time.time()
                        self._ensure_memory_cache_size()
                        
                        self._stats["hits"] += 1
                        self._stats["disk_reads"] += 1
                        self._logger.debug("Cache hit (disk)", cache_key=cache_key)
                        return entry['data']
                    else:
                        # Expired, remove file
                        cache_file.unlink()
                
                except Exception as e:
                    self._logger.warning("Error reading cache file", 
                                       cache_key=cache_key, error=str(e))
            
            self._stats["misses"] += 1
            self._logger.debug("Cache miss", cache_key=cache_key)
            return None
    
    def put(self, cache_key: str, data: Dict[str, Any]) -> None:
        """
        Store compilation result in cache.
        
        Args:
            cache_key: Cache key
            data: Compilation result data
        """
        with self._cache_lock:
            timestamp = time.time()
            entry = {
                'data': data,
                'timestamp': timestamp
            }
            
            # Store in memory cache
            self._memory_cache[cache_key] = entry
            self._access_times[cache_key] = timestamp
            self._ensure_memory_cache_size()
            
            # Store on disk asynchronously
            self._store_to_disk_async(cache_key, entry)
    
    def _store_to_disk_async(self, cache_key: str, entry: Dict[str, Any]) -> None:
        """Store cache entry to disk asynchronously."""
        import threading
        
        def store():
            try:
                cache_file = self._cache_dir / f"{cache_key}.json"
                with open(cache_file, 'w') as f:
                    json.dump(entry, f)
                self._stats["disk_writes"] += 1
                self._logger.debug("Cache stored to disk", cache_key=cache_key)
            except Exception as e:
                self._logger.warning("Error storing cache to disk", 
                                   cache_key=cache_key, error=str(e))
        
        thread = threading.Thread(target=store, daemon=True)
        thread.start()
    
    def _ensure_memory_cache_size(self) -> None:
        """Ensure memory cache doesn't exceed maximum size."""
        while len(self._memory_cache) > self._max_cache_size:
            # Evict least recently used item
            oldest_key = min(self._access_times.keys(), 
                            key=lambda k: self._access_times[k])
            
            del self._memory_cache[oldest_key]
            del self._access_times[oldest_key]
            self._stats["evictions"] += 1
            
            self._logger.debug("Evicted cache entry", cache_key=oldest_key)
    
    def clear(self) -> None:
        """Clear all cache data."""
        with self._cache_lock:
            self._memory_cache.clear()
            self._access_times.clear()
            
            # Clear disk cache
            for cache_file in self._cache_dir.glob("*.json"):
                try:
                    cache_file.unlink()
                except Exception as e:
                    self._logger.warning("Error deleting cache file", 
                                       file=str(cache_file), error=str(e))
            
            self._logger.info("Cache cleared")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._cache_lock:
            total_requests = self._stats["hits"] + self._stats["misses"]
            hit_rate = self._stats["hits"] / total_requests if total_requests > 0 else 0
            
            return {
                **self._stats,
                "hit_rate": hit_rate,
                "memory_cache_size": len(self._memory_cache),
                "disk_cache_files": len(list(self._cache_dir.glob("*.json")))
            }
    
    def cleanup_expired(self) -> int:
        """Clean up expired cache entries."""
        removed_count = 0
        current_time = time.time()
        
        with self._cache_lock:
            # Clean memory cache
            expired_keys = [
                key for key, entry in self._memory_cache.items()
                if current_time - entry['timestamp'] >= self._ttl
            ]
            
            for key in expired_keys:
                del self._memory_cache[key]
                del self._access_times[key]
                removed_count += 1
            
            # Clean disk cache
            for cache_file in self._cache_dir.glob("*.json"):
                try:
                    with open(cache_file, 'r') as f:
                        entry = json.load(f)
                    
                    if current_time - entry['timestamp'] >= self._ttl:
                        cache_file.unlink()
                        removed_count += 1
                
                except Exception:
                    # Remove corrupted files
                    cache_file.unlink()
                    removed_count += 1
        
        self._logger.info("Cleanup completed", removed_entries=removed_count)
        return removed_count


# Global cache instance
_global_compiler_cache: Optional[CompilerCache] = None


def get_compiler_cache() -> CompilerCache:
    """Get the global compiler cache instance."""
    global _global_compiler_cache
    if _global_compiler_cache is None:
        _global_compiler_cache = CompilerCache()
    return _global_compiler_cache