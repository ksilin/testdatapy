"""Schema cache management system.

This module provides comprehensive caching capabilities for compiled schemas,
including local file-based caching with metadata tracking and TTL management.
"""

import hashlib
import json
import time
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta

from ..logging_config import get_schema_logger
from ..performance.performance_monitor import get_performance_monitor, monitored


@dataclass
class CacheEntry:
    """Represents a cached schema entry."""
    schema_path: str
    compiled_path: str
    cache_key: str
    created_at: float
    accessed_at: float
    access_count: int
    file_hash: str
    file_size: int
    compilation_duration: float
    schema_type: str
    dependencies: List[str]
    metadata: Dict[str, Any]
    
    def is_expired(self, ttl_seconds: int) -> bool:
        """Check if cache entry is expired."""
        return time.time() - self.created_at > ttl_seconds
    
    def is_stale(self, current_file_hash: str) -> bool:
        """Check if cache entry is stale based on file hash."""
        return self.file_hash != current_file_hash
    
    def update_access(self):
        """Update access tracking."""
        self.accessed_at = time.time()
        self.access_count += 1
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CacheEntry':
        """Create from dictionary."""
        return cls(**data)


class SchemaCacheManager:
    """Manages schema caching with file-based persistence."""
    
    def __init__(self, cache_dir: Optional[Path] = None, max_cache_size: int = 1000,
                 default_ttl: int = 3600, cleanup_interval: int = 300):
        """
        Initialize schema cache manager.
        
        Args:
            cache_dir: Directory for cache storage
            max_cache_size: Maximum number of cached entries
            default_ttl: Default TTL in seconds
            cleanup_interval: Cleanup interval in seconds
        """
        self._logger = get_schema_logger(__name__)
        self._monitor = get_performance_monitor()
        
        # Set up cache directory
        if cache_dir is None:
            cache_dir = Path.home() / ".testdatapy" / "schema_cache"
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Cache settings
        self._max_cache_size = max_cache_size
        self._default_ttl = default_ttl
        self._cleanup_interval = cleanup_interval
        
        # Cache storage
        self._entries: Dict[str, CacheEntry] = {}
        self._cache_index_file = self._cache_dir / "cache_index.json"
        
        # Statistics
        self._stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "cleanups": 0,
            "total_size": 0
        }
        
        # Load existing cache
        self._load_cache_index()
        self._last_cleanup = time.time()
        
        self._logger.info("SchemaCacheManager initialized",
                         cache_dir=str(self._cache_dir),
                         max_size=max_cache_size,
                         default_ttl=default_ttl)
    
    @monitored("cache_get")
    def get(self, schema_path: str, schema_type: str = "protobuf") -> Optional[Tuple[str, CacheEntry]]:
        """
        Get cached compiled schema.
        
        Args:
            schema_path: Path to source schema file
            schema_type: Type of schema (protobuf, avro, etc.)
            
        Returns:
            Tuple of (compiled_path, cache_entry) if found, None otherwise
        """
        # Generate cache key
        cache_key = self._generate_cache_key(schema_path, schema_type)
        
        # Check if entry exists
        if cache_key not in self._entries:
            self._stats["misses"] += 1
            return None
        
        entry = self._entries[cache_key]
        
        # Check if entry is expired
        if entry.is_expired(self._default_ttl):
            self._logger.debug("Cache entry expired", cache_key=cache_key)
            self._remove_entry(cache_key)
            self._stats["misses"] += 1
            return None
        
        # Check if source file has changed
        try:
            current_hash = self._calculate_file_hash(schema_path)
            if entry.is_stale(current_hash):
                self._logger.debug("Cache entry stale", cache_key=cache_key)
                self._remove_entry(cache_key)
                self._stats["misses"] += 1
                return None
        except FileNotFoundError:
            self._logger.debug("Source file not found", schema_path=schema_path)
            self._remove_entry(cache_key)
            self._stats["misses"] += 1
            return None
        
        # Check if compiled file still exists
        compiled_path = Path(entry.compiled_path)
        if not compiled_path.exists():
            self._logger.debug("Compiled file missing", compiled_path=str(compiled_path))
            self._remove_entry(cache_key)
            self._stats["misses"] += 1
            return None
        
        # Cache hit
        entry.update_access()
        self._stats["hits"] += 1
        
        self._logger.debug("Cache hit", cache_key=cache_key, schema_path=schema_path)
        return str(compiled_path), entry
    
    @monitored("cache_put")
    def put(self, schema_path: str, compiled_path: str, compilation_duration: float,
            schema_type: str = "protobuf", dependencies: Optional[List[str]] = None,
            metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Store compiled schema in cache.
        
        Args:
            schema_path: Path to source schema file
            compiled_path: Path to compiled schema
            compilation_duration: Time taken to compile
            schema_type: Type of schema
            dependencies: List of dependency files
            metadata: Additional metadata
            
        Returns:
            Cache key for the stored entry
        """
        # Generate cache key
        cache_key = self._generate_cache_key(schema_path, schema_type)
        
        # Calculate file hash and size
        file_hash = self._calculate_file_hash(schema_path)
        file_size = Path(schema_path).stat().st_size
        
        # Create cache directory for this entry
        cache_entry_dir = self._cache_dir / "entries" / cache_key
        cache_entry_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy compiled file to cache
        cached_compiled_path = cache_entry_dir / Path(compiled_path).name
        shutil.copy2(compiled_path, cached_compiled_path)
        
        # Create cache entry
        entry = CacheEntry(
            schema_path=schema_path,
            compiled_path=str(cached_compiled_path),
            cache_key=cache_key,
            created_at=time.time(),
            accessed_at=time.time(),
            access_count=1,
            file_hash=file_hash,
            file_size=file_size,
            compilation_duration=compilation_duration,
            schema_type=schema_type,
            dependencies=dependencies or [],
            metadata=metadata or {}
        )
        
        # Store entry
        self._entries[cache_key] = entry
        self._stats["total_size"] += file_size
        
        # Enforce cache size limit
        self._enforce_cache_limit()
        
        # Periodic cleanup
        if time.time() - self._last_cleanup > self._cleanup_interval:
            self._cleanup_expired_entries()
        
        # Save cache index
        self._save_cache_index()
        
        self._logger.debug("Cache entry stored", cache_key=cache_key, schema_path=schema_path)
        return cache_key
    
    @monitored("cache_remove")
    def remove(self, schema_path: str, schema_type: str = "protobuf") -> bool:
        """
        Remove cached entry.
        
        Args:
            schema_path: Path to source schema file
            schema_type: Type of schema
            
        Returns:
            True if entry was removed, False if not found
        """
        cache_key = self._generate_cache_key(schema_path, schema_type)
        return self._remove_entry(cache_key)
    
    def clear(self) -> int:
        """
        Clear all cached entries.
        
        Returns:
            Number of entries removed
        """
        count = len(self._entries)
        
        # Remove all cache files
        cache_entries_dir = self._cache_dir / "entries"
        if cache_entries_dir.exists():
            shutil.rmtree(cache_entries_dir)
        
        # Clear in-memory cache
        self._entries.clear()
        self._stats["total_size"] = 0
        
        # Save empty index
        self._save_cache_index()
        
        self._logger.info("Cache cleared", entries_removed=count)
        return count
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_requests = self._stats["hits"] + self._stats["misses"]
        hit_rate = self._stats["hits"] / total_requests if total_requests > 0 else 0
        
        return {
            **self._stats,
            "entries": len(self._entries),
            "hit_rate": hit_rate,
            "cache_size_mb": self._stats["total_size"] / (1024 * 1024),
            "cache_dir": str(self._cache_dir)
        }
    
    def list_entries(self, include_expired: bool = False) -> List[Dict[str, Any]]:
        """
        List all cache entries.
        
        Args:
            include_expired: Whether to include expired entries
            
        Returns:
            List of cache entry information
        """
        entries = []
        for cache_key, entry in self._entries.items():
            if not include_expired and entry.is_expired(self._default_ttl):
                continue
            
            entry_info = {
                "cache_key": cache_key,
                "schema_path": entry.schema_path,
                "schema_type": entry.schema_type,
                "created_at": datetime.fromtimestamp(entry.created_at).isoformat(),
                "accessed_at": datetime.fromtimestamp(entry.accessed_at).isoformat(),
                "access_count": entry.access_count,
                "file_size": entry.file_size,
                "compilation_duration": entry.compilation_duration,
                "is_expired": entry.is_expired(self._default_ttl)
            }
            entries.append(entry_info)
        
        return sorted(entries, key=lambda x: x["accessed_at"], reverse=True)
    
    def cleanup_expired_entries(self) -> int:
        """
        Clean up expired cache entries.
        
        Returns:
            Number of entries removed
        """
        return self._cleanup_expired_entries()
    
    def optimize_cache(self) -> Dict[str, Any]:
        """
        Optimize cache by removing unused and expired entries.
        
        Returns:
            Optimization statistics
        """
        start_time = time.time()
        initial_count = len(self._entries)
        initial_size = self._stats["total_size"]
        
        # Remove expired entries
        expired_removed = self._cleanup_expired_entries()
        
        # Remove least recently used entries if over limit
        lru_removed = 0
        if len(self._entries) > self._max_cache_size * 0.8:  # Optimize when 80% full
            lru_removed = self._remove_lru_entries(int(self._max_cache_size * 0.2))
        
        # Verify cached files exist
        missing_removed = self._remove_missing_files()
        
        duration = time.time() - start_time
        final_count = len(self._entries)
        final_size = self._stats["total_size"]
        
        stats = {
            "duration": duration,
            "initial_entries": initial_count,
            "final_entries": final_count,
            "expired_removed": expired_removed,
            "lru_removed": lru_removed,
            "missing_files_removed": missing_removed,
            "total_removed": expired_removed + lru_removed + missing_removed,
            "size_freed_mb": (initial_size - final_size) / (1024 * 1024)
        }
        
        self._logger.info("Cache optimized", **stats)
        return stats
    
    def _generate_cache_key(self, schema_path: str, schema_type: str) -> str:
        """Generate cache key for schema."""
        path_str = str(Path(schema_path).resolve())
        key_input = f"{path_str}:{schema_type}"
        return hashlib.sha256(key_input.encode()).hexdigest()[:16]
    
    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate SHA256 hash of file contents."""
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
    
    def _remove_entry(self, cache_key: str) -> bool:
        """Remove cache entry and associated files."""
        if cache_key not in self._entries:
            return False
        
        entry = self._entries[cache_key]
        
        # Remove cached files
        cache_entry_dir = self._cache_dir / "entries" / cache_key
        if cache_entry_dir.exists():
            shutil.rmtree(cache_entry_dir)
        
        # Update statistics
        self._stats["total_size"] -= entry.file_size
        
        # Remove from memory
        del self._entries[cache_key]
        
        return True
    
    def _enforce_cache_limit(self):
        """Enforce maximum cache size by removing LRU entries."""
        if len(self._entries) <= self._max_cache_size:
            return
        
        # Calculate how many entries to remove (remove 10% when limit reached)
        entries_to_remove = max(1, len(self._entries) - int(self._max_cache_size * 0.9))
        removed = self._remove_lru_entries(entries_to_remove)
        
        if removed > 0:
            self._stats["evictions"] += removed
            self._logger.debug("Cache limit enforced", entries_removed=removed)
    
    def _remove_lru_entries(self, count: int) -> int:
        """Remove least recently used entries."""
        if count <= 0 or not self._entries:
            return 0
        
        # Sort by access time (oldest first)
        sorted_entries = sorted(
            self._entries.items(),
            key=lambda x: x[1].accessed_at
        )
        
        removed = 0
        for cache_key, _ in sorted_entries[:count]:
            if self._remove_entry(cache_key):
                removed += 1
        
        return removed
    
    def _cleanup_expired_entries(self) -> int:
        """Clean up expired cache entries."""
        expired_keys = [
            cache_key for cache_key, entry in self._entries.items()
            if entry.is_expired(self._default_ttl)
        ]
        
        removed = 0
        for cache_key in expired_keys:
            if self._remove_entry(cache_key):
                removed += 1
        
        self._last_cleanup = time.time()
        
        if removed > 0:
            self._stats["cleanups"] += 1
            self._logger.debug("Expired entries cleaned", entries_removed=removed)
        
        return removed
    
    def _remove_missing_files(self) -> int:
        """Remove entries where cached files are missing."""
        missing_keys = []
        
        for cache_key, entry in self._entries.items():
            if not Path(entry.compiled_path).exists():
                missing_keys.append(cache_key)
        
        removed = 0
        for cache_key in missing_keys:
            if self._remove_entry(cache_key):
                removed += 1
        
        return removed
    
    def _load_cache_index(self):
        """Load cache index from disk."""
        if not self._cache_index_file.exists():
            return
        
        try:
            with open(self._cache_index_file, 'r') as f:
                data = json.load(f)
            
            # Load entries
            for cache_key, entry_data in data.get("entries", {}).items():
                try:
                    entry = CacheEntry.from_dict(entry_data)
                    self._entries[cache_key] = entry
                    self._stats["total_size"] += entry.file_size
                except Exception as e:
                    self._logger.warning("Failed to load cache entry", 
                                       cache_key=cache_key, error=str(e))
            
            # Load statistics
            self._stats.update(data.get("stats", {}))
            
            self._logger.debug("Cache index loaded", entries=len(self._entries))
            
        except Exception as e:
            self._logger.error("Failed to load cache index", error=str(e))
            self._entries.clear()
    
    def _save_cache_index(self):
        """Save cache index to disk."""
        try:
            data = {
                "entries": {
                    cache_key: entry.to_dict()
                    for cache_key, entry in self._entries.items()
                },
                "stats": self._stats,
                "updated_at": time.time()
            }
            
            # Write atomically
            temp_file = self._cache_index_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(data, f, indent=2)
            temp_file.replace(self._cache_index_file)
            
        except Exception as e:
            self._logger.error("Failed to save cache index", error=str(e))


# Global cache instance
_schema_cache: Optional[SchemaCacheManager] = None


def get_schema_cache() -> SchemaCacheManager:
    """Get global schema cache instance."""
    global _schema_cache
    if _schema_cache is None:
        _schema_cache = SchemaCacheManager()
    return _schema_cache


def configure_schema_cache(cache_dir: Optional[Path] = None, max_cache_size: int = 1000,
                          default_ttl: int = 3600) -> SchemaCacheManager:
    """Configure global schema cache."""
    global _schema_cache
    _schema_cache = SchemaCacheManager(
        cache_dir=cache_dir,
        max_cache_size=max_cache_size,
        default_ttl=default_ttl
    )
    return _schema_cache