"""Schema caching and versioning system.

This package provides comprehensive caching capabilities for compiled schemas,
version management, and cache optimization features.
"""

from .cache_manager import (
    SchemaCacheManager,
    CacheEntry,
    get_schema_cache,
    configure_schema_cache
)

from .version_manager import (
    SchemaVersionManager,
    VersionInfo,
    VersionComparison,
    get_version_manager,
    configure_version_manager
)

__all__ = [
    'SchemaCacheManager',
    'CacheEntry', 
    'get_schema_cache',
    'configure_schema_cache',
    'SchemaVersionManager',
    'VersionInfo',
    'VersionComparison',
    'get_version_manager',
    'configure_version_manager'
]