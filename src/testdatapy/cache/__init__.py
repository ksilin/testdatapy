"""Cache system compatibility module.

This module provides backward compatibility aliases to match documentation examples.
"""

# Create compatibility aliases for documented cache interfaces
try:
    from testdatapy.schema_cache.cache_manager import CacheManager as SchemaCache
    from testdatapy.schema_cache.version_manager import VersionManager as SchemaVersionManager
except ImportError:
    # Fallback if schema_cache module structure is different
    class SchemaCache:
        """Placeholder SchemaCache for compatibility."""
        def __init__(self, *args, **kwargs):
            raise ImportError("SchemaCache implementation not found. Check schema_cache module structure.")
    
    class SchemaVersionManager:
        """Placeholder SchemaVersionManager for compatibility."""
        def __init__(self, *args, **kwargs):
            raise ImportError("SchemaVersionManager implementation not found. Check schema_cache module structure.")

__all__ = [
    "SchemaCache",
    "SchemaVersionManager"
]