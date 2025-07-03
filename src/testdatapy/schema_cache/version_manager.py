"""Schema versioning and evolution management.

This module provides comprehensive version management for schemas including
version tracking, compatibility checking, and evolution support.
"""

import json
import time
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum

from ..logging_config import get_schema_logger
from ..performance.performance_monitor import get_performance_monitor, monitored


class VersionComparison(Enum):
    """Version comparison results."""
    IDENTICAL = "identical"
    COMPATIBLE = "compatible"
    BREAKING_CHANGE = "breaking_change"
    UNKNOWN = "unknown"


@dataclass
class VersionInfo:
    """Information about a schema version."""
    version: str
    schema_path: str
    schema_hash: str
    created_at: float
    created_by: str
    description: str
    tags: List[str]
    metadata: Dict[str, Any]
    dependencies: List[str]
    parent_version: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'VersionInfo':
        """Create from dictionary."""
        return cls(**data)
    
    def get_age_days(self) -> float:
        """Get version age in days."""
        return (time.time() - self.created_at) / 86400


@dataclass
class VersionDiff:
    """Difference between two schema versions."""
    from_version: str
    to_version: str
    comparison: VersionComparison
    changes: List[Dict[str, Any]]
    breaking_changes: List[Dict[str, Any]]
    migration_notes: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


class SchemaVersionManager:
    """Manages schema versions and evolution."""
    
    def __init__(self, versions_dir: Optional[Path] = None, max_versions_per_schema: int = 50):
        """
        Initialize schema version manager.
        
        Args:
            versions_dir: Directory for version storage
            max_versions_per_schema: Maximum versions to keep per schema
        """
        self._logger = get_schema_logger(__name__)
        self._monitor = get_performance_monitor()
        
        # Set up versions directory
        if versions_dir is None:
            versions_dir = Path.home() / ".testdatapy" / "schema_versions"
        self._versions_dir = Path(versions_dir)
        self._versions_dir.mkdir(parents=True, exist_ok=True)
        
        self._max_versions_per_schema = max_versions_per_schema
        
        # Version storage
        self._versions: Dict[str, Dict[str, VersionInfo]] = {}  # schema_name -> version -> info
        self._version_index_file = self._versions_dir / "version_index.json"
        
        # Load existing versions
        self._load_version_index()
        
        self._logger.info("SchemaVersionManager initialized",
                         versions_dir=str(self._versions_dir),
                         max_versions=max_versions_per_schema)
    
    @monitored("version_create")
    def create_version(self, schema_path: str, version: Optional[str] = None,
                      description: str = "", tags: Optional[List[str]] = None,
                      metadata: Optional[Dict[str, Any]] = None,
                      dependencies: Optional[List[str]] = None) -> VersionInfo:
        """
        Create a new version of a schema.
        
        Args:
            schema_path: Path to schema file
            version: Version string (auto-generated if None)
            description: Version description
            tags: Version tags
            metadata: Additional metadata
            dependencies: Schema dependencies
            
        Returns:
            Created version info
        """
        schema_name = self._get_schema_name(schema_path)
        
        # Auto-generate version if not provided
        if version is None:
            version = self._generate_version(schema_name)
        
        # Calculate schema hash
        schema_hash = self._calculate_schema_hash(schema_path)
        
        # Check if this exact version already exists
        if schema_name in self._versions and version in self._versions[schema_name]:
            existing = self._versions[schema_name][version]
            if existing.schema_hash == schema_hash:
                self._logger.debug("Version already exists with same content", 
                                 schema=schema_name, version=version)
                return existing
            else:
                raise ValueError(f"Version {version} already exists for {schema_name} with different content")
        
        # Create version directory
        version_dir = self._versions_dir / schema_name / version
        version_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy schema file to version directory
        versioned_schema_path = version_dir / Path(schema_path).name
        import shutil
        shutil.copy2(schema_path, versioned_schema_path)
        
        # Find parent version (latest existing version)
        parent_version = self._get_latest_version(schema_name)
        
        # Create version info
        version_info = VersionInfo(
            version=version,
            schema_path=str(versioned_schema_path),
            schema_hash=schema_hash,
            created_at=time.time(),
            created_by="testdatapy",  # Could be made configurable
            description=description,
            tags=tags or [],
            metadata=metadata or {},
            dependencies=dependencies or [],
            parent_version=parent_version.version if parent_version else None
        )
        
        # Store version
        if schema_name not in self._versions:
            self._versions[schema_name] = {}
        self._versions[schema_name][version] = version_info
        
        # Enforce version limit
        self._enforce_version_limit(schema_name)
        
        # Save index
        self._save_version_index()
        
        self._logger.info("Schema version created",
                         schema=schema_name,
                         version=version,
                         parent=parent_version.version if parent_version else None)
        
        return version_info
    
    @monitored("version_get")
    def get_version(self, schema_name: str, version: str) -> Optional[VersionInfo]:
        """
        Get specific version information.
        
        Args:
            schema_name: Schema name
            version: Version string
            
        Returns:
            Version info if found
        """
        if schema_name not in self._versions:
            return None
        return self._versions[schema_name].get(version)
    
    def get_latest_version(self, schema_name: str) -> Optional[VersionInfo]:
        """
        Get latest version of a schema.
        
        Args:
            schema_name: Schema name
            
        Returns:
            Latest version info if found
        """
        return self._get_latest_version(schema_name)
    
    def list_versions(self, schema_name: str, limit: Optional[int] = None) -> List[VersionInfo]:
        """
        List versions for a schema.
        
        Args:
            schema_name: Schema name
            limit: Maximum number of versions to return
            
        Returns:
            List of version info ordered by creation time (newest first)
        """
        if schema_name not in self._versions:
            return []
        
        versions = list(self._versions[schema_name].values())
        versions.sort(key=lambda v: v.created_at, reverse=True)
        
        if limit:
            versions = versions[:limit]
        
        return versions
    
    def list_schemas(self) -> List[str]:
        """
        List all schemas with versions.
        
        Returns:
            List of schema names
        """
        return list(self._versions.keys())
    
    @monitored("version_compare")
    def compare_versions(self, schema_name: str, from_version: str, 
                        to_version: str) -> VersionDiff:
        """
        Compare two versions of a schema.
        
        Args:
            schema_name: Schema name
            from_version: Source version
            to_version: Target version
            
        Returns:
            Version comparison result
        """
        from_info = self.get_version(schema_name, from_version)
        to_info = self.get_version(schema_name, to_version)
        
        if not from_info or not to_info:
            raise ValueError(f"Version not found for {schema_name}")
        
        # Compare hashes for identical check
        if from_info.schema_hash == to_info.schema_hash:
            return VersionDiff(
                from_version=from_version,
                to_version=to_version,
                comparison=VersionComparison.IDENTICAL,
                changes=[],
                breaking_changes=[]
            )
        
        # Detailed comparison (simplified for this implementation)
        changes = self._analyze_schema_changes(from_info, to_info)
        breaking_changes = [c for c in changes if c.get("breaking", False)]
        
        # Determine compatibility
        if breaking_changes:
            comparison = VersionComparison.BREAKING_CHANGE
        else:
            comparison = VersionComparison.COMPATIBLE
        
        return VersionDiff(
            from_version=from_version,
            to_version=to_version,
            comparison=comparison,
            changes=changes,
            breaking_changes=breaking_changes
        )
    
    def delete_version(self, schema_name: str, version: str) -> bool:
        """
        Delete a specific version.
        
        Args:
            schema_name: Schema name
            version: Version to delete
            
        Returns:
            True if deleted, False if not found
        """
        if schema_name not in self._versions or version not in self._versions[schema_name]:
            return False
        
        # Remove version directory
        version_dir = self._versions_dir / schema_name / version
        if version_dir.exists():
            import shutil
            shutil.rmtree(version_dir)
        
        # Remove from memory
        del self._versions[schema_name][version]
        
        # Clean up empty schema
        if not self._versions[schema_name]:
            del self._versions[schema_name]
            schema_dir = self._versions_dir / schema_name
            if schema_dir.exists():
                schema_dir.rmdir()
        
        # Save index
        self._save_version_index()
        
        self._logger.info("Version deleted", schema=schema_name, version=version)
        return True
    
    def cleanup_old_versions(self, max_age_days: int = 90,
                           keep_minimum: int = 5) -> Dict[str, int]:
        """
        Clean up old versions based on age.
        
        Args:
            max_age_days: Maximum age in days
            keep_minimum: Minimum versions to keep per schema
            
        Returns:
            Cleanup statistics
        """
        cleanup_stats = {}
        total_removed = 0
        
        for schema_name in list(self._versions.keys()):
            versions = self.list_versions(schema_name)
            
            # Keep minimum number of recent versions
            versions_to_check = versions[keep_minimum:]
            
            removed_count = 0
            for version_info in versions_to_check:
                if version_info.get_age_days() > max_age_days:
                    if self.delete_version(schema_name, version_info.version):
                        removed_count += 1
            
            if removed_count > 0:
                cleanup_stats[schema_name] = removed_count
                total_removed += removed_count
        
        self._logger.info("Old versions cleaned up",
                         schemas_affected=len(cleanup_stats),
                         total_removed=total_removed)
        
        return cleanup_stats
    
    def get_version_tree(self, schema_name: str) -> Dict[str, Any]:
        """
        Get version tree showing parent-child relationships.
        
        Args:
            schema_name: Schema name
            
        Returns:
            Version tree structure
        """
        if schema_name not in self._versions:
            return {}
        
        versions = self._versions[schema_name]
        tree = {}
        
        # Build tree structure
        for version, version_info in versions.items():
            tree[version] = {
                "info": version_info.to_dict(),
                "children": []
            }
        
        # Link children to parents
        for version, version_info in versions.items():
            if version_info.parent_version and version_info.parent_version in tree:
                tree[version_info.parent_version]["children"].append(version)
        
        return tree
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get version management statistics."""
        total_versions = sum(len(versions) for versions in self._versions.values())
        
        schema_stats = {}
        for schema_name, versions in self._versions.items():
            version_list = list(versions.values())
            schema_stats[schema_name] = {
                "version_count": len(versions),
                "latest_version": max(version_list, key=lambda v: v.created_at).version if version_list else None,
                "oldest_version": min(version_list, key=lambda v: v.created_at).version if version_list else None,
                "age_days": max(v.get_age_days() for v in version_list) if version_list else 0
            }
        
        return {
            "total_schemas": len(self._versions),
            "total_versions": total_versions,
            "schemas": schema_stats,
            "storage_dir": str(self._versions_dir)
        }
    
    def _get_schema_name(self, schema_path: str) -> str:
        """Extract schema name from path."""
        return Path(schema_path).stem
    
    def _generate_version(self, schema_name: str) -> str:
        """Generate auto-incremented version."""
        if schema_name not in self._versions:
            return "v1.0.0"
        
        # Find highest version number
        versions = list(self._versions[schema_name].keys())
        version_numbers = []
        
        for version in versions:
            try:
                # Extract numeric parts from version (e.g., "v1.2.3" -> [1, 2, 3])
                if version.startswith('v'):
                    parts = version[1:].split('.')
                    version_numbers.append([int(p) for p in parts])
            except ValueError:
                continue
        
        if not version_numbers:
            return "v1.0.0"
        
        # Find max version and increment patch number
        max_version = max(version_numbers)
        max_version[2] += 1  # Increment patch number
        
        return f"v{'.'.join(map(str, max_version))}"
    
    def _calculate_schema_hash(self, schema_path: str) -> str:
        """Calculate hash of schema file."""
        hasher = hashlib.sha256()
        with open(schema_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
    
    def _get_latest_version(self, schema_name: str) -> Optional[VersionInfo]:
        """Get latest version for a schema."""
        if schema_name not in self._versions:
            return None
        
        versions = list(self._versions[schema_name].values())
        if not versions:
            return None
        
        return max(versions, key=lambda v: v.created_at)
    
    def _enforce_version_limit(self, schema_name: str):
        """Enforce maximum versions per schema."""
        if schema_name not in self._versions:
            return
        
        versions = list(self._versions[schema_name].values())
        if len(versions) <= self._max_versions_per_schema:
            return
        
        # Sort by creation time and keep the newest ones
        versions.sort(key=lambda v: v.created_at, reverse=True)
        versions_to_remove = versions[self._max_versions_per_schema:]
        
        for version_info in versions_to_remove:
            self.delete_version(schema_name, version_info.version)
    
    def _analyze_schema_changes(self, from_info: VersionInfo, to_info: VersionInfo) -> List[Dict[str, Any]]:
        """
        Analyze changes between schema versions.
        
        This is a simplified implementation. In practice, you'd want more
        sophisticated schema parsing and comparison logic.
        """
        changes = []
        
        # Basic file size comparison
        try:
            from_size = Path(from_info.schema_path).stat().st_size
            to_size = Path(to_info.schema_path).stat().st_size
            
            if from_size != to_size:
                changes.append({
                    "type": "file_size_change",
                    "description": f"File size changed from {from_size} to {to_size} bytes",
                    "breaking": False
                })
        except FileNotFoundError:
            pass
        
        # Metadata comparison
        if from_info.metadata != to_info.metadata:
            changes.append({
                "type": "metadata_change",
                "description": "Metadata updated",
                "breaking": False
            })
        
        # Dependencies comparison
        if set(from_info.dependencies) != set(to_info.dependencies):
            changes.append({
                "type": "dependencies_change",
                "description": "Dependencies modified",
                "breaking": True  # Dependency changes could be breaking
            })
        
        # If no specific changes detected but hashes differ, mark as content change
        if not changes and from_info.schema_hash != to_info.schema_hash:
            changes.append({
                "type": "content_change",
                "description": "Schema content modified",
                "breaking": False  # Conservative assumption
            })
        
        return changes
    
    def _load_version_index(self):
        """Load version index from disk."""
        if not self._version_index_file.exists():
            return
        
        try:
            with open(self._version_index_file, 'r') as f:
                data = json.load(f)
            
            # Load versions
            for schema_name, versions_data in data.get("versions", {}).items():
                self._versions[schema_name] = {}
                for version, version_data in versions_data.items():
                    try:
                        version_info = VersionInfo.from_dict(version_data)
                        self._versions[schema_name][version] = version_info
                    except Exception as e:
                        self._logger.warning("Failed to load version info",
                                           schema=schema_name, version=version, error=str(e))
            
            self._logger.debug("Version index loaded", schemas=len(self._versions))
            
        except Exception as e:
            self._logger.error("Failed to load version index", error=str(e))
            self._versions.clear()
    
    def _save_version_index(self):
        """Save version index to disk."""
        try:
            data = {
                "versions": {
                    schema_name: {
                        version: version_info.to_dict()
                        for version, version_info in versions.items()
                    }
                    for schema_name, versions in self._versions.items()
                },
                "updated_at": time.time()
            }
            
            # Write atomically
            temp_file = self._version_index_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(data, f, indent=2)
            temp_file.replace(self._version_index_file)
            
        except Exception as e:
            self._logger.error("Failed to save version index", error=str(e))


# Global version manager instance
_version_manager: Optional[SchemaVersionManager] = None


def get_version_manager() -> SchemaVersionManager:
    """Get global version manager instance."""
    global _version_manager
    if _version_manager is None:
        _version_manager = SchemaVersionManager()
    return _version_manager


def configure_version_manager(versions_dir: Optional[Path] = None,
                             max_versions_per_schema: int = 50) -> SchemaVersionManager:
    """Configure global version manager."""
    global _version_manager
    _version_manager = SchemaVersionManager(
        versions_dir=versions_dir,
        max_versions_per_schema=max_versions_per_schema
    )
    return _version_manager