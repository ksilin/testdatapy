"""CLI commands for schema cache and version management.

This module provides command-line interface for managing schema caches,
versions, and optimization operations.
"""

import json
import sys
from pathlib import Path
from typing import Optional

import click

from .schema_cache import (
    get_schema_cache, configure_schema_cache,
    get_version_manager, configure_version_manager
)
from .logging_config import get_schema_logger


@click.group()
@click.pass_context
def cache(ctx):
    """Schema cache and version management commands."""
    # Ensure context object exists
    if ctx.obj is None:
        ctx.obj = {}


@cache.group()
@click.pass_context  
def schema_cache(ctx):
    """Schema cache management commands."""
    pass


@cache.group()
@click.pass_context
def versions(ctx):
    """Schema version management commands."""
    pass


# === CACHE MANAGEMENT COMMANDS ===

@schema_cache.command()
@click.option('--cache-dir', type=click.Path(), help='Custom cache directory')
@click.option('--max-size', type=int, default=1000, help='Maximum cache entries')
@click.option('--ttl', type=int, default=3600, help='Default TTL in seconds')
@click.pass_context
def configure(ctx, cache_dir: Optional[str], max_size: int, ttl: int):
    """Configure schema cache settings."""
    try:
        cache_path = Path(cache_dir) if cache_dir else None
        cache_manager = configure_schema_cache(
            cache_dir=cache_path,
            max_cache_size=max_size,
            default_ttl=ttl
        )
        
        click.echo(f"✅ Schema cache configured")
        click.echo(f"   Directory: {cache_manager._cache_dir}")
        click.echo(f"   Max size: {max_size}")
        click.echo(f"   TTL: {ttl}s")
        
    except Exception as e:
        click.echo(f"❌ Failed to configure cache: {e}", err=True)
        sys.exit(1)


@schema_cache.command()
@click.pass_context
def stats(ctx):
    """Show cache statistics."""
    try:
        cache_manager = get_schema_cache()
        stats = cache_manager.get_stats()
        
        click.echo("📊 Cache Statistics")
        click.echo(f"   Entries: {stats['entries']}")
        click.echo(f"   Hits: {stats['hits']}")
        click.echo(f"   Misses: {stats['misses']}")
        click.echo(f"   Hit rate: {stats['hit_rate']:.1%}")
        click.echo(f"   Evictions: {stats['evictions']}")
        click.echo(f"   Cache size: {stats['cache_size_mb']:.1f} MB")
        click.echo(f"   Directory: {stats['cache_dir']}")
        
    except Exception as e:
        click.echo(f"❌ Failed to get cache stats: {e}", err=True)
        sys.exit(1)


@schema_cache.command()
@click.option('--include-expired', is_flag=True, help='Include expired entries')
@click.option('--format', 'output_format', type=click.Choice(['table', 'json']), 
              default='table', help='Output format')
@click.pass_context
def list(ctx, include_expired: bool, output_format: str):
    """List cached schema entries."""
    try:
        cache_manager = get_schema_cache()
        entries = cache_manager.list_entries(include_expired=include_expired)
        
        if output_format == 'json':
            click.echo(json.dumps(entries, indent=2))
            return
        
        if not entries:
            click.echo("No cache entries found")
            return
        
        click.echo(f"📋 Cached Schemas ({len(entries)} entries)")
        click.echo()
        
        # Table format
        for entry in entries:
            status = "EXPIRED" if entry['is_expired'] else "VALID"
            click.echo(f"🔹 {Path(entry['schema_path']).name}")
            click.echo(f"   Status: {status}")
            click.echo(f"   Type: {entry['schema_type']}")
            click.echo(f"   Created: {entry['created_at']}")
            click.echo(f"   Accessed: {entry['accessed_at']}")
            click.echo(f"   Count: {entry['access_count']}")
            click.echo(f"   Size: {entry['file_size']} bytes")
            click.echo(f"   Duration: {entry['compilation_duration']:.3f}s")
            click.echo()
        
    except Exception as e:
        click.echo(f"❌ Failed to list cache entries: {e}", err=True)
        sys.exit(1)


@schema_cache.command()
@click.argument('schema_path', type=click.Path(exists=True))
@click.option('--schema-type', default='protobuf', help='Schema type')
@click.pass_context
def remove(ctx, schema_path: str, schema_type: str):
    """Remove cached entry for a schema."""
    try:
        cache_manager = get_schema_cache()
        removed = cache_manager.remove(schema_path, schema_type)
        
        if removed:
            click.echo(f"✅ Removed cache entry for {schema_path}")
        else:
            click.echo(f"⚠️  No cache entry found for {schema_path}")
        
    except Exception as e:
        click.echo(f"❌ Failed to remove cache entry: {e}", err=True)
        sys.exit(1)


@schema_cache.command()
@click.option('--confirm', is_flag=True, help='Skip confirmation prompt')
@click.pass_context
def clear(ctx, confirm: bool):
    """Clear all cached entries."""
    try:
        if not confirm:
            click.echo("⚠️  This will remove all cached schema entries.")
            if not click.confirm("Do you want to continue?"):
                click.echo("Operation cancelled.")
                return
        
        cache_manager = get_schema_cache()
        removed_count = cache_manager.clear()
        
        click.echo(f"✅ Cleared cache ({removed_count} entries removed)")
        
    except Exception as e:
        click.echo(f"❌ Failed to clear cache: {e}", err=True)
        sys.exit(1)


@schema_cache.command()
@click.pass_context
def optimize(ctx):
    """Optimize cache by removing expired and unused entries."""
    try:
        cache_manager = get_schema_cache()
        
        click.echo("🔧 Optimizing schema cache...")
        stats = cache_manager.optimize_cache()
        
        click.echo(f"✅ Cache optimization completed")
        click.echo(f"   Duration: {stats['duration']:.2f}s")
        click.echo(f"   Initial entries: {stats['initial_entries']}")
        click.echo(f"   Final entries: {stats['final_entries']}")
        click.echo(f"   Expired removed: {stats['expired_removed']}")
        click.echo(f"   LRU removed: {stats['lru_removed']}")
        click.echo(f"   Missing files removed: {stats['missing_files_removed']}")
        click.echo(f"   Total removed: {stats['total_removed']}")
        click.echo(f"   Space freed: {stats['size_freed_mb']:.1f} MB")
        
    except Exception as e:
        click.echo(f"❌ Failed to optimize cache: {e}", err=True)
        sys.exit(1)


@schema_cache.command()
@click.pass_context
def cleanup(ctx):
    """Clean up expired cache entries."""
    try:
        cache_manager = get_schema_cache()
        removed_count = cache_manager.cleanup_expired_entries()
        
        click.echo(f"✅ Cleanup completed ({removed_count} expired entries removed)")
        
    except Exception as e:
        click.echo(f"❌ Failed to cleanup cache: {e}", err=True)
        sys.exit(1)


# === VERSION MANAGEMENT COMMANDS ===

@versions.command()
@click.option('--versions-dir', type=click.Path(), help='Custom versions directory')
@click.option('--max-versions', type=int, default=50, help='Max versions per schema')
@click.pass_context
def configure(ctx, versions_dir: Optional[str], max_versions: int):
    """Configure schema version management."""
    try:
        versions_path = Path(versions_dir) if versions_dir else None
        version_manager = configure_version_manager(
            versions_dir=versions_path,
            max_versions_per_schema=max_versions
        )
        
        click.echo(f"✅ Version management configured")
        click.echo(f"   Directory: {version_manager._versions_dir}")
        click.echo(f"   Max versions: {max_versions}")
        
    except Exception as e:
        click.echo(f"❌ Failed to configure version management: {e}", err=True)
        sys.exit(1)


@versions.command()
@click.argument('schema_path', type=click.Path(exists=True))
@click.option('--version', help='Version string (auto-generated if not provided)')
@click.option('--description', default='', help='Version description')
@click.option('--tag', 'tags', multiple=True, help='Version tags (can be used multiple times)')
@click.pass_context
def create(ctx, schema_path: str, version: Optional[str], description: str, tags: tuple):
    """Create a new version of a schema."""
    try:
        version_manager = get_version_manager()
        version_info = version_manager.create_version(
            schema_path=schema_path,
            version=version,
            description=description,
            tags=list(tags)
        )
        
        click.echo(f"✅ Created version {version_info.version} for {Path(schema_path).name}")
        click.echo(f"   Schema: {version_info.schema_path}")
        click.echo(f"   Hash: {version_info.schema_hash[:16]}...")
        if version_info.parent_version:
            click.echo(f"   Parent: {version_info.parent_version}")
        if description:
            click.echo(f"   Description: {description}")
        if tags:
            click.echo(f"   Tags: {', '.join(tags)}")
        
    except Exception as e:
        click.echo(f"❌ Failed to create version: {e}", err=True)
        sys.exit(1)


@versions.command()
@click.option('--format', 'output_format', type=click.Choice(['table', 'json']),
              default='table', help='Output format')
@click.pass_context
def list_schemas(ctx, output_format: str):
    """List all schemas with versions."""
    try:
        version_manager = get_version_manager()
        schemas = version_manager.list_schemas()
        
        if not schemas:
            click.echo("No versioned schemas found")
            return
        
        if output_format == 'json':
            stats = version_manager.get_statistics()
            click.echo(json.dumps(stats, indent=2))
            return
        
        click.echo(f"📁 Versioned Schemas ({len(schemas)})")
        click.echo()
        
        for schema_name in sorted(schemas):
            versions = version_manager.list_versions(schema_name, limit=5)
            latest = versions[0] if versions else None
            
            click.echo(f"🔹 {schema_name}")
            if latest:
                click.echo(f"   Latest: {latest.version}")
                click.echo(f"   Versions: {len(versions)}")
                click.echo(f"   Updated: {latest.created_at}")
            click.echo()
        
    except Exception as e:
        click.echo(f"❌ Failed to list schemas: {e}", err=True)
        sys.exit(1)


@versions.command()
@click.argument('schema_name')
@click.option('--limit', type=int, help='Maximum number of versions to show')
@click.option('--format', 'output_format', type=click.Choice(['table', 'json']),
              default='table', help='Output format')
@click.pass_context
def list_versions(ctx, schema_name: str, limit: Optional[int], output_format: str):
    """List versions for a schema."""
    try:
        version_manager = get_version_manager()
        versions = version_manager.list_versions(schema_name, limit=limit)
        
        if not versions:
            click.echo(f"No versions found for schema: {schema_name}")
            return
        
        if output_format == 'json':
            version_data = [v.to_dict() for v in versions]
            click.echo(json.dumps(version_data, indent=2))
            return
        
        click.echo(f"📋 Versions for {schema_name} ({len(versions)})")
        click.echo()
        
        for version_info in versions:
            click.echo(f"🔹 {version_info.version}")
            click.echo(f"   Created: {version_info.created_at}")
            click.echo(f"   Age: {version_info.get_age_days():.1f} days")
            click.echo(f"   Hash: {version_info.schema_hash[:16]}...")
            if version_info.parent_version:
                click.echo(f"   Parent: {version_info.parent_version}")
            if version_info.description:
                click.echo(f"   Description: {version_info.description}")
            if version_info.tags:
                click.echo(f"   Tags: {', '.join(version_info.tags)}")
            click.echo()
        
    except Exception as e:
        click.echo(f"❌ Failed to list versions: {e}", err=True)
        sys.exit(1)


@versions.command()
@click.argument('schema_name')
@click.argument('from_version')
@click.argument('to_version')
@click.option('--format', 'output_format', type=click.Choice(['table', 'json']),
              default='table', help='Output format')
@click.pass_context
def compare(ctx, schema_name: str, from_version: str, to_version: str, output_format: str):
    """Compare two versions of a schema."""
    try:
        version_manager = get_version_manager()
        diff = version_manager.compare_versions(schema_name, from_version, to_version)
        
        if output_format == 'json':
            click.echo(json.dumps(diff.to_dict(), indent=2))
            return
        
        # Determine status emoji
        status_emoji = {
            'identical': '✅',
            'compatible': '🟡', 
            'breaking_change': '🔴',
            'unknown': '❓'
        }
        
        emoji = status_emoji.get(diff.comparison.value, '❓')
        
        click.echo(f"📊 Version Comparison")
        click.echo(f"   Schema: {schema_name}")
        click.echo(f"   From: {from_version}")
        click.echo(f"   To: {to_version}")
        click.echo(f"   Result: {emoji} {diff.comparison.value.upper()}")
        click.echo()
        
        if diff.changes:
            click.echo("📝 Changes:")
            for change in diff.changes:
                breaking_indicator = " 🔴" if change.get('breaking', False) else ""
                click.echo(f"   • {change['description']}{breaking_indicator}")
            click.echo()
        
        if diff.breaking_changes:
            click.echo("⚠️  Breaking Changes:")
            for change in diff.breaking_changes:
                click.echo(f"   • {change['description']}")
            click.echo()
        
        if diff.migration_notes:
            click.echo(f"📋 Migration Notes:")
            click.echo(f"   {diff.migration_notes}")
        
    except Exception as e:
        click.echo(f"❌ Failed to compare versions: {e}", err=True)
        sys.exit(1)


@versions.command()
@click.argument('schema_name')
@click.argument('version')
@click.option('--confirm', is_flag=True, help='Skip confirmation prompt')
@click.pass_context
def delete(ctx, schema_name: str, version: str, confirm: bool):
    """Delete a specific version."""
    try:
        if not confirm:
            click.echo(f"⚠️  This will permanently delete version {version} of {schema_name}.")
            if not click.confirm("Do you want to continue?"):
                click.echo("Operation cancelled.")
                return
        
        version_manager = get_version_manager()
        deleted = version_manager.delete_version(schema_name, version)
        
        if deleted:
            click.echo(f"✅ Deleted version {version} of {schema_name}")
        else:
            click.echo(f"⚠️  Version {version} not found for {schema_name}")
        
    except Exception as e:
        click.echo(f"❌ Failed to delete version: {e}", err=True)
        sys.exit(1)


@versions.command()
@click.option('--max-age-days', type=int, default=90, help='Maximum age in days')
@click.option('--keep-minimum', type=int, default=5, help='Minimum versions to keep')
@click.option('--confirm', is_flag=True, help='Skip confirmation prompt')
@click.pass_context
def cleanup(ctx, max_age_days: int, keep_minimum: int, confirm: bool):
    """Clean up old versions based on age."""
    try:
        if not confirm:
            click.echo(f"⚠️  This will delete versions older than {max_age_days} days.")
            click.echo(f"   Minimum {keep_minimum} versions will be kept per schema.")
            if not click.confirm("Do you want to continue?"):
                click.echo("Operation cancelled.")
                return
        
        version_manager = get_version_manager()
        cleanup_stats = version_manager.cleanup_old_versions(
            max_age_days=max_age_days,
            keep_minimum=keep_minimum
        )
        
        if cleanup_stats:
            total_removed = sum(cleanup_stats.values())
            click.echo(f"✅ Cleaned up {total_removed} old versions")
            for schema_name, count in cleanup_stats.items():
                click.echo(f"   {schema_name}: {count} versions removed")
        else:
            click.echo("✅ No old versions found to clean up")
        
    except Exception as e:
        click.echo(f"❌ Failed to cleanup versions: {e}", err=True)
        sys.exit(1)


@versions.command()
@click.argument('schema_name')
@click.pass_context
def tree(ctx, schema_name: str):
    """Show version tree for a schema."""
    try:
        version_manager = get_version_manager()
        tree = version_manager.get_version_tree(schema_name)
        
        if not tree:
            click.echo(f"No versions found for schema: {schema_name}")
            return
        
        click.echo(f"🌳 Version Tree for {schema_name}")
        click.echo()
        
        # Find root versions (no parent)
        roots = [v for v, data in tree.items() if not data['info']['parent_version']]
        
        def print_tree(version: str, level: int = 0):
            indent = "  " * level
            version_info = tree[version]['info']
            
            click.echo(f"{indent}🔹 {version}")
            click.echo(f"{indent}   Created: {version_info['created_at']}")
            
            # Print children
            for child in tree[version]['children']:
                print_tree(child, level + 1)
        
        for root in sorted(roots):
            print_tree(root)
            click.echo()
        
    except Exception as e:
        click.echo(f"❌ Failed to show version tree: {e}", err=True)
        sys.exit(1)


@versions.command()
@click.pass_context
def stats(ctx):
    """Show version management statistics."""
    try:
        version_manager = get_version_manager()
        stats = version_manager.get_statistics()
        
        click.echo("📊 Version Management Statistics")
        click.echo(f"   Total schemas: {stats['total_schemas']}")
        click.echo(f"   Total versions: {stats['total_versions']}")
        click.echo(f"   Storage directory: {stats['storage_dir']}")
        click.echo()
        
        if stats['schemas']:
            click.echo("📁 Schema Details:")
            for schema_name, schema_stats in stats['schemas'].items():
                click.echo(f"   🔹 {schema_name}")
                click.echo(f"      Versions: {schema_stats['version_count']}")
                click.echo(f"      Latest: {schema_stats['latest_version']}")
                click.echo(f"      Age: {schema_stats['age_days']:.1f} days")
        
    except Exception as e:
        click.echo(f"❌ Failed to get version stats: {e}", err=True)
        sys.exit(1)


# Add cache commands to main CLI
def register_cache_commands(main_cli):
    """Register cache commands with the main CLI."""
    main_cli.add_command(cache)


if __name__ == '__main__':
    cache()