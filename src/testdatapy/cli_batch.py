"""CLI commands for batch schema operations.

This module provides command-line interface for batch compilation,
registration, and validation operations.
"""

import json
import sys
from pathlib import Path
from typing import Optional, List

import click

from .batch_operations import (
    BatchCompiler, BatchRegistryManager, BatchValidator,
    ValidationLevel, create_progress_tracker
)
from .logging_config import get_schema_logger


@click.group()
@click.pass_context
def batch(ctx):
    """Batch schema processing commands."""
    # Ensure context object exists
    if ctx.obj is None:
        ctx.obj = {}


@batch.group()
@click.pass_context
def compile(ctx):
    """Batch compilation commands."""
    pass


@batch.group()
@click.pass_context
def register(ctx):
    """Batch registration commands."""
    pass


@batch.group()
@click.pass_context
def validate(ctx):
    """Batch validation commands."""
    pass


# === BATCH COMPILATION COMMANDS ===

@compile.command()
@click.argument('directory', type=click.Path(exists=True, file_okay=False))
@click.option('--pattern', default='*.proto', help='File pattern to match')
@click.option('--recursive/--no-recursive', default=True, help='Search recursively')
@click.option('--include-path', 'include_paths', multiple=True, 
              help='Include paths for compilation (can be used multiple times)')
@click.option('--output-dir', type=click.Path(), help='Output directory for compiled schemas')
@click.option('--parallel/--sequential', default=True, help='Use parallel processing')
@click.option('--max-workers', type=int, default=4, help='Maximum worker threads')
@click.option('--use-cache/--no-cache', default=True, help='Use schema caching')
@click.option('--show-progress/--no-progress', default=True, help='Show progress')
@click.option('--format', 'output_format', type=click.Choice(['table', 'json']),
              default='table', help='Output format')
@click.pass_context
def directory(ctx, directory: str, pattern: str, recursive: bool,
              include_paths: tuple, output_dir: Optional[str],
              parallel: bool, max_workers: int, use_cache: bool,
              show_progress: bool, output_format: str):
    """Compile all schemas in a directory."""
    try:
        # Initialize compiler
        compiler = BatchCompiler(max_workers=max_workers, use_cache=use_cache)
        
        # Discover schemas
        click.echo(f"🔍 Discovering schemas in {directory}...")
        schema_files = compiler.discover_schemas(
            directory=directory,
            pattern=pattern,
            recursive=recursive
        )
        
        if not schema_files:
            click.echo(f"No schema files found matching pattern: {pattern}")
            return
        
        click.echo(f"Found {len(schema_files)} schema files")
        
        # Set up progress callback
        progress_callback = None
        if show_progress:
            progress_callback = create_progress_tracker(show_details=True)
        
        # Compile schemas
        click.echo("🚀 Starting batch compilation...")
        result = compiler.compile_batch(
            schema_paths=schema_files,
            include_paths=list(include_paths) if include_paths else None,
            output_dir=output_dir,
            parallel=parallel,
            progress_callback=progress_callback
        )
        
        if show_progress:
            print()  # New line after progress
        
        # Display results
        if output_format == 'json':
            click.echo(json.dumps(result.to_dict(), indent=2))
            return
        
        # Table format
        status_emoji = "✅" if result.success_rate == 1.0 else "⚠️" if result.failed_items == 0 else "❌"
        
        click.echo(f"\n{status_emoji} Batch Compilation Results")
        click.echo(f"   Total files: {result.total_items}")
        click.echo(f"   Successful: {result.successful_items}")
        click.echo(f"   Failed: {result.failed_items}")
        click.echo(f"   Success rate: {result.success_rate:.1%}")
        click.echo(f"   Duration: {result.total_duration:.2f}s")
        
        # Show failed items
        failed_items = result.get_failed_items()
        if failed_items:
            click.echo(f"\n❌ Failed Compilations ({len(failed_items)}):")
            for item in failed_items[:10]:  # Show first 10
                click.echo(f"   • {Path(item.schema_path).name}: {item.error}")
            if len(failed_items) > 10:
                click.echo(f"   ... and {len(failed_items) - 10} more")
        
        # Show cache statistics
        if use_cache:
            cache_hits = len([i for i in result.items if i.metadata.get("from_cache", False)])
            if cache_hits > 0:
                click.echo(f"\n💾 Cache hits: {cache_hits}/{result.total_items} ({cache_hits/result.total_items:.1%})")
        
        if result.failed_items > 0:
            sys.exit(1)
        
    except Exception as e:
        click.echo(f"❌ Batch compilation failed: {e}", err=True)
        sys.exit(1)


@compile.command()
@click.argument('schema_files', nargs=-1, required=True, type=click.Path(exists=True))
@click.option('--include-path', 'include_paths', multiple=True,
              help='Include paths for compilation (can be used multiple times)')
@click.option('--output-dir', type=click.Path(), help='Output directory for compiled schemas')
@click.option('--parallel/--sequential', default=True, help='Use parallel processing')
@click.option('--max-workers', type=int, default=4, help='Maximum worker threads')
@click.option('--use-cache/--no-cache', default=True, help='Use schema caching')
@click.option('--show-progress/--no-progress', default=True, help='Show progress')
@click.option('--format', 'output_format', type=click.Choice(['table', 'json']),
              default='table', help='Output format')
@click.pass_context
def files(ctx, schema_files: tuple, include_paths: tuple, output_dir: Optional[str],
          parallel: bool, max_workers: int, use_cache: bool,
          show_progress: bool, output_format: str):
    """Compile specific schema files."""
    try:
        # Initialize compiler
        compiler = BatchCompiler(max_workers=max_workers, use_cache=use_cache)
        
        # Set up progress callback
        progress_callback = None
        if show_progress:
            progress_callback = create_progress_tracker(show_details=True)
        
        # Compile schemas
        click.echo(f"🚀 Compiling {len(schema_files)} schema files...")
        result = compiler.compile_batch(
            schema_paths=list(schema_files),
            include_paths=list(include_paths) if include_paths else None,
            output_dir=output_dir,
            parallel=parallel,
            progress_callback=progress_callback
        )
        
        if show_progress:
            print()  # New line after progress
        
        # Display results
        if output_format == 'json':
            click.echo(json.dumps(result.to_dict(), indent=2))
            return
        
        # Table format
        status_emoji = "✅" if result.success_rate == 1.0 else "⚠️" if result.failed_items == 0 else "❌"
        
        click.echo(f"\n{status_emoji} Batch Compilation Results")
        click.echo(f"   Total files: {result.total_items}")
        click.echo(f"   Successful: {result.successful_items}")
        click.echo(f"   Failed: {result.failed_items}")
        click.echo(f"   Success rate: {result.success_rate:.1%}")
        click.echo(f"   Duration: {result.total_duration:.2f}s")
        
        # Show details for each file
        click.echo(f"\n📄 File Details:")
        for item in result.items:
            status_symbol = "✅" if item.status.value == "completed" else "❌"
            cache_indicator = " (cached)" if item.metadata.get("from_cache", False) else ""
            
            click.echo(f"   {status_symbol} {Path(item.schema_path).name}{cache_indicator}")
            if item.error:
                click.echo(f"      Error: {item.error}")
            elif item.result:
                click.echo(f"      Output: {item.result}")
            if item.duration > 0:
                click.echo(f"      Duration: {item.duration:.3f}s")
        
        if result.failed_items > 0:
            sys.exit(1)
        
    except Exception as e:
        click.echo(f"❌ Batch compilation failed: {e}", err=True)
        sys.exit(1)


# === BATCH REGISTRATION COMMANDS ===

@register.command()
@click.argument('directory', type=click.Path(exists=True, file_okay=False))
@click.option('--pattern', default='*.proto', help='File pattern to match')
@click.option('--recursive/--no-recursive', default=True, help='Search recursively')
@click.option('--schema-registry-url', '-s', required=True, help='Schema Registry URL')
@click.option('--subject-template', default='{schema_name}-value',
              help='Subject template (supports {schema_name})')
@click.option('--compatibility-level', type=click.Choice(['BACKWARD', 'FORWARD', 'FULL', 'NONE']),
              help='Compatibility level to set')
@click.option('--check-compatibility/--no-check', default=True,
              help='Check compatibility before registration')
@click.option('--parallel/--sequential', default=True, help='Use parallel processing')
@click.option('--max-workers', type=int, default=4, help='Maximum worker threads')
@click.option('--show-progress/--no-progress', default=True, help='Show progress')
@click.option('--format', 'output_format', type=click.Choice(['table', 'json']),
              default='table', help='Output format')
@click.pass_context
def directory(ctx, directory: str, pattern: str, recursive: bool,
              schema_registry_url: str, subject_template: str,
              compatibility_level: Optional[str], check_compatibility: bool,
              parallel: bool, max_workers: int, show_progress: bool,
              output_format: str):
    """Register all schemas in a directory."""
    try:
        # Initialize registry manager
        registry_manager = BatchRegistryManager(
            schema_registry_url=schema_registry_url,
            max_workers=max_workers
        )
        
        # Initialize compiler for discovery
        from .batch_operations import BatchCompiler
        compiler = BatchCompiler()
        
        # Discover schemas
        click.echo(f"🔍 Discovering schemas in {directory}...")
        schema_files = compiler.discover_schemas(
            directory=directory,
            pattern=pattern,
            recursive=recursive
        )
        
        if not schema_files:
            click.echo(f"No schema files found matching pattern: {pattern}")
            return
        
        click.echo(f"Found {len(schema_files)} schema files")
        
        # Set up progress callback
        progress_callback = None
        if show_progress:
            progress_callback = create_progress_tracker(show_details=True)
        
        # Register schemas
        click.echo("🚀 Starting batch registration...")
        result = registry_manager.register_schemas_batch(
            schema_files=schema_files,
            subject_template=subject_template,
            compatibility_level=compatibility_level,
            parallel=parallel,
            check_compatibility=check_compatibility,
            progress_callback=progress_callback
        )
        
        if show_progress:
            print()  # New line after progress
        
        # Display results
        if output_format == 'json':
            click.echo(json.dumps(result.to_dict(), indent=2))
            return
        
        # Table format
        status_emoji = "✅" if result.success_rate == 1.0 else "⚠️" if result.failed_items == 0 else "❌"
        
        click.echo(f"\n{status_emoji} Batch Registration Results")
        click.echo(f"   Total files: {result.total_items}")
        click.echo(f"   Successful: {result.successful_items}")
        click.echo(f"   Failed: {result.failed_items}")
        click.echo(f"   Success rate: {result.success_rate:.1%}")
        click.echo(f"   Duration: {result.total_duration:.2f}s")
        
        # Show successful registrations
        successful_items = result.get_successful_items()
        if successful_items:
            click.echo(f"\n✅ Successful Registrations ({len(successful_items)}):")
            for item in successful_items[:10]:  # Show first 10
                schema_id = item.result.get('id', 'unknown') if item.result else 'unknown'
                click.echo(f"   • {item.subject}: Schema ID {schema_id}")
            if len(successful_items) > 10:
                click.echo(f"   ... and {len(successful_items) - 10} more")
        
        # Show failed registrations
        failed_items = result.get_failed_items()
        if failed_items:
            click.echo(f"\n❌ Failed Registrations ({len(failed_items)}):")
            for item in failed_items[:10]:  # Show first 10
                click.echo(f"   • {item.subject}: {item.error}")
            if len(failed_items) > 10:
                click.echo(f"   ... and {len(failed_items) - 10} more")
        
        if result.failed_items > 0:
            sys.exit(1)
        
    except Exception as e:
        click.echo(f"❌ Batch registration failed: {e}", err=True)
        sys.exit(1)


@register.command()
@click.argument('directory', type=click.Path(exists=True, file_okay=False))
@click.option('--pattern', default='*.proto', help='File pattern to match')
@click.option('--recursive/--no-recursive', default=True, help='Search recursively')
@click.option('--schema-registry-url', '-s', required=True, help='Schema Registry URL')
@click.option('--subject-template', default='{schema_name}-value',
              help='Subject template (supports {schema_name})')
@click.option('--parallel/--sequential', default=True, help='Use parallel processing')
@click.option('--max-workers', type=int, default=4, help='Maximum worker threads')
@click.option('--show-progress/--no-progress', default=True, help='Show progress')
@click.option('--format', 'output_format', type=click.Choice(['table', 'json']),
              default='table', help='Output format')
@click.pass_context
def check_compatibility(ctx, directory: str, pattern: str, recursive: bool,
                       schema_registry_url: str, subject_template: str,
                       parallel: bool, max_workers: int, show_progress: bool,
                       output_format: str):
    """Check compatibility for schemas in a directory."""
    try:
        # Initialize registry manager
        registry_manager = BatchRegistryManager(
            schema_registry_url=schema_registry_url,
            max_workers=max_workers
        )
        
        # Initialize compiler for discovery
        from .batch_operations import BatchCompiler
        compiler = BatchCompiler()
        
        # Discover schemas
        click.echo(f"🔍 Discovering schemas in {directory}...")
        schema_files = compiler.discover_schemas(
            directory=directory,
            pattern=pattern,
            recursive=recursive
        )
        
        if not schema_files:
            click.echo(f"No schema files found matching pattern: {pattern}")
            return
        
        click.echo(f"Found {len(schema_files)} schema files")
        
        # Set up progress callback
        progress_callback = None
        if show_progress:
            progress_callback = create_progress_tracker(show_details=True)
        
        # Check compatibility
        click.echo("🔍 Starting batch compatibility check...")
        result = registry_manager.check_compatibility_batch(
            schema_files=schema_files,
            subject_template=subject_template,
            parallel=parallel,
            progress_callback=progress_callback
        )
        
        if show_progress:
            print()  # New line after progress
        
        # Display results
        if output_format == 'json':
            click.echo(json.dumps(result.to_dict(), indent=2))
            return
        
        # Table format
        status_emoji = "✅" if result.success_rate == 1.0 else "⚠️" if result.failed_items == 0 else "❌"
        
        click.echo(f"\n{status_emoji} Batch Compatibility Check Results")
        click.echo(f"   Total files: {result.total_items}")
        click.echo(f"   Successful: {result.successful_items}")
        click.echo(f"   Failed: {result.failed_items}")
        click.echo(f"   Success rate: {result.success_rate:.1%}")
        click.echo(f"   Duration: {result.total_duration:.2f}s")
        
        # Show compatibility results
        for item in result.items:
            if item.result:
                compatible = item.result.get('is_compatible', False)
                symbol = "✅" if compatible else "❌"
                message = item.result.get('message', '')
                click.echo(f"   {symbol} {item.subject}: {message}")
        
        if result.failed_items > 0:
            sys.exit(1)
        
    except Exception as e:
        click.echo(f"❌ Batch compatibility check failed: {e}", err=True)
        sys.exit(1)


# === BATCH VALIDATION COMMANDS ===

@validate.command()
@click.argument('directory', type=click.Path(exists=True, file_okay=False))
@click.option('--pattern', default='*.proto', help='File pattern to match')
@click.option('--recursive/--no-recursive', default=True, help='Search recursively')
@click.option('--level', type=click.Choice(['basic', 'comprehensive', 'strict']),
              default='comprehensive', help='Validation level')
@click.option('--include-path', 'include_paths', multiple=True,
              help='Include paths for validation (can be used multiple times)')
@click.option('--check-dependencies/--no-dependencies', default=True,
              help='Check schema dependencies')
@click.option('--check-naming/--no-naming', default=True,
              help='Check naming conventions')
@click.option('--parallel/--sequential', default=True, help='Use parallel processing')
@click.option('--max-workers', type=int, default=4, help='Maximum worker threads')
@click.option('--show-progress/--no-progress', default=True, help='Show progress')
@click.option('--format', 'output_format', type=click.Choice(['table', 'json']),
              default='table', help='Output format')
@click.pass_context
def schemas(ctx, directory: str, pattern: str, recursive: bool, level: str,
            include_paths: tuple, check_dependencies: bool, check_naming: bool,
            parallel: bool, max_workers: int, show_progress: bool,
            output_format: str):
    """Validate schemas in a directory."""
    try:
        # Initialize validator
        validator = BatchValidator(max_workers=max_workers)
        
        # Map validation level
        validation_level = ValidationLevel(level)
        
        # Initialize compiler for discovery
        from .batch_operations import BatchCompiler
        compiler = BatchCompiler()
        
        # Discover schemas
        click.echo(f"🔍 Discovering schemas in {directory}...")
        schema_files = compiler.discover_schemas(
            directory=directory,
            pattern=pattern,
            recursive=recursive
        )
        
        if not schema_files:
            click.echo(f"No schema files found matching pattern: {pattern}")
            return
        
        click.echo(f"Found {len(schema_files)} schema files")
        
        # Set up progress callback
        progress_callback = None
        if show_progress:
            progress_callback = create_progress_tracker(show_details=True)
        
        # Validate schemas
        click.echo("🔍 Starting batch validation...")
        result = validator.validate_schemas_batch(
            schema_files=schema_files,
            validation_level=validation_level,
            include_paths=list(include_paths) if include_paths else None,
            check_dependencies=check_dependencies,
            check_naming=check_naming,
            parallel=parallel,
            progress_callback=progress_callback
        )
        
        if show_progress:
            print()  # New line after progress
        
        # Display results
        if output_format == 'json':
            click.echo(json.dumps(result.to_dict(), indent=2))
            return
        
        # Table format
        status_emoji = {
            "passed": "✅",
            "warning": "⚠️",
            "failed": "❌"
        }.get(result.overall_status, "❓")
        
        click.echo(f"\n{status_emoji} Batch Validation Results ({result.overall_status.upper()})")
        click.echo(f"   Total files: {result.total_items}")
        click.echo(f"   Passed: {result.successful_items}")
        click.echo(f"   Failed: {result.failed_items}")
        click.echo(f"   Total errors: {result.total_errors}")
        click.echo(f"   Total warnings: {result.total_warnings}")
        click.echo(f"   Success rate: {result.success_rate:.1%}")
        click.echo(f"   Duration: {result.total_duration:.2f}s")
        
        # Show validation details
        failed_items = result.get_failed_items()
        warning_items = result.get_items_with_warnings()
        
        if failed_items:
            click.echo(f"\n❌ Failed Validations ({len(failed_items)}):")
            for item in failed_items[:10]:  # Show first 10
                click.echo(f"   • {Path(item.schema_path).name}: {item.get_error_count()} errors")
                for issue in item.issues[:3]:  # Show first 3 issues
                    if issue.severity == "error":
                        click.echo(f"     - {issue.message}")
                if len(item.issues) > 3:
                    click.echo(f"     ... and {len(item.issues) - 3} more issues")
            if len(failed_items) > 10:
                click.echo(f"   ... and {len(failed_items) - 10} more")
        
        if warning_items:
            click.echo(f"\n⚠️  Items with Warnings ({len(warning_items)}):")
            for item in warning_items[:5]:  # Show first 5
                click.echo(f"   • {Path(item.schema_path).name}: {item.get_warning_count()} warnings")
            if len(warning_items) > 5:
                click.echo(f"   ... and {len(warning_items) - 5} more")
        
        if result.overall_status == "failed":
            sys.exit(1)
        
    except Exception as e:
        click.echo(f"❌ Batch validation failed: {e}", err=True)
        sys.exit(1)


# Add batch commands to main CLI
def register_batch_commands(main_cli):
    """Register batch commands with the main CLI."""
    main_cli.add_command(batch)


if __name__ == '__main__':
    batch()