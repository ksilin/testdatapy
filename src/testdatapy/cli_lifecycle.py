"""CLI commands for topic lifecycle management.

This module provides command-line interface for managing topic lifecycles
including creation, cleanup, and strategy application.
"""

import json
import sys
from pathlib import Path
from typing import List, Optional

import click

from .lifecycle import (
    TopicLifecycleManager, LifecycleConfig, TopicConfig,
    LifecycleAction, CleanupPolicy, get_strategy, list_strategies
)
from .config.loader import AppConfig


@click.group()
@click.pass_context
def lifecycle(ctx):
    """Topic lifecycle management commands."""
    # Ensure context object exists
    if ctx.obj is None:
        ctx.obj = {}


@lifecycle.command()
@click.option('--bootstrap-servers', default='localhost:9092',
              help='Kafka bootstrap servers')
@click.option('--config', type=click.Path(exists=True), 
              help='Configuration file path')
@click.pass_context
def test_connection(ctx, bootstrap_servers: str, config: str):
    """Test connection to Kafka cluster."""
    try:
        # Load configuration if provided
        if config:
            app_config = AppConfig.from_file(config)
            bootstrap_servers = app_config.kafka.bootstrap_servers
        
        manager = TopicLifecycleManager(bootstrap_servers)
        connection_info = manager.validate_kafka_connection()
        
        if connection_info['connected']:
            click.echo(f"✅ Connected to Kafka cluster")
            click.echo(f"   Brokers: {connection_info['broker_count']}")
            click.echo(f"   Topics: {connection_info['topic_count']}")
            if 'cluster_id' in connection_info:
                click.echo(f"   Cluster ID: {connection_info['cluster_id']}")
        else:
            click.echo(f"❌ Failed to connect to Kafka")
            click.echo(f"   Error: {connection_info['error']}")
            sys.exit(1)
    
    except Exception as e:
        click.echo(f"❌ Connection test failed: {e}")
        sys.exit(1)


@lifecycle.command()
@click.option('--bootstrap-servers', default='localhost:9092',
              help='Kafka bootstrap servers')
@click.option('--filter', 'name_filter', help='Filter topics by name pattern')
@click.option('--config', type=click.Path(exists=True), 
              help='Configuration file path')
@click.pass_context
def list_topics(ctx, bootstrap_servers: str, name_filter: Optional[str], config: str):
    """List existing Kafka topics."""
    try:
        # Load configuration if provided
        if config:
            app_config = AppConfig.from_file(config)
            bootstrap_servers = app_config.kafka.bootstrap_servers
        
        manager = TopicLifecycleManager(bootstrap_servers)
        topics = manager.get_existing_topics(name_filter)
        
        if topics:
            click.echo(f"Found {len(topics)} topics:")
            for topic in sorted(topics):
                click.echo(f"  • {topic}")
        else:
            filter_msg = f" matching '{name_filter}'" if name_filter else ""
            click.echo(f"No topics found{filter_msg}")
    
    except Exception as e:
        click.echo(f"❌ Failed to list topics: {e}")
        sys.exit(1)


@lifecycle.command()
@click.argument('config_file', type=click.Path())
@click.option('--bootstrap-servers', default='localhost:9092',
              help='Kafka bootstrap servers')
@click.option('--phase', type=click.Choice(['pre', 'post', 'both']), 
              default='both', help='Lifecycle phase to execute')
@click.option('--dry-run', is_flag=True, 
              help='Preview operations without executing them')
@click.option('--confirm', is_flag=True,
              help='Skip confirmation prompts')
@click.pass_context
def execute(ctx, config_file: str, bootstrap_servers: str, phase: str, 
           dry_run: bool, confirm: bool):
    """Execute lifecycle operations from configuration file."""
    try:
        # Load lifecycle configuration
        config_path = Path(config_file)
        if not config_path.exists():
            click.echo(f"❌ Configuration file not found: {config_file}")
            sys.exit(1)
        
        config = LifecycleConfig.load_from_file(config_file)
        
        # Override dry run mode
        if dry_run:
            config.dry_run_mode = True
        
        # Override confirmation setting
        if confirm:
            config.confirm_destructive_actions = False
        
        # Initialize manager
        manager = TopicLifecycleManager(bootstrap_servers)
        
        # Preview operations if requested
        if dry_run:
            click.echo("🔍 Previewing lifecycle operations...")
            preview = manager.preview_lifecycle_operations(config, phase)
            
            click.echo(f"\nPlanned operations ({preview['total_operations']} total):")
            for action, operations in preview['operations_by_action'].items():
                click.echo(f"\n{action.upper()}:")
                for op in operations:
                    click.echo(f"  • {op['topic']}: {op['message']}")
            
            if preview['validation_issues']:
                click.echo(f"\n⚠️  Validation issues:")
                for issue in preview['validation_issues']:
                    click.echo(f"  • {issue}")
            
            return
        
        # Confirm destructive operations
        if config.confirm_destructive_actions and not confirm:
            destructive_actions = [LifecycleAction.DELETE, LifecycleAction.RECREATE, LifecycleAction.CLEAR_DATA]
            has_destructive = any(
                topic.pre_action in destructive_actions or topic.post_action in destructive_actions
                for topic in config.topics
            )
            
            if has_destructive or config.post_generation_policy in [CleanupPolicy.DELETE_TOPICS, CleanupPolicy.CLEAR_DATA]:
                click.echo("⚠️  This operation includes destructive actions.")
                if not click.confirm("Do you want to continue?"):
                    click.echo("Operation cancelled.")
                    return
        
        # Execute lifecycle operations
        click.echo(f"🚀 Executing {phase} lifecycle operations...")
        result = manager.execute_lifecycle(config, phase)
        
        # Display results
        if result.success:
            click.echo(f"✅ Lifecycle execution completed successfully")
        else:
            click.echo(f"❌ Lifecycle execution completed with errors")
        
        click.echo(f"\nSummary:")
        click.echo(f"  • Total operations: {result.total_operations}")
        click.echo(f"  • Successful: {result.successful_operations}")
        click.echo(f"  • Failed: {result.failed_operations}")
        click.echo(f"  • Skipped: {result.skipped_operations}")
        click.echo(f"  • Duration: {result.total_duration:.2f}s")
        
        # Show failed operations
        if result.failed_operations > 0:
            click.echo(f"\n❌ Failed operations:")
            for failed_result in result.get_failed_results():
                click.echo(f"  • {failed_result.topic_name} ({failed_result.action}): {failed_result.message}")
        
        if not result.success:
            sys.exit(1)
    
    except Exception as e:
        click.echo(f"❌ Execution failed: {e}")
        sys.exit(1)


@lifecycle.command()
@click.argument('strategy_name')
@click.argument('topic_names', nargs=-1, required=True)
@click.option('--output', type=click.Path(), 
              help='Output file for generated configuration')
@click.option('--partitions', type=int, help='Number of partitions')
@click.option('--replication-factor', type=int, help='Replication factor')
@click.option('--cleanup-after', is_flag=True, help='Clean up topics after use')
@click.option('--clear-before', is_flag=True, help='Clear data before use')
@click.option('--show-config', is_flag=True, help='Display configuration instead of saving')
@click.pass_context
def apply_strategy(ctx, strategy_name: str, topic_names: List[str], output: str,
                  partitions: Optional[int], replication_factor: Optional[int],
                  cleanup_after: bool, clear_before: bool, show_config: bool):
    """Apply a predefined lifecycle strategy to topics."""
    try:
        # Build strategy parameters
        strategy_params = {}
        if partitions is not None:
            strategy_params['partitions'] = partitions
        if replication_factor is not None:
            strategy_params['replication_factor'] = replication_factor
        if cleanup_after:
            strategy_params['cleanup_after'] = cleanup_after
        if clear_before:
            strategy_params['clear_before'] = clear_before
        
        # Create dummy manager to apply strategy
        manager = TopicLifecycleManager('localhost:9092')
        config = manager.apply_strategy(strategy_name, list(topic_names), **strategy_params)
        
        # Output configuration
        if show_config:
            click.echo(json.dumps(config.to_dict(), indent=2))
        elif output:
            config.save_to_file(output)
            click.echo(f"✅ Configuration saved to {output}")
        else:
            # Save to default location
            default_output = f"lifecycle-{strategy_name}.json"
            config.save_to_file(default_output)
            click.echo(f"✅ Configuration saved to {default_output}")
    
    except ValueError as e:
        click.echo(f"❌ Strategy error: {e}")
        
        # List available strategies
        strategies = list_strategies()
        click.echo(f"\nAvailable strategies:")
        for name, info in strategies.items():
            click.echo(f"  • {name}: {info['description']}")
        
        sys.exit(1)
    
    except Exception as e:
        click.echo(f"❌ Failed to apply strategy: {e}")
        sys.exit(1)


@lifecycle.command()
@click.pass_context
def list_strategies_cmd(ctx):
    """List available lifecycle strategies."""
    try:
        strategies = list_strategies()
        
        click.echo("Available lifecycle strategies:")
        for name, info in strategies.items():
            click.echo(f"\n• {name}")
            click.echo(f"  Description: {info['description']}")
            click.echo(f"  Type: {info['type']}")
    
    except Exception as e:
        click.echo(f"❌ Failed to list strategies: {e}")
        sys.exit(1)


@lifecycle.command()
@click.argument('config_file', type=click.Path())
@click.option('--bootstrap-servers', default='localhost:9092',
              help='Kafka bootstrap servers')
@click.option('--phase', type=click.Choice(['pre', 'post', 'both']), 
              default='both', help='Lifecycle phase to preview')
@click.pass_context
def preview(ctx, config_file: str, bootstrap_servers: str, phase: str):
    """Preview lifecycle operations without executing them."""
    try:
        # Load configuration
        config_path = Path(config_file)
        if not config_path.exists():
            click.echo(f"❌ Configuration file not found: {config_file}")
            sys.exit(1)
        
        config = LifecycleConfig.load_from_file(config_file)
        manager = TopicLifecycleManager(bootstrap_servers)
        
        # Get preview
        preview = manager.preview_lifecycle_operations(config, phase)
        
        click.echo(f"📋 Lifecycle Operations Preview")
        click.echo(f"Configuration: {config_file}")
        click.echo(f"Phase: {phase}")
        click.echo(f"Environment: {config.environment}")
        
        # Show existing topics
        if preview['existing_topics']:
            click.echo(f"\nExisting topics ({len(preview['existing_topics'])}):")
            for topic in sorted(preview['existing_topics'])[:10]:  # Show first 10
                click.echo(f"  • {topic}")
            if len(preview['existing_topics']) > 10:
                click.echo(f"  ... and {len(preview['existing_topics']) - 10} more")
        
        # Show planned operations
        click.echo(f"\nPlanned operations ({preview['total_operations']} total):")
        for action, operations in preview['operations_by_action'].items():
            click.echo(f"\n{action.upper()} ({len(operations)} operations):")
            for op in operations:
                click.echo(f"  • {op['topic']}")
                if op['message'] != 'DRY RUN: Would execute ' + action:
                    click.echo(f"    {op['message']}")
        
        # Show validation issues
        if preview['validation_issues']:
            click.echo(f"\n⚠️  Configuration issues:")
            for issue in preview['validation_issues']:
                click.echo(f"  • {issue}")
        else:
            click.echo(f"\n✅ Configuration validation passed")
    
    except Exception as e:
        click.echo(f"❌ Preview failed: {e}")
        sys.exit(1)


@lifecycle.command()
@click.option('--output', type=click.Path(), default='lifecycle-config-template.json',
              help='Output file path')
@click.option('--strategy', type=click.Choice(['test_data', 'development', 'production']),
              default='test_data', help='Base strategy for template')
@click.pass_context
def create_template(ctx, output: str, strategy: str):
    """Create a lifecycle configuration template."""
    try:
        # Create sample configuration using strategy
        sample_topics = ['sample-topic-1', 'sample-topic-2', 'sample-topic-3']
        manager = TopicLifecycleManager('localhost:9092')
        config = manager.apply_strategy(strategy, sample_topics)
        
        # Save template
        config.save_to_file(output)
        
        click.echo(f"✅ Lifecycle configuration template created: {output}")
        click.echo(f"   Strategy: {strategy}")
        click.echo(f"   Sample topics: {len(sample_topics)}")
        click.echo(f"\nEdit the configuration file and use 'lifecycle execute' to apply it.")
    
    except Exception as e:
        click.echo(f"❌ Failed to create template: {e}")
        sys.exit(1)


@lifecycle.command()
@click.argument('config_file', type=click.Path(exists=True))
@click.pass_context
def validate_config(ctx, config_file: str):
    """Validate a lifecycle configuration file."""
    try:
        config = LifecycleConfig.load_from_file(config_file)
        issues = config.validate()
        
        if issues:
            click.echo(f"❌ Configuration validation failed:")
            for issue in issues:
                click.echo(f"  • {issue}")
            sys.exit(1)
        else:
            click.echo(f"✅ Configuration is valid")
            click.echo(f"   Topics: {len(config.topics)}")
            click.echo(f"   Environment: {config.environment}")
            click.echo(f"   Pre-generation policy: {config.pre_generation_policy.value}")
            click.echo(f"   Post-generation policy: {config.post_generation_policy.value}")
    
    except Exception as e:
        click.echo(f"❌ Validation failed: {e}")
        sys.exit(1)


# Add lifecycle commands to main CLI
def register_lifecycle_commands(main_cli):
    """Register lifecycle commands with the main CLI."""
    main_cli.add_command(lifecycle)


if __name__ == '__main__':
    lifecycle()