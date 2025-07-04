"""CLI commands for Kafka topic management."""
import json
import time
import click
from typing import Dict, Any, Optional
from pathlib import Path

from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, RESOURCE_TOPIC
from confluent_kafka import KafkaException

from testdatapy.config.correlation_config import CorrelationConfig


def create_admin_client(correlation_config: CorrelationConfig, producer_config_path: Optional[str] = None) -> AdminClient:
    """Create and configure a Kafka AdminClient from CorrelationConfig and optional producer config.
    
    Args:
        correlation_config: Configuration containing topic definitions
        producer_config_path: Optional path to producer configuration JSON file
        
    Returns:
        Configured AdminClient instance
        
    Raises:
        ValueError: If required configuration is missing
        KafkaException: If AdminClient creation fails
    """
    admin_config = {}
    
    # Method 1: Use producer config file (matches correlated generate command)
    if producer_config_path:
        try:
            with open(producer_config_path, 'r') as f:
                producer_config = json.load(f)
            
            # Convert producer config to AdminClient format
            # Producer config already uses dot notation (bootstrap.servers)
            admin_config = producer_config.copy()
            
            # Ensure bootstrap servers are present
            if "bootstrap.servers" not in admin_config:
                raise ValueError("bootstrap.servers not found in producer configuration")
                
        except FileNotFoundError:
            raise ValueError(f"Producer configuration file not found: {producer_config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in producer configuration file: {e}")
    
    # Method 2: Use embedded kafka config (legacy support)
    else:
        kafka_config = correlation_config.config.get("kafka")
        if not kafka_config:
            raise ValueError("Kafka configuration not found in config file and no --producer-config specified")
        
        bootstrap_servers = kafka_config.get("bootstrap_servers")
        if not bootstrap_servers:
            raise ValueError("bootstrap_servers not found in Kafka configuration")
        
        # Map configuration keys to AdminClient format (underscore to dot notation)
        key_mapping = {
            "bootstrap_servers": "bootstrap.servers",
            "security_protocol": "security.protocol",
            "sasl_mechanism": "sasl.mechanism",
            "sasl_username": "sasl.username",
            "sasl_password": "sasl.password",
            "ssl_ca_location": "ssl.ca.location",
            "ssl_certificate_location": "ssl.certificate.location",
            "ssl_key_location": "ssl.key.location",
            "ssl_key_password": "ssl.key.password",
            "request_timeout_ms": "request.timeout.ms",
            "api_version_request": "api.version.request",
            "socket_timeout_ms": "socket.timeout.ms"
        }
        
        # Convert and filter relevant configuration
        for original_key, admin_key in key_mapping.items():
            if original_key in kafka_config:
                admin_config[admin_key] = kafka_config[original_key]
    
    try:
        return AdminClient(admin_config)
    except Exception as e:
        raise KafkaException(f"Failed to create AdminClient: {e}")


@click.group()
@click.option('--config', '-c', 
              type=click.Path(exists=True, readable=True),
              help='Path to configuration file')
@click.option('--producer-config', '-p',
              type=click.Path(exists=True, readable=True),
              help='Path to producer configuration JSON file (same as correlated generate)')
@click.option('--verbose', '-v', is_flag=True, 
              help='Enable verbose output')
@click.pass_context
def topics(ctx, config, producer_config, verbose):
    """Manage Kafka topics for testdatapy.
    
    This command group provides tools to create, manage, and cleanup
    Kafka topics based on configurations defined in your testdatapy
    configuration files. Supports the same --producer-config option
    as the correlated generate command.
    """
    # Ensure context object exists
    ctx.ensure_object(dict)
    
    # Store common options in context
    ctx.obj['config_path'] = config
    ctx.obj['producer_config_path'] = producer_config
    ctx.obj['verbose'] = verbose
    
    if verbose:
        click.echo("Verbose mode enabled")
        if producer_config:
            click.echo(f"Using producer config: {producer_config}")


@topics.command()
@click.option('--dry-run', is_flag=True,
              help='Show what would be created without actually creating topics')
@click.option('--partitions', type=int,
              help='Override default partition count for all topics')
@click.option('--replication-factor', type=int,
              help='Override default replication factor for all topics')
@click.pass_context
def create(ctx, dry_run, partitions, replication_factor):
    """Create Kafka topics defined in configuration.
    
    Creates topics for master data and transactional data as defined
    in the configuration file. Topic settings like partitions and
    replication factor can be specified in the config or overridden
    via command line options.
    """
    config_path = ctx.obj.get('config_path')
    producer_config_path = ctx.obj.get('producer_config_path')
    verbose = ctx.obj.get('verbose', False)
    
    if not config_path:
        click.echo("Error: Configuration file is required for topic creation", err=True)
        click.echo("Use --config option to specify configuration file", err=True)
        ctx.exit(1)
    
    try:
        # Load configuration (support both JSON and YAML like correlated generate)
        if verbose:
            click.echo(f"Loading configuration from: {config_path}")
        
        if config_path.endswith('.yaml') or config_path.endswith('.yml'):
            # Use CorrelationConfig.from_yaml_file like correlated generate
            correlation_config = CorrelationConfig.from_yaml_file(config_path)
        else:
            # Legacy JSON support
            with open(config_path, 'r') as f:
                config_dict = json.load(f)
            correlation_config = CorrelationConfig(config_dict)
        
        # Create AdminClient with producer config support
        if verbose:
            click.echo("Creating Kafka AdminClient...")
            if producer_config_path:
                click.echo(f"Using producer config: {producer_config_path}")
        
        admin_client = create_admin_client(correlation_config, producer_config_path)
        
        # Extract topic configurations
        topics_to_create = []
        
        # Master data topics
        master_data = correlation_config.config.get("master_data", {})
        for entity_name, entity_config in master_data.items():
            topic_name = entity_config.get("kafka_topic")
            if topic_name:
                topic_partitions = partitions or entity_config.get("partitions", 1)
                topic_replication = replication_factor or entity_config.get("replication_factor", 1)
                
                topics_to_create.append(NewTopic(
                    topic=topic_name,
                    num_partitions=topic_partitions,
                    replication_factor=topic_replication
                ))
                
                if verbose:
                    click.echo(f"  Master data topic: {topic_name} "
                              f"(partitions: {topic_partitions}, replication: {topic_replication})")
        
        # Transactional data topics
        transactional_data = correlation_config.config.get("transactional_data", {})
        for entity_name, entity_config in transactional_data.items():
            topic_name = entity_config.get("kafka_topic")
            if topic_name:
                topic_partitions = partitions or entity_config.get("partitions", 1)
                topic_replication = replication_factor or entity_config.get("replication_factor", 1)
                
                topics_to_create.append(NewTopic(
                    topic=topic_name,
                    num_partitions=topic_partitions,
                    replication_factor=topic_replication
                ))
                
                if verbose:
                    click.echo(f"  Transactional data topic: {topic_name} "
                              f"(partitions: {topic_partitions}, replication: {topic_replication})")
        
        if not topics_to_create:
            click.echo("No topics found in configuration to create")
            return
        
        if dry_run:
            click.echo("DRY RUN - Would create the following topics:")
            for topic in topics_to_create:
                click.echo(f"  - {topic.topic} (partitions: {topic.num_partitions}, "
                          f"replication: {topic.replication_factor})")
            return
        
        # Create topics
        click.echo(f"Creating {len(topics_to_create)} topics...")
        
        # Create topics and wait for completion
        fs = admin_client.create_topics(topics_to_create, request_timeout=30)
        
        # Wait for each topic creation to complete
        for topic, f in fs.items():
            try:
                f.result(timeout=60)  # The result itself is None
                click.echo(f"✅ Created topic: {topic}")
            except Exception as e:
                if "already exists" in str(e).lower():
                    click.echo(f"⚠️  Topic already exists: {topic}")
                else:
                    click.echo(f"❌ Failed to create topic {topic}: {e}", err=True)
        
        click.echo("Topic creation completed")
        
    except FileNotFoundError:
        click.echo(f"Error: Configuration file not found: {config_path}", err=True)
        ctx.exit(1)
    except json.JSONDecodeError as e:
        click.echo(f"Error: Invalid JSON in configuration file: {e}", err=True)
        ctx.exit(1)
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        ctx.exit(1)
    except KafkaException as e:
        click.echo(f"Error: Kafka operation failed: {e}", err=True)
        ctx.exit(1)
    except Exception as e:
        click.echo(f"Error: Unexpected error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        ctx.exit(1)


@topics.command()
@click.option('--dry-run', is_flag=True,
              help='Show what would be cleaned up without actually deleting')
@click.option('--confirm', is_flag=True,
              help='Skip confirmation prompts')
@click.option('--strategy', type=click.Choice(['truncate', 'delete']), default='truncate',
              help='Cleanup strategy: truncate (clear data but keep topics, default) or delete (remove topics entirely)')
@click.pass_context
def cleanup(ctx, dry_run, confirm, strategy):
    """Clean up Kafka topics defined in configuration.
    
    Two cleanup strategies are available:
    - truncate: Clears all data but keeps topics (default - true cleanup)
    - delete: Permanently removes topics entirely
    
    By default, this performs a "truncate" which clears topic data while
    preserving topic configuration and structure. Use --strategy=delete
    only when you want to completely remove topics.
    """
    config_path = ctx.obj.get('config_path')
    producer_config_path = ctx.obj.get('producer_config_path')
    verbose = ctx.obj.get('verbose', False)
    
    if not config_path:
        click.echo("Error: Configuration file is required for topic cleanup", err=True)
        click.echo("Use --config option to specify configuration file", err=True)
        ctx.exit(1)
    
    try:
        # Load configuration (support both JSON and YAML like correlated generate)
        if verbose:
            click.echo(f"Loading configuration from: {config_path}")
        
        if config_path.endswith('.yaml') or config_path.endswith('.yml'):
            # Use CorrelationConfig.from_yaml_file like correlated generate
            correlation_config = CorrelationConfig.from_yaml_file(config_path)
        else:
            # Legacy JSON support
            with open(config_path, 'r') as f:
                config_dict = json.load(f)
            correlation_config = CorrelationConfig(config_dict)
        
        # Create AdminClient with producer config support
        if verbose:
            click.echo("Creating Kafka AdminClient...")
            if producer_config_path:
                click.echo(f"Using producer config: {producer_config_path}")
        
        admin_client = create_admin_client(correlation_config, producer_config_path)
        
        # Extract topic names to delete
        topics_to_delete = []
        
        # Master data topics
        master_data = correlation_config.config.get("master_data", {})
        for entity_name, entity_config in master_data.items():
            topic_name = entity_config.get("kafka_topic")
            if topic_name and topic_name not in topics_to_delete:
                topics_to_delete.append(topic_name)
                if verbose:
                    click.echo(f"  Master data topic to delete: {topic_name}")
        
        # Transactional data topics
        transactional_data = correlation_config.config.get("transactional_data", {})
        for entity_name, entity_config in transactional_data.items():
            topic_name = entity_config.get("kafka_topic")
            if topic_name and topic_name not in topics_to_delete:
                topics_to_delete.append(topic_name)
                if verbose:
                    click.echo(f"  Transactional data topic to delete: {topic_name}")
        
        if not topics_to_delete:
            click.echo("No topics found in configuration to cleanup")
            return
        
        # For delete strategy, we can show dry-run immediately
        if strategy == 'delete':
            if dry_run:
                click.echo("DRY RUN - Would permanently delete the following topics:")
                for topic in topics_to_delete:
                    click.echo(f"  - {topic}")
                return
            
            # Confirmation prompt for delete
            if not confirm:
                click.echo(f"⚠️  This will permanently DELETE {len(topics_to_delete)} topics (topics will be removed entirely):")
                for topic in topics_to_delete:
                    click.echo(f"  - {topic}")
                
                if not click.confirm("Are you sure you want to proceed?"):
                    click.echo("Operation cancelled")
                    return
        
        if strategy == 'delete':
            # Delete topics permanently
            click.echo(f"Deleting {len(topics_to_delete)} topics...")
            
            # Delete topics and wait for completion
            fs = admin_client.delete_topics(topics_to_delete, request_timeout=30)
            
            # Wait for each topic deletion to complete
            for topic, f in fs.items():
                try:
                    f.result(timeout=60)  # The result itself is None
                    click.echo(f"✅ Deleted topic: {topic}")
                except Exception as e:
                    if "does not exist" in str(e).lower():
                        click.echo(f"⚠️  Topic does not exist: {topic}")
                    else:
                        click.echo(f"❌ Failed to delete topic {topic}: {e}", err=True)
            
            click.echo("Topic deletion completed")
            
        else:  # truncate strategy
            # Ensure topics exist and are clean (idempotent cleanup)
            click.echo(f"Cleaning up {len(topics_to_delete)} topics...")
            
            # First, get metadata for existing topics to preserve their configuration
            if verbose:
                click.echo("Fetching topic metadata...")
            
            metadata = admin_client.list_topics(timeout=30)
            existing_topics = metadata.topics
            
            topics_to_delete_and_recreate = []
            topics_to_create_new = []
            
            # Build topic recreation/creation configs from our configuration
            topic_configs = {}
            
            # Get topic configs from master data
            master_data = correlation_config.config.get("master_data", {})
            for entity_name, entity_config in master_data.items():
                topic_name = entity_config.get("kafka_topic")
                if topic_name and topic_name in topics_to_delete:
                    topic_configs[topic_name] = {
                        'partitions': entity_config.get("partitions", 1),
                        'replication_factor': entity_config.get("replication_factor", 1)
                    }
            
            # Get topic configs from transactional data
            transactional_data = correlation_config.config.get("transactional_data", {})
            for entity_name, entity_config in transactional_data.items():
                topic_name = entity_config.get("kafka_topic")
                if topic_name and topic_name in topics_to_delete:
                    topic_configs[topic_name] = {
                        'partitions': entity_config.get("partitions", 1),
                        'replication_factor': entity_config.get("replication_factor", 1)
                    }
            
            # Categorize topics: existing (to truncate) vs missing (to create)
            for topic_name in topics_to_delete:
                if topic_name in existing_topics:
                    # Topic exists - will delete and recreate to clear data
                    topic_metadata = existing_topics[topic_name]
                    partition_count = len(topic_metadata.partitions)
                    
                    if partition_count > 0:
                        # Preserve existing config unless overridden in our config
                        if topic_name in topic_configs:
                            # Use config from our YAML/JSON
                            partitions = topic_configs[topic_name]['partitions']
                            replication = topic_configs[topic_name]['replication_factor']
                        else:
                            # Preserve existing config
                            first_partition = list(topic_metadata.partitions.values())[0]
                            partitions = partition_count
                            replication = len(first_partition.replicas)
                        
                        topics_to_delete_and_recreate.append(NewTopic(
                            topic=topic_name,
                            num_partitions=partitions,
                            replication_factor=replication
                        ))
                        
                        if verbose:
                            click.echo(f"  Will truncate {topic_name} (delete + recreate) with {partitions} partitions, "
                                      f"replication factor {replication}")
                    else:
                        click.echo(f"⚠️  Topic {topic_name} has no partitions, skipping")
                else:
                    # Topic doesn't exist - will create it clean
                    if topic_name in topic_configs:
                        partitions = topic_configs[topic_name]['partitions']
                        replication = topic_configs[topic_name]['replication_factor']
                    else:
                        # Use defaults if not specified in config
                        partitions = 1
                        replication = 1
                    
                    topics_to_create_new.append(NewTopic(
                        topic=topic_name,
                        num_partitions=partitions,
                        replication_factor=replication
                    ))
                    
                    if verbose:
                        click.echo(f"  Will create {topic_name} (doesn't exist) with {partitions} partitions, "
                                  f"replication factor {replication}")
            
            if not topics_to_delete_and_recreate and not topics_to_create_new:
                click.echo("No topics to clean up")
                return
            
            # Dry-run and confirmation for truncate strategy
            if dry_run:
                click.echo("DRY RUN - Would perform the following cleanup actions:")
                if topics_to_delete_and_recreate:
                    click.echo("  Truncate (delete + recreate to clear data):")
                    for topic in topics_to_delete_and_recreate:
                        click.echo(f"    - {topic.topic}")
                if topics_to_create_new:
                    click.echo("  Create (topics don't exist):")
                    for topic in topics_to_create_new:
                        click.echo(f"    - {topic.topic}")
                return
            
            # Confirmation prompt for truncate
            if not confirm:
                click.echo("🧹 This will perform CLEANUP (idempotent - ensure topics exist and are clean):")
                if topics_to_delete_and_recreate:
                    click.echo(f"  Truncate {len(topics_to_delete_and_recreate)} existing topics (clear data, preserve structure):")
                    for topic in topics_to_delete_and_recreate:
                        click.echo(f"    - {topic.topic}")
                if topics_to_create_new:
                    click.echo(f"  Create {len(topics_to_create_new)} missing topics (clean):")
                    for topic in topics_to_create_new:
                        click.echo(f"    - {topic.topic}")
                
                if not click.confirm("Are you sure you want to proceed?"):
                    click.echo("Operation cancelled")
                    return
            
            # Phase 1: Delete existing topics (to clear their data)
            deletion_errors = []
            if topics_to_delete_and_recreate:
                if verbose:
                    click.echo("Phase 1: Deleting existing topics to clear data...")
                
                topics_to_delete_names = [topic.topic for topic in topics_to_delete_and_recreate]
                fs_delete = admin_client.delete_topics(topics_to_delete_names, request_timeout=30)
                
                # Wait for deletions to complete
                for topic, f in fs_delete.items():
                    try:
                        f.result(timeout=60)
                        if verbose:
                            click.echo(f"  Deleted topic: {topic}")
                    except Exception as e:
                        deletion_errors.append(f"Failed to delete {topic}: {e}")
                        if verbose:
                            click.echo(f"  ❌ Failed to delete topic {topic}: {e}")
                
                if deletion_errors:
                    click.echo("Some topic deletions failed, will continue with creation", err=True)
                    for error in deletion_errors:
                        click.echo(f"  {error}", err=True)
                
                # Wait a bit for deletion to propagate
                if verbose:
                    click.echo("Waiting for deletion to propagate...")
                time.sleep(2)
            
            # Phase 2: Create/recreate all topics
            all_topics_to_create = topics_to_delete_and_recreate + topics_to_create_new
            
            if all_topics_to_create:
                if verbose:
                    click.echo("Phase 2: Creating/recreating topics...")
                
                fs_create = admin_client.create_topics(all_topics_to_create, request_timeout=30)
                
                # Wait for creations to complete
                for topic, f in fs_create.items():
                    try:
                        f.result(timeout=60)
                        if topic in [t.topic for t in topics_to_create_new]:
                            click.echo(f"✅ Created topic: {topic} (new clean topic)")
                        else:
                            click.echo(f"✅ Truncated topic: {topic} (data cleared, topic structure preserved)")
                    except Exception as e:
                        click.echo(f"❌ Failed to create topic {topic}: {e}", err=True)
            
            created_count = len(topics_to_create_new)
            truncated_count = len(topics_to_delete_and_recreate)
            
            if created_count > 0 and truncated_count > 0:
                click.echo(f"🧹 Cleanup completed - {truncated_count} topics truncated, {created_count} topics created")
            elif created_count > 0:
                click.echo(f"🧹 Cleanup completed - {created_count} new topics created")
            elif truncated_count > 0:
                click.echo(f"🧹 Cleanup completed - {truncated_count} topics truncated")
            else:
                click.echo("🧹 Cleanup completed - no changes needed")
        
    except FileNotFoundError:
        click.echo(f"Error: Configuration file not found: {config_path}", err=True)
        ctx.exit(1)
    except json.JSONDecodeError as e:
        click.echo(f"Error: Invalid JSON in configuration file: {e}", err=True)
        ctx.exit(1)
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        ctx.exit(1)
    except KafkaException as e:
        click.echo(f"Error: Kafka operation failed: {e}", err=True)
        ctx.exit(1)
    except Exception as e:
        click.echo(f"Error: Unexpected error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        ctx.exit(1)


@topics.command('list')
@click.option('--show-config', is_flag=True,
              help='Show topic configuration details')
@click.pass_context
def list_topics(ctx, show_config):
    """List Kafka topics and their status.
    
    Shows the status of topics defined in the configuration file,
    including whether they exist, their partition count, and
    replication factor.
    """
    config_path = ctx.obj.get('config_path')
    producer_config_path = ctx.obj.get('producer_config_path')
    verbose = ctx.obj.get('verbose', False)
    
    if not config_path:
        click.echo("Error: Configuration file is required for topic listing", err=True)
        click.echo("Use --config option to specify configuration file", err=True)
        ctx.exit(1)
    
    try:
        # Load configuration (support both JSON and YAML like correlated generate)
        if verbose:
            click.echo(f"Loading configuration from: {config_path}")
        
        if config_path.endswith('.yaml') or config_path.endswith('.yml'):
            # Use CorrelationConfig.from_yaml_file like correlated generate
            correlation_config = CorrelationConfig.from_yaml_file(config_path)
        else:
            # Legacy JSON support
            with open(config_path, 'r') as f:
                config_dict = json.load(f)
            correlation_config = CorrelationConfig(config_dict)
        
        # Create AdminClient with producer config support
        if verbose:
            click.echo("Creating Kafka AdminClient...")
            if producer_config_path:
                click.echo(f"Using producer config: {producer_config_path}")
        
        admin_client = create_admin_client(correlation_config, producer_config_path)
        
        # Get cluster metadata
        if verbose:
            click.echo("Fetching cluster metadata...")
        
        metadata = admin_client.list_topics(timeout=30)
        existing_topics = set(metadata.topics.keys())
        
        # Extract configured topics
        configured_topics = {}
        
        # Master data topics
        master_data = correlation_config.config.get("master_data", {})
        for entity_name, entity_config in master_data.items():
            topic_name = entity_config.get("kafka_topic")
            if topic_name:
                configured_topics[topic_name] = {
                    "type": "master_data",
                    "entity": entity_name,
                    "config": entity_config
                }
        
        # Transactional data topics
        transactional_data = correlation_config.config.get("transactional_data", {})
        for entity_name, entity_config in transactional_data.items():
            topic_name = entity_config.get("kafka_topic")
            if topic_name:
                configured_topics[topic_name] = {
                    "type": "transactional_data",
                    "entity": entity_name,
                    "config": entity_config
                }
        
        if not configured_topics:
            click.echo("No topics found in configuration")
            return
        
        # Display topic status
        click.echo(f"Topic Status (Total: {len(configured_topics)}):")
        click.echo("-" * 60)
        
        for topic_name, topic_info in configured_topics.items():
            exists = topic_name in existing_topics
            status_icon = "✅" if exists else "❌"
            
            click.echo(f"{status_icon} {topic_name}")
            click.echo(f"    Type: {topic_info['type']}")
            click.echo(f"    Entity: {topic_info['entity']}")
            
            if exists:
                topic_metadata = metadata.topics[topic_name]
                partition_count = len(topic_metadata.partitions)
                click.echo(f"    Partitions: {partition_count}")
                
                if partition_count > 0:
                    # Get replication factor from first partition
                    first_partition = list(topic_metadata.partitions.values())[0]
                    replication_factor = len(first_partition.replicas)
                    click.echo(f"    Replication Factor: {replication_factor}")
                
                if show_config:
                    # Get topic configuration
                    try:
                        config_resource = ConfigResource(RESOURCE_TOPIC, topic_name)
                        configs = admin_client.describe_configs([config_resource])
                        
                        for resource, config_future in configs.items():
                            config_result = config_future.result(timeout=10)
                            click.echo(f"    Configuration:")
                            for config_name, config_entry in config_result.items():
                                if not config_entry.is_default:
                                    click.echo(f"      {config_name}: {config_entry.value}")
                    except Exception as e:
                        if verbose:
                            click.echo(f"    Warning: Could not fetch config: {e}")
            else:
                click.echo(f"    Status: Topic does not exist")
                
                # Show configured settings
                config = topic_info['config']
                if 'partitions' in config:
                    click.echo(f"    Configured Partitions: {config['partitions']}")
                if 'replication_factor' in config:
                    click.echo(f"    Configured Replication Factor: {config['replication_factor']}")
            
            click.echo()  # Empty line for readability
        
    except FileNotFoundError:
        click.echo(f"Error: Configuration file not found: {config_path}", err=True)
        ctx.exit(1)
    except json.JSONDecodeError as e:
        click.echo(f"Error: Invalid JSON in configuration file: {e}", err=True)
        ctx.exit(1)
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        ctx.exit(1)
    except KafkaException as e:
        click.echo(f"Error: Kafka operation failed: {e}", err=True)
        ctx.exit(1)
    except Exception as e:
        click.echo(f"Error: Unexpected error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        ctx.exit(1)


if __name__ == '__main__':
    topics()