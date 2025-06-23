"""CLI commands for correlated data generation."""
import click
import sys
from pathlib import Path
import yaml

from testdatapy.generators import ReferencePool, CorrelatedDataGenerator
from testdatapy.generators.master_data_generator import MasterDataGenerator
from testdatapy.config.correlation_config import CorrelationConfig
from testdatapy.producers import JsonProducer
from testdatapy.producers.protobuf_producer import ProtobufProducer
from testdatapy.schemas.schema_loader import get_protobuf_class_for_entity, fallback_to_hardcoded_mapping


@click.group()
def correlated():
    """Commands for correlated data generation."""
    pass


@correlated.command()
@click.option('--config', '-c', required=True, help='Path to correlation config YAML file')
@click.option('--bootstrap-servers', '-b', default='localhost:9092', help='Kafka bootstrap servers')
@click.option('--producer-config', '-p', help='Path to producer config JSON file')
@click.option('--dry-run', is_flag=True, help='Print data without producing to Kafka')
@click.option('--master-only', is_flag=True, help='Only load master data')
@click.option('--transaction-only', is_flag=True, help='Only generate transactional data')
@click.option('--format', '-f', type=click.Choice(['json', 'protobuf']), default='json', help='Output format')
@click.option('--schema-registry-url', '-s', help='Schema Registry URL (required for protobuf)')
@click.option('--clean-topics', is_flag=True, help='Clean topics before generation')
def generate(config, bootstrap_servers, producer_config, dry_run, master_only, transaction_only, format, schema_registry_url, clean_topics):
    """Generate correlated test data based on configuration."""
    
    # Load configuration
    try:
        correlation_config = CorrelationConfig.from_yaml_file(config)
    except Exception as e:
        click.echo(f"Error loading configuration: {e}", err=True)
        sys.exit(1)
    
    # Create reference pool
    ref_pool = ReferencePool()
    ref_pool.enable_stats()
    
    # Validate protobuf requirements
    if format == 'protobuf' and not schema_registry_url:
        click.echo("Error: Protobuf format requires --schema-registry-url", err=True)
        sys.exit(1)
    
    # Setup producer if not dry run
    producer = None
    topic_producers = {}  # Store per-topic producers for protobuf
    
    if not dry_run:
        try:
            kafka_config = {"bootstrap.servers": bootstrap_servers}
            
            if producer_config:
                with open(producer_config, 'r') as f:
                    import json
                    additional_config = json.load(f)
                    kafka_config.update(additional_config)
            
            # Extract bootstrap_servers and pass remaining as config
            servers = kafka_config.pop("bootstrap.servers", bootstrap_servers)
            
            if format == 'json':
                # We need a dummy topic for the producer - will be overridden per message
                producer = JsonProducer(
                    bootstrap_servers=servers,
                    topic="dummy",  # Will be overridden in produce calls
                    config=kafka_config
                )
            elif format == 'protobuf':
                # For protobuf, we'll create producers per topic later
                # as we need specific protobuf classes for each entity type
                pass
                
        except Exception as e:
            click.echo(f"Error creating producer: {e}", err=True)
            sys.exit(1)
    
    # Phase 0: Clean topics if requested
    if clean_topics and not dry_run:
        click.echo("Cleaning topics...")
        try:
            from testdatapy.topics import TopicManager
            
            # Get all topics from configuration
            topics_to_clean = []
            
            # Master data topics
            for entity_config in correlation_config.config.get("master_data", {}).values():
                topic = entity_config.get("kafka_topic")
                if topic:
                    topics_to_clean.append(topic)
            
            # Transactional data topics
            for entity_config in correlation_config.config.get("transactional_data", {}).values():
                topic = entity_config.get("kafka_topic") 
                if topic:
                    topics_to_clean.append(topic)
            
            if topics_to_clean:
                # Create topic manager
                kafka_config = {"bootstrap.servers": bootstrap_servers}
                if producer_config:
                    with open(producer_config, 'r') as f:
                        import json
                        additional_config = json.load(f)
                        kafka_config.update(additional_config)
                
                topic_manager = TopicManager(bootstrap_servers, kafka_config)
                
                # Delete topics
                for topic in topics_to_clean:
                    try:
                        if topic_manager.topic_exists(topic):
                            topic_manager.delete_topic(topic)
                            click.echo(f"  ✓ Deleted topic: {topic}")
                        else:
                            click.echo(f"  - Topic not found: {topic}")
                    except Exception as e:
                        click.echo(f"  ✗ Failed to delete {topic}: {e}")
                
                click.echo(f"Topic cleanup completed for {len(topics_to_clean)} topics")
            else:
                click.echo("No topics found in configuration to clean")
                
        except Exception as e:
            click.echo(f"Error during topic cleanup: {e}", err=True)
            # Don't exit - continue with generation
    
    # Phase 1: Load master data
    if not transaction_only:
        click.echo("Loading master data...")
        master_gen = MasterDataGenerator(
            config=correlation_config,
            reference_pool=ref_pool,
            producer=producer
        )
        
        try:
            master_gen.load_all()
            
            # Show summary
            for entity_type in correlation_config.config.get("master_data", {}):
                count = ref_pool.get_type_count(entity_type)
                click.echo(f"  {entity_type}: {count} records")
                
                # Show sample if dry run
                if dry_run:
                    sample = master_gen.get_sample(entity_type, count=3)
                    for record in sample:
                        click.echo(f"    {record}")
            
            # Produce if not dry run
            if not dry_run:
                if format == 'json':
                    master_gen.produce_all()
                elif format == 'protobuf':
                    # Produce master data with protobuf using dynamic loading
                    for entity_type, entity_config in correlation_config.config.get("master_data", {}).items():
                        topic = entity_config.get("kafka_topic")
                        # Use consistent key field priority logic for master data
                        key_field = correlation_config.get_key_field(entity_type, is_master=True)
                        
                        # Try to get protobuf class using dynamic loading
                        proto_class = None
                        try:
                            # First try configuration-based loading
                            proto_class = get_protobuf_class_for_entity(entity_config, entity_type)
                            
                            # If no config-based class found, try hardcoded mapping fallback
                            if proto_class is None:
                                proto_class = fallback_to_hardcoded_mapping(entity_type)
                            
                            if proto_class is None:
                                click.echo(f"Warning: No protobuf class found for {entity_type}, using JSON", err=True)
                                continue
                                
                        except Exception as e:
                            click.echo(f"❌ Error loading protobuf class for {entity_type}: {e}", err=True)
                            if hasattr(e, '__class__') and 'ModuleNotFoundError' in str(e.__class__):
                                click.echo(f"💡 Hint: Ensure protobuf module is compiled with: protoc --python_out=. your_schema.proto", err=True)
                                click.echo(f"💡 Or specify 'schema_path' in configuration to use custom location", err=True)
                            click.echo(f"⚠️  Falling back to JSON for {entity_type}", err=True)
                            continue
                        
                        if topic not in topic_producers:
                            topic_producers[topic] = ProtobufProducer(
                                bootstrap_servers=servers,
                                topic=topic,
                                schema_registry_url=schema_registry_url,
                                schema_proto_class=proto_class,
                                config=kafka_config.copy(),
                                key_field=key_field,
                                auto_create_topic=True
                            )
                        
                        # Produce each record
                        data = master_gen.loaded_data.get(entity_type, [])
                        for record in data:
                            topic_producers[topic].produce(record)
                        
                        topic_producers[topic].flush()
                
                click.echo("Master data produced to Kafka")
                
        except Exception as e:
            click.echo(f"Error loading master data: {e}", err=True)
            sys.exit(1)
    
    # Phase 2: Generate transactional data
    if not master_only:
        click.echo("\nGenerating transactional data...")
        
        for entity_type, entity_config in correlation_config.config.get("transactional_data", {}).items():
            click.echo(f"  Generating {entity_type}...")
            
            try:
                generator = CorrelatedDataGenerator(
                    entity_type=entity_type,
                    config=correlation_config,
                    reference_pool=ref_pool,
                    rate_per_second=entity_config.get("rate_per_second"),
                    max_messages=entity_config.get("max_messages")
                )
                
                # Track recent items if needed
                if entity_config.get("track_recent", False):
                    ref_pool.enable_recent_tracking(entity_type, window_size=1000)
                
                count = 0
                for record in generator.generate():
                    count += 1
                    
                    if dry_run:
                        click.echo(f"    {record}")
                        if count >= 5:  # Show only first 5 in dry run
                            break
                    else:
                        topic = entity_config.get("kafka_topic")
                        # Use consistent key field priority logic
                        key_field = correlation_config.get_key_field(entity_type, is_master=False)
                        key = record.get(key_field)
                        
                        if format == 'json':
                            # Create topic-specific producer if needed
                            if not hasattr(producer, '_topic_producers'):
                                producer._topic_producers = {}
                            
                            if topic not in producer._topic_producers:
                                producer._topic_producers[topic] = JsonProducer(
                                    bootstrap_servers=producer.bootstrap_servers,
                                    topic=topic,
                                    config=producer.config,
                                    auto_create_topic=True
                                )
                            
                            producer._topic_producers[topic].produce(key=key, value=record)
                        
                        elif format == 'protobuf':
                            # Create protobuf producer for this entity type if needed
                            if topic not in topic_producers:
                                # Get protobuf class using dynamic loading
                                proto_class = None
                                try:
                                    # First try configuration-based loading
                                    proto_class = get_protobuf_class_for_entity(entity_config, entity_type)
                                    
                                    # If no config-based class found, try hardcoded mapping fallback
                                    if proto_class is None:
                                        proto_class = fallback_to_hardcoded_mapping(entity_type)
                                    
                                    if proto_class is None:
                                        click.echo(f"Warning: No protobuf class found for {entity_type}, skipping", err=True)
                                        continue
                                        
                                except Exception as e:
                                    click.echo(f"❌ Error loading protobuf class for {entity_type}: {e}", err=True)
                                    if hasattr(e, '__class__') and 'ModuleNotFoundError' in str(e.__class__):
                                        click.echo(f"💡 Hint: Ensure protobuf module is compiled with: protoc --python_out=. your_schema.proto", err=True)
                                        click.echo(f"💡 Or add 'protobuf_module' and 'protobuf_class' fields to your configuration", err=True)
                                    click.echo(f"⚠️  Skipping {entity_type}", err=True)
                                    continue
                                
                                topic_producers[topic] = ProtobufProducer(
                                    bootstrap_servers=servers,
                                    topic=topic,
                                    schema_registry_url=schema_registry_url,
                                    schema_proto_class=proto_class,
                                    config=kafka_config.copy(),
                                    key_field=key_field,
                                    auto_create_topic=True
                                )
                            
                            topic_producers[topic].produce(record)
                    
                    # Show progress
                    if count % 1000 == 0:
                        click.echo(f"    Generated {count} records...")
                    
                    # Check if we should stop
                    max_messages = entity_config.get("max_messages")
                    if max_messages and count >= max_messages:
                        break
                
                click.echo(f"    Total: {count} {entity_type}")
                
            except Exception as e:
                click.echo(f"Error generating {entity_type}: {e}", err=True)
                continue
    
    # Show statistics
    if ref_pool._stats_enabled:
        click.echo("\nGeneration Statistics:")
        stats = ref_pool.get_stats()
        for ref_type, type_stats in stats.items():
            click.echo(f"  {ref_type}:")
            click.echo(f"    References: {type_stats['reference_count']}")
            click.echo(f"    Accesses: {type_stats['access_count']}")
    
    if not dry_run:
        # Flush all producers
        if format == 'json' and producer:
            if hasattr(producer, '_topic_producers'):
                for topic_producer in producer._topic_producers.values():
                    topic_producer.flush()
            else:
                producer.flush()
        elif format == 'protobuf':
            for topic_producer in topic_producers.values():
                topic_producer.flush()
        
        click.echo("\nAll data produced to Kafka successfully!")


@correlated.command()
@click.argument('output_file')
def example_config(output_file):
    """Generate an example correlation configuration file."""
    
    example = {
        "master_data": {
            "customers": {
                "source": "faker",
                "count": 1000,
                "kafka_topic": "customers",
                "id_field": "customer_id",
                "bulk_load": True,
                "schema": {
                    "customer_id": {
                        "type": "string",
                        "format": "CUST_{seq:06d}"
                    },
                    "name": {
                        "type": "faker",
                        "method": "name"
                    },
                    "email": {
                        "type": "faker",
                        "method": "email"
                    },
                    "created_at": {
                        "type": "faker",
                        "method": "iso8601"
                    }
                }
            },
            "products": {
                "source": "csv",
                "file": "data/products.csv",
                "kafka_topic": "products",
                "id_field": "product_id",
                "bulk_load": True
            }
        },
        "transactional_data": {
            "orders": {
                "kafka_topic": "orders",
                "rate_per_second": 10,
                "max_messages": 10000,
                "track_recent": True,
                "relationships": {
                    "customer_id": {
                        "references": "customers.customer_id",
                        "distribution": "zipf",
                        "alpha": 1.5
                    },
                    "order_items": {
                        "type": "array",
                        "min_items": 1,
                        "max_items": 5,
                        "item_schema": {
                            "product_id": {
                                "references": "products.product_id"
                            },
                            "quantity": {
                                "type": "integer",
                                "min": 1,
                                "max": 10
                            },
                            "price": {
                                "type": "float",
                                "min": 9.99,
                                "max": 999.99
                            }
                        }
                    }
                },
                "derived_fields": {
                    "order_date": {
                        "type": "timestamp",
                        "format": "iso8601"
                    },
                    "status": {
                        "type": "string",
                        "initial_value": "pending"
                    },
                    "total_amount": {
                        "type": "calculated",
                        "formula": "sum(order_items.price * order_items.quantity)"
                    }
                }
            },
            "payments": {
                "kafka_topic": "payments",
                "rate_per_second": 8,
                "max_messages": 8000,
                "relationships": {
                    "order_id": {
                        "references": "orders.order_id",
                        "recency_bias": True,
                        "max_delay_minutes": 30
                    }
                },
                "derived_fields": {
                    "payment_method": {
                        "type": "conditional",
                        "conditions": [
                            {"if": "amount > 500", "then": "credit_card"},
                            {"if": "amount <= 50", "then": "cash"},
                            {"else": "debit_card"}
                        ]
                    },
                    "amount": {
                        "type": "reference",
                        "source": "orders.total_amount",
                        "via": "order_id"
                    },
                    "payment_date": {
                        "type": "timestamp",
                        "format": "iso8601"
                    },
                    "status": {
                        "type": "string",
                        "initial_value": "completed"
                    }
                }
            }
        }
    }
    
    try:
        with open(output_file, 'w') as f:
            yaml.dump(example, f, default_flow_style=False, sort_keys=False)
        click.echo(f"Example configuration written to {output_file}")
    except Exception as e:
        click.echo(f"Error writing example config: {e}", err=True)
        sys.exit(1)


@correlated.command()
@click.argument('config_file')
def validate(config_file):
    """Validate a correlation configuration file."""
    
    try:
        config = CorrelationConfig.from_yaml_file(config_file)
        click.echo("✅ Configuration is valid!")
        
        # Show summary
        master_types = list(config.config.get("master_data", {}).keys())
        transaction_types = list(config.config.get("transactional_data", {}).keys())
        
        click.echo(f"\nMaster data types: {', '.join(master_types)}")
        click.echo(f"Transactional types: {', '.join(transaction_types)}")
        
        # Check relationships
        click.echo("\nRelationships:")
        for t_type, t_config in config.config.get("transactional_data", {}).items():
            relationships = t_config.get("relationships", {})
            for field, rel in relationships.items():
                if isinstance(rel, dict) and "references" in rel:
                    click.echo(f"  {t_type}.{field} -> {rel['references']}")
        
    except Exception as e:
        click.echo(f"❌ Configuration invalid: {e}", err=True)
        sys.exit(1)
