"""CLI commands for correlated data generation."""
import click
import sys
import time
from pathlib import Path
import yaml

from testdatapy.generators import ReferencePool, CorrelatedDataGenerator
from testdatapy.generators.master_data_generator import MasterDataGenerator
from testdatapy.config.correlation_config import CorrelationConfig
from testdatapy.producers import JsonProducer
from testdatapy.producers.protobuf_producer import ProtobufProducer
from testdatapy.schemas.schema_loader import get_protobuf_class_for_entity, fallback_to_hardcoded_mapping
from testdatapy.performance.benchmark import VehicleBenchmarkSuite, PerformanceMonitor


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
@click.option('--benchmark', is_flag=True, help='Enable performance benchmarking and detailed monitoring')
@click.option('--progress-interval', default=1000, help='Progress reporting interval (records)')
@click.option('--monitor-memory', is_flag=True, help='Enable real-time memory monitoring')
@click.option('--correlation-report', is_flag=True, help='Generate detailed correlation analysis report')
@click.option('--benchmark-output', help='Directory to save benchmark results')
def generate(config, bootstrap_servers, producer_config, dry_run, master_only, transaction_only, format, schema_registry_url, clean_topics, benchmark, progress_interval, monitor_memory, correlation_report, benchmark_output):
    """Generate correlated test data based on configuration."""
    
    # Load configuration with vehicle validation
    try:
        correlation_config = CorrelationConfig.from_yaml_file(config)
        
        # Vehicle enhancement: Run vehicle-specific validation
        vehicle_validation = correlation_config.get_vehicle_specific_validation()
        if not vehicle_validation["valid"]:
            click.echo("❌ Vehicle Configuration validation failed:", err=True)
            for error in vehicle_validation["errors"]:
                click.echo(f"   Error: {error}", err=True)
            sys.exit(1)
        
        # Show vehicle validation warnings
        if vehicle_validation["warnings"]:
            click.echo("⚠️  Vehicle Configuration warnings:")
            for warning in vehicle_validation["warnings"]:
                click.echo(f"   Warning: {warning}")
    
    except Exception as e:
        click.echo(f"Error loading configuration: {e}", err=True)
        sys.exit(1)
    
    # Create reference pool with enhanced monitoring
    ref_pool = ReferencePool()
    ref_pool.enable_stats()
    
    # Initialize advanced monitoring if requested
    performance_monitor = None
    benchmark_suite = None
    
    if benchmark:
        click.echo("🔧 Benchmark mode enabled - detailed performance monitoring active")
        benchmark_suite = VehicleBenchmarkSuite()
        if benchmark_output:
            Path(benchmark_output).mkdir(parents=True, exist_ok=True)
            
    if monitor_memory or benchmark:
        performance_monitor = PerformanceMonitor()
        performance_monitor.start_monitoring()
        click.echo("📊 Real-time memory monitoring enabled")
    
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
            
            # Show loading performance statistics
            loading_stats = master_gen.get_loading_stats()
            total_stats = loading_stats.get("_total", {})
            total_duration = total_stats.get("duration_seconds", 0)
            total_records = total_stats.get("total_records", 0)
            
            click.echo(f"✅ Master data loaded in {total_duration:.2f}s ({total_records} total records)")
            
            # Show detailed summary with performance metrics
            for entity_type in correlation_config.config.get("master_data", {}):
                count = ref_pool.get_type_count(entity_type)
                entity_stats = loading_stats.get(entity_type, {})
                rps = entity_stats.get("records_per_second", 0)
                click.echo(f"  {entity_type}: {count} records ({rps:.0f} rec/s)")
                
                # Show sample if dry run
                if dry_run:
                    sample = master_gen.get_sample(entity_type, count=3)
                    for record in sample:
                        click.echo(f"    {record}")
            
            # Show memory usage for large datasets
            memory_usage = master_gen.get_memory_usage()
            if memory_usage["total_records"] > 10000:  # Only for large datasets
                click.echo(f"📊 Memory usage: {memory_usage['total_records']} records loaded")
                for entity_type, usage in memory_usage["entities"].items():
                    if usage["memory_mb"] > 0:
                        click.echo(f"   {entity_type}: {usage['memory_mb']:.1f} MB")
            
            # Produce if not dry run
            if not dry_run:
                if format == 'json':
                    try:
                        click.echo("Producing master data to Kafka...")
                        master_gen.produce_all()
                        click.echo("Master data production completed")
                    except Exception as e:
                        click.echo(f"Error producing master data: {e}", err=True)
                        import traceback
                        traceback.print_exc()
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
                
                # Initialize monitoring for this entity
                correlation_count = 0
                total_count = 0
                generation_start_time = time.time()
                
                count = 0
                for record in generator.generate():
                    count += 1
                    total_count += 1
                    
                    # Track correlations for detailed reporting
                    if correlation_report and record.get("appointment_plate") is not None:
                        correlation_count += 1
                    
                    if dry_run:
                        click.echo(f"    {record}")
                        if count >= 5:  # Show only first 5 in dry run
                            break
                    else:
                        topic = entity_config.get("kafka_topic")
                        # Use consistent key field priority logic
                        key_field = correlation_config.get_key_field(entity_type, is_master=False)
                        
                        # Check if key field is in key_only_fields first
                        key_only_fields = record.get("_key_only_fields", {})
                        if key_field in key_only_fields:
                            key = key_only_fields[key_field]
                        else:
                            key = record.get(key_field)
                        
                        # Remove _key_only_fields from the record before producing
                        if "_key_only_fields" in record:
                            record_copy = record.copy()
                            del record_copy["_key_only_fields"]
                            record = record_copy
                        
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
                    
                    # Enhanced progress reporting
                    if count % progress_interval == 0:
                        elapsed_time = time.time() - generation_start_time
                        rate = count / elapsed_time if elapsed_time > 0 else 0
                        
                        if correlation_report and total_count > 0:
                            current_correlation_ratio = correlation_count / total_count
                            click.echo(f"    Generated {count} records ({rate:.0f} rps, correlation: {current_correlation_ratio:.3f})")
                        else:
                            click.echo(f"    Generated {count} records ({rate:.0f} rps)")
                        
                        # Memory monitoring update
                        if performance_monitor:
                            current_memory = performance_monitor.get_peak_memory()
                            if current_memory > 0:
                                click.echo(f"    Memory usage: {current_memory:.1f} MB")
                    
                    # Check if we should stop
                    max_messages = entity_config.get("max_messages")
                    if max_messages and count >= max_messages:
                        break
                
                # Entity completion statistics
                entity_duration = time.time() - generation_start_time
                entity_rate = count / entity_duration if entity_duration > 0 else 0
                
                click.echo(f"    Total: {count} {entity_type} ({entity_rate:.0f} rps)")
                
                # Correlation reporting
                if correlation_report and total_count > 0:
                    final_correlation_ratio = correlation_count / total_count
                    click.echo(f"    Correlation ratio: {final_correlation_ratio:.3f} ({correlation_count}/{total_count})")
                    
                    # Vehicle requirement validation
                    target_ratio = 0.25
                    if abs(final_correlation_ratio - target_ratio) <= 0.05:
                        click.echo(f"    ✅ Vehicle correlation requirement met (target: {target_ratio})")
                    else:
                        click.echo(f"    ⚠️  Vehicle correlation requirement not met (target: {target_ratio}, actual: {final_correlation_ratio:.3f})")
                
            except Exception as e:
                click.echo(f"Error generating {entity_type}: {e}", err=True)
                continue
    
    # Enhanced statistics and monitoring reports
    click.echo("\n📊 Generation Statistics:")
    
    # Reference pool statistics
    if ref_pool._stats_enabled:
        stats = ref_pool.get_stats()
        for ref_type, type_stats in stats.items():
            click.echo(f"  {ref_type}:")
            click.echo(f"    References: {type_stats['reference_count']}")
            click.echo(f"    Accesses: {type_stats['access_count']}")
    
    # Memory monitoring results
    if performance_monitor:
        performance_monitor.stop_monitoring()
        peak_memory = performance_monitor.get_peak_memory()
        avg_cpu = performance_monitor.get_average_cpu()
        peak_cpu = performance_monitor.get_peak_cpu()
        
        click.echo(f"\n💾 Memory & Performance:")
        click.echo(f"  Peak memory usage: {peak_memory:.1f} MB")
        if avg_cpu > 0:
            click.echo(f"  Average CPU usage: {avg_cpu:.1f}%")
            click.echo(f"  Peak CPU usage: {peak_cpu:.1f}%")
    
    # Reference pool optimization metrics
    if hasattr(ref_pool, 'get_memory_usage'):
        ref_memory = ref_pool.get_memory_usage()
        click.echo(f"\n🔗 Reference Pool Metrics:")
        click.echo(f"  Total references: {ref_memory.get('total_references', 0)}")
        click.echo(f"  Cached records: {ref_memory.get('total_cached_records', 0)}")
        click.echo(f"  Index entries: {ref_memory.get('total_indices', 0)}")
        if ref_memory.get('memory_estimate_mb', 0) > 0:
            click.echo(f"  Estimated memory: {ref_memory['memory_estimate_mb']} MB")
    
    # Benchmark results
    if benchmark:
        try:
            click.echo(f"\n⚡ Running benchmark analysis...")
            metrics = benchmark_suite.benchmark_vehicle_scenario(
                config_file=config,
                test_name="CLI_Benchmark",
                enable_monitoring=False  # Already monitored above
            )
            
            click.echo(f"📈 Benchmark Results:")
            click.echo(f"  Total records: {metrics.records_generated}")
            click.echo(f"  Throughput: {metrics.records_per_second:.0f} records/sec")
            click.echo(f"  Duration: {metrics.duration_seconds:.1f} seconds")
            click.echo(f"  Correlation ratio: {metrics.correlation_ratio:.3f}")
            
            # Vehicle requirements validation
            validation = benchmark_suite.validate_vehicle_requirements(metrics)
            click.echo(f"\n🎯 Vehicle Requirements Validation:")
            for req, passed in validation.items():
                status = "✅" if passed else "❌"
                click.echo(f"  {status} {req.replace('_', ' ').title()}: {'PASS' if passed else 'FAIL'}")
            
            # Save benchmark results if output directory specified
            if benchmark_output:
                import json
                report = benchmark_suite.generate_performance_report()
                report_file = Path(benchmark_output) / f"cli_benchmark_{int(time.time())}.json"
                with open(report_file, 'w') as f:
                    json.dump(report, f, indent=2)
                click.echo(f"\n📁 Benchmark report saved: {report_file}")
                
        except Exception as e:
            click.echo(f"⚠️  Benchmark analysis failed: {e}")
    
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


@correlated.command()
@click.option('--config', '-c', required=True, help='Path to correlation config YAML file')
@click.option('--output-dir', '-o', default='./benchmark_results', help='Output directory for benchmark results')
@click.option('--scale-factors', default='1000,5000,10000', help='Comma-separated scale factors for testing')
@click.option('--test-name', default='Benchmark', help='Name for the benchmark test')
def benchmark(config, output_dir, scale_factors, test_name):
    """Run comprehensive performance benchmarks."""
    
    click.echo("🚀 Starting Performance Benchmark Suite...")
    
    try:
        # Parse scale factors
        scales = [int(s.strip()) for s in scale_factors.split(',')]
        
        # Initialize benchmark suite
        from testdatapy.performance.benchmark import run_vehicle_benchmark_suite
        
        # Create output directory
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Run comprehensive benchmark
        run_vehicle_benchmark_suite(
            config_file=config,
            output_dir=output_dir
        )
        
        click.echo(f"\n✅ Benchmark suite completed!")
        click.echo(f"📁 Results saved to: {output_dir}")
        
    except Exception as e:
        click.echo(f"❌ Benchmark failed: {e}", err=True)
        sys.exit(1)


@correlated.command()
@click.option('--config', '-c', required=True, help='Path to correlation config YAML file')
@click.option('--entity-type', '-e', help='Specific entity type to analyze')
@click.option('--sample-size', default=1000, help='Sample size for correlation analysis')
def analyze_correlation(config, entity_type, sample_size):
    """Analyze correlation patterns in scenario data."""
    
    click.echo("🔍 Analyzing correlation patterns...")
    
    try:
        # Load configuration
        correlation_config = CorrelationConfig.from_yaml_file(config)
        
        # Initialize components
        ref_pool = ReferencePool()
        ref_pool.enable_stats()
        
        # Load master data
        click.echo("Loading master data...")
        master_gen = MasterDataGenerator(
            config=correlation_config,
            reference_pool=ref_pool
        )
        master_gen.load_all()
        
        # Analyze correlations
        if entity_type:
            entities_to_analyze = [entity_type]
        else:
            entities_to_analyze = list(correlation_config.config.get("transactional_data", {}).keys())
        
        for entity in entities_to_analyze:
            click.echo(f"\n📊 Analyzing {entity}...")
            
            generator = CorrelatedDataGenerator(
                entity_type=entity,
                config=correlation_config,
                reference_pool=ref_pool,
                max_messages=sample_size
            )
            
            correlation_count = 0
            total_count = 0
            
            for record in generator.generate():
                total_count += 1
                if record.get("appointment_plate") is not None:
                    correlation_count += 1
            
            if total_count > 0:
                correlation_ratio = correlation_count / total_count
                click.echo(f"  Correlation ratio: {correlation_ratio:.3f} ({correlation_count}/{total_count})")
                
                # requirement check
                target = 0.25
                deviation = abs(correlation_ratio - target)
                if deviation <= 0.05:
                    click.echo(f"  ✅ requirement met (±5% tolerance)")
                else:
                    click.echo(f"  ⚠️  Deviation from target: {deviation:.3f}")
            else:
                click.echo(f"  ❌ No records generated")
        
        click.echo(f"\n✅ Correlation analysis complete!")
        
    except Exception as e:
        click.echo(f"❌ Analysis failed: {e}", err=True)
        sys.exit(1)
