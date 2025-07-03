"""CLI interface for test data generation."""
import json
import sys
import time

import click

from testdatapy.config.loader import AppConfig
from testdatapy.generators import CSVGenerator, FakerGenerator
from testdatapy.health import create_health_monitor
from testdatapy.metrics.collector import create_metrics_collector
from testdatapy.producers import AvroProducer, JsonProducer, ProtobufProducer
from testdatapy.schema_evolution import SchemaEvolutionManager
from testdatapy.shutdown import GracefulProducer, create_shutdown_handler


def _load_protobuf_class(
    proto_class: str | None,
    proto_module: str | None,
    proto_file: str | None,
    schema_paths: tuple[str, ...],
):
    """Load protobuf message class using various methods."""
    import sys
    from pathlib import Path
    import tempfile
    import subprocess
    import importlib.util
    import time
    
    from testdatapy.exceptions import (
        ProtobufClassNotFoundError,
        ProtobufImportError,
        ProtobufCompilerNotFoundError,
        SchemaCompilationError,
        SchemaNotFoundError
    )
    from testdatapy.logging_config import get_schema_logger, PerformanceTimer
    
    logger = get_schema_logger(__name__)
    start_time = time.time()
    
    if proto_class:
        # Method 1: Direct class import (e.g., 'customer_pb2.Customer')
        try:
            with PerformanceTimer(logger, "protobuf_class_import", proto_class=proto_class):
                module_name, class_name = proto_class.rsplit('.', 1)
                module = __import__(module_name, fromlist=[class_name])
                loaded_class = getattr(module, class_name)
                
                # Verify it's a protobuf class
                if not (hasattr(loaded_class, 'DESCRIPTOR') and callable(loaded_class)):
                    raise ProtobufClassNotFoundError(
                        class_spec=proto_class,
                        module_name=module_name
                    )
                
                logger.log_schema_loading(
                    schema_spec=proto_class,
                    method="class",
                    success=True,
                    duration=time.time() - start_time,
                    class_name=loaded_class.__name__
                )
                
                return loaded_class
                
        except (ImportError, AttributeError, ValueError) as e:
            logger.log_schema_loading(
                schema_spec=proto_class,
                method="class",
                success=False,
                duration=time.time() - start_time,
                error=str(e)
            )
            
            if isinstance(e, ImportError):
                raise ProtobufImportError(
                    module_name=module_name,
                    import_error=e,
                    search_paths=list(schema_paths) if schema_paths else None
                )
            else:
                raise ProtobufClassNotFoundError(
                    class_spec=proto_class,
                    module_name=module_name
                )
    
    elif proto_module:
        # Method 2: Module-based loading (e.g., 'customer_pb2')
        try:
            # Add schema paths to sys.path temporarily
            original_path = sys.path.copy()
            for path in schema_paths:
                if path not in sys.path:
                    sys.path.insert(0, path)
            
            try:
                with PerformanceTimer(logger, "protobuf_module_import", proto_module=proto_module):
                    module = __import__(proto_module)
                    
                    # Look for a protobuf message class in the module
                    protobuf_classes = []
                    for attr_name in dir(module):
                        attr = getattr(module, attr_name)
                        if (hasattr(attr, 'DESCRIPTOR') and 
                            hasattr(attr.DESCRIPTOR, 'full_name') and
                            callable(attr)):
                            protobuf_classes.append(attr)
                    
                    if not protobuf_classes:
                        raise ProtobufClassNotFoundError(
                            class_spec=proto_module,
                            module_name=proto_module,
                            search_paths=list(schema_paths) if schema_paths else None
                        )
                    
                    # Return the first protobuf class found
                    loaded_class = protobuf_classes[0]
                    
                    logger.log_schema_loading(
                        schema_spec=proto_module,
                        method="module",
                        success=True,
                        duration=time.time() - start_time,
                        class_name=loaded_class.__name__
                    )
                    
                    if len(protobuf_classes) > 1:
                        logger.warning(f"Multiple protobuf classes found in module '{proto_module}', using '{loaded_class.__name__}'")
                    
                    return loaded_class
                    
            finally:
                sys.path = original_path
                
        except ImportError as e:
            logger.log_schema_loading(
                schema_spec=proto_module,
                method="module",
                success=False,
                duration=time.time() - start_time,
                error=str(e)
            )
            
            raise ProtobufImportError(
                module_name=proto_module,
                import_error=e,
                search_paths=list(schema_paths) if schema_paths else None
            )
    
    elif proto_file:
        # Method 3: Compile .proto file and load dynamically
        proto_path = Path(proto_file)
        
        if not proto_path.exists():
            raise SchemaNotFoundError(
                schema_path=str(proto_path),
                schema_type="protobuf schema",
                search_paths=list(schema_paths) if schema_paths else None
            )
        
        try:
            # Create temporary directory for compilation
            with tempfile.TemporaryDirectory() as temp_dir:
                # Compile the .proto file
                cmd = [
                    'protoc',
                    f'--python_out={temp_dir}',
                    f'--proto_path={proto_path.parent}',
                ]
                
                # Add custom schema paths
                for path in schema_paths:
                    cmd.extend([f'--proto_path={path}'])
                
                cmd.append(str(proto_path))
                
                logger.debug(f"Compiling protobuf schema with command: {' '.join(cmd)}")
                
                try:
                    with PerformanceTimer(logger, "protobuf_compilation", proto_file=str(proto_path)):
                        result = subprocess.run(cmd, capture_output=True, text=True)
                        
                    if result.returncode != 0:
                        logger.log_schema_compilation(
                            schema_path=str(proto_path),
                            compiler="protoc",
                            success=False,
                            duration=time.time() - start_time,
                            error=result.stderr
                        )
                        
                        raise SchemaCompilationError(
                            schema_path=str(proto_path),
                            compilation_error=result.stderr,
                            compiler_output=result.stdout
                        )
                    
                    logger.log_schema_compilation(
                        schema_path=str(proto_path),
                        compiler="protoc",
                        success=True,
                        duration=time.time() - start_time,
                        output=result.stdout
                    )
                    
                except FileNotFoundError:
                    raise ProtobufCompilerNotFoundError()
                
                # Load the compiled module
                module_name = proto_path.stem + '_pb2'
                module_path = Path(temp_dir) / f'{module_name}.py'
                
                if not module_path.exists():
                    raise SchemaCompilationError(
                        schema_path=str(proto_path),
                        compilation_error=f"Compilation did not produce expected file: {module_path}"
                    )
                
                # Load module dynamically
                try:
                    with PerformanceTimer(logger, "protobuf_module_loading", module_path=str(module_path)):
                        spec = importlib.util.spec_from_file_location(module_name, module_path)
                        if spec is None or spec.loader is None:
                            raise SchemaCompilationError(
                                schema_path=str(proto_path),
                                compilation_error=f"Failed to create module spec for {module_path}"
                            )
                        
                        module = importlib.util.module_from_spec(spec)
                        sys.modules[module_name] = module
                        spec.loader.exec_module(module)
                        
                        # Find the message class
                        protobuf_classes = []
                        for attr_name in dir(module):
                            attr = getattr(module, attr_name)
                            if (hasattr(attr, 'DESCRIPTOR') and 
                                hasattr(attr.DESCRIPTOR, 'full_name') and
                                callable(attr)):
                                protobuf_classes.append(attr)
                        
                        if not protobuf_classes:
                            raise SchemaCompilationError(
                                schema_path=str(proto_path),
                                compilation_error=f"No protobuf message class found in compiled {proto_file}"
                            )
                        
                        # Return the first protobuf class found
                        loaded_class = protobuf_classes[0]
                        
                        logger.log_schema_loading(
                            schema_spec=str(proto_path),
                            method="file",
                            success=True,
                            duration=time.time() - start_time,
                            class_name=loaded_class.__name__
                        )
                        
                        return loaded_class
                        
                except Exception as e:
                    logger.log_schema_loading(
                        schema_spec=str(proto_path),
                        method="file",
                        success=False,
                        duration=time.time() - start_time,
                        error=str(e)
                    )
                    
                    raise SchemaCompilationError(
                        schema_path=str(proto_path),
                        compilation_error=f"Failed to load compiled module: {e}"
                    )
                
        except Exception as e:
            if not isinstance(e, (SchemaCompilationError, ProtobufCompilerNotFoundError, SchemaNotFoundError)):
                logger.log_schema_loading(
                    schema_spec=str(proto_path),
                    method="file", 
                    success=False,
                    duration=time.time() - start_time,
                    error=str(e)
                )
                
                raise SchemaCompilationError(
                    schema_path=str(proto_path),
                    compilation_error=str(e)
                )
            raise
    
    else:
        raise click.UsageError("No protobuf class specification provided")


@click.group()
@click.version_option()
def cli():
    """Test data generation tool for Kafka."""
    pass


@cli.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    help="Configuration file path (JSON format)",
)
@click.option("--topic", "-t", required=True, help="Kafka topic to produce to")
@click.option(
    "--format",
    "-f",
    type=click.Choice(["json", "avro", "protobuf"]),
    default="json",
    help="Message format",
)
@click.option(
    "--generator",
    "-g",
    type=click.Choice(["faker", "csv"]),
    default="faker",
    help="Data generator type",
)
@click.option("--schema-file", type=click.Path(exists=True), help="Avro schema file")
@click.option("--proto-file", type=click.Path(exists=True), help="Protobuf schema file (.proto)")
@click.option("--proto-module", help="Pre-compiled protobuf module name (e.g., 'customer_pb2')")
@click.option("--proto-class", help="Fully qualified protobuf message class name (e.g., 'mypackage.Customer')")
@click.option("--schema-path", multiple=True, help="Custom schema directory paths (can be used multiple times)")
@click.option("--auto-register/--no-auto-register", default=False, help="Automatically register protobuf schemas with Schema Registry")
@click.option("--csv-file", type=click.Path(exists=True), help="CSV file for csv generator")
@click.option("--key-field", help="Field to use as message key")
@click.option("--rate", type=float, help="Messages per second")
@click.option("--count", type=int, help="Maximum number of messages to produce")
@click.option("--duration", type=float, help="Maximum duration in seconds")
@click.option("--seed", type=int, help="Random seed for reproducible data")
@click.option("--dry-run", is_flag=True, help="Print messages without producing")
@click.option("--metrics/--no-metrics", default=False, help="Enable metrics collection")
@click.option("--metrics-port", type=int, default=9090, help="Port for metrics server")
@click.option("--health/--no-health", default=False, help="Enable health checks")
@click.option("--health-port", type=int, default=8080, help="Port for health server")
@click.option("--auto-create-topic/--no-auto-create-topic", default=True, help="Auto-create topic if it doesn't exist")
@click.option("--topic-partitions", type=int, default=1, help="Number of partitions for auto-created topics")
@click.option("--topic-replication", type=int, default=1, help="Replication factor for auto-created topics")
def produce(
    config: str | None,
    topic: str,
    format: str,
    generator: str,
    schema_file: str | None,
    proto_file: str | None,
    proto_module: str | None,
    proto_class: str | None,
    schema_path: tuple[str, ...],
    auto_register: bool,
    csv_file: str | None,
    key_field: str | None,
    rate: float,
    count: int | None,
    duration: float | None,
    seed: int | None,
    dry_run: bool,
    metrics: bool,
    metrics_port: int,
    health: bool,
    health_port: int,
    auto_create_topic: bool,
    topic_partitions: int,
    topic_replication: int,
):
    """Produce test data to Kafka."""
    # Create shutdown handler
    shutdown_handler = create_shutdown_handler()
    
    # Load configuration
    if config:
        app_config = AppConfig.from_file(config)
    else:
        app_config = AppConfig()

    # Override with CLI options (only if explicitly provided)
    if rate is not None:
        app_config.producer.rate_per_second = rate
    if count is not None:
        app_config.producer.max_messages = count
    if duration is not None:
        app_config.producer.max_duration_seconds = duration

    # Set up metrics
    metrics_collector = create_metrics_collector(enabled=metrics)
    if metrics:
        metrics_collector.start_metrics_server(port=metrics_port)
        click.echo(f"Metrics server started on port {metrics_port}")
    
    # Set up health monitoring
    health_monitor = create_health_monitor(enabled=health, port=health_port)
    if health:
        click.echo(f"Health server started on port {health_port}")
        # Add health checks
        from testdatapy.health import HealthCheck, HealthStatus
        health_monitor.add_check(
            "kafka",
            HealthCheck("kafka", HealthStatus.HEALTHY, "Kafka connection")
        )
    
    # Set up generator
    if generator == "faker":
        if format == "avro" and not schema_file:
            raise click.UsageError("Avro format requires --schema-file")
        if format == "protobuf":
            # Validate protobuf options - need at least one specification method
            if not proto_class and not proto_module and not proto_file:
                raise click.UsageError(
                    "Protobuf format requires one of: --proto-class, --proto-module, or --proto-file"
                )
            # Only allow one primary method at a time
            methods_count = sum([bool(proto_class), bool(proto_module), bool(proto_file)])
            if methods_count > 1:
                raise click.UsageError(
                    "Can only specify one of: --proto-class, --proto-module, or --proto-file"
                )

        data_generator = FakerGenerator(
            rate_per_second=app_config.producer.rate_per_second,
            max_messages=app_config.producer.max_messages,
            seed=seed,
        )
    elif generator == "csv":
        if not csv_file:
            raise click.UsageError("CSV generator requires --csv-file")

        data_generator = CSVGenerator(
            csv_file=csv_file,
            rate_per_second=app_config.producer.rate_per_second,
            max_messages=app_config.producer.max_messages,
        )
    else:
        raise click.BadParameter(f"Unknown generator: {generator}")

    # Set up producer
    if not dry_run:
        kafka_config = app_config.to_confluent_config()
        
        # Topic configuration for auto-creation
        topic_config = {
            "num_partitions": topic_partitions,
            "replication_factor": topic_replication
        }

        if format == "json":
            producer = JsonProducer(
                bootstrap_servers=app_config.kafka.bootstrap_servers,
                topic=topic,
                config=kafka_config,
                key_field=key_field,
                auto_create_topic=auto_create_topic,
                topic_config=topic_config,
            )
        elif format == "avro":
            if not schema_file:
                raise click.UsageError("Avro format requires --schema-file")

            sr_config = app_config.to_schema_registry_config()
            producer = AvroProducer(
                bootstrap_servers=app_config.kafka.bootstrap_servers,
                topic=topic,
                schema_registry_url=app_config.schema_registry.url,
                schema_path=schema_file,
                config=kafka_config,
                schema_registry_config=sr_config,
                key_field=key_field,
                auto_create_topic=auto_create_topic,
                topic_config=topic_config,
            )
        elif format == "protobuf":
            # Dynamically load protobuf class
            proto_message_class = _load_protobuf_class(
                proto_class=proto_class,
                proto_module=proto_module,
                proto_file=proto_file,
                schema_paths=schema_path,
            )
            
            sr_config = app_config.to_schema_registry_config()
            producer = ProtobufProducer(
                bootstrap_servers=app_config.kafka.bootstrap_servers,
                topic=topic,
                schema_registry_url=app_config.schema_registry.url,
                schema_proto_class=proto_message_class,
                schema_file_path=proto_file,
                config=kafka_config,
                schema_registry_config=sr_config,
                key_field=key_field,
                auto_create_topic=auto_create_topic,
                topic_config=topic_config,
            )
            
            # Auto-register schema if requested
            if auto_register and proto_file:
                try:
                    subject = f"{topic}-value"
                    schema_id = producer.register_schema(subject)
                    click.echo(f"Registered protobuf schema with ID: {schema_id}")
                except Exception as e:
                    click.echo(f"Warning: Failed to register schema: {e}", err=True)
        else:
            raise click.BadParameter(f"Unknown format: {format}")
        
        # Wrap producer for graceful shutdown
        producer = GracefulProducer(producer, shutdown_handler)
    else:
        producer = None

    # Start producing
    start_time = time.time()
    message_count = 0

    click.echo("Starting data generation...")
    click.echo(f"Generator: {generator}")
    click.echo(f"Format: {format}")
    click.echo(f"Topic: {topic}")
    click.echo(f"Rate: {app_config.producer.rate_per_second} msg/s")
    if app_config.producer.max_messages:
        click.echo(f"Max messages: {app_config.producer.max_messages}")
    if app_config.producer.max_duration_seconds:
        click.echo(f"Max duration: {app_config.producer.max_duration_seconds}s")
    click.echo()

    try:
        # Generate data based on format and generator
        if generator == "faker" and format == "avro" and schema_file:
            # Generate based on Avro schema
            with open(schema_file) as f:
                schema = json.load(f)
            data_iterator = data_generator.generate_generic(schema)
        elif generator == "faker" and format == "protobuf" and (proto_class or proto_module or proto_file):
            # For protobuf, we'll use generic generation with field mapping
            # The protobuf producer will handle conversion
            data_iterator = data_generator.generate()
        else:
            # Use default generation
            data_iterator = data_generator.generate()

        for data in data_iterator:
            # Check for shutdown
            if shutdown_handler.is_shutting_down():
                break
                
            # Check duration limit
            if app_config.producer.max_duration_seconds:
                elapsed = time.time() - start_time
                if elapsed >= app_config.producer.max_duration_seconds:
                    break

            # Extract key if specified
            key = None
            if key_field and key_field in data:
                key = str(data[key_field])

            if dry_run:
                click.echo(f"Key: {key}, Value: {json.dumps(data, indent=2)}")
            else:
                produce_start = time.time()
                producer.produce(key=key, value=data)
                produce_duration = time.time() - produce_start
                
                # Record metrics
                if metrics:
                    data_size = len(json.dumps(data).encode('utf-8'))
                    metrics_collector.record_message_produced(
                        topic=topic,
                        format=format,
                        generator=generator,
                        size_bytes=data_size,
                        duration=produce_duration
                    )

            message_count += 1

            # Print progress every 100 messages
            if message_count % 100 == 0:
                elapsed = time.time() - start_time
                actual_rate = message_count / elapsed if elapsed > 0 else 0
                click.echo(
                    f"Produced {message_count} messages "
                    f"(rate: {actual_rate:.1f} msg/s)"
                )
                
                # Update health check
                if health:
                    health_monitor.update_check(
                        "kafka",
                        HealthStatus.HEALTHY,
                        f"Producing at {actual_rate:.1f} msg/s"
                    )

    except KeyboardInterrupt:
        click.echo("\nInterrupted by user")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if metrics:
            metrics_collector.record_message_failed(
                topic=topic,
                format=format,
                error_type=type(e).__name__
            )
    finally:
        if producer and not dry_run:
            remaining = producer.flush(10.0)
            if remaining > 0:
                click.echo(f"Warning: {remaining} messages still in queue")

    # Print summary
    elapsed = time.time() - start_time
    actual_rate = message_count / elapsed if elapsed > 0 else 0
    click.echo()
    click.echo("Summary:")
    click.echo(f"Total messages: {message_count}")
    click.echo(f"Duration: {elapsed:.1f}s")
    click.echo(f"Actual rate: {actual_rate:.1f} msg/s")
    
    if metrics:
        stats = metrics_collector.get_stats()
        click.echo(f"Success rate: {stats['success_rate']:.1%}")
        click.echo(f"Total bytes: {stats['bytes_produced']:,}")
    
    # Graceful shutdown
    shutdown_handler.shutdown()


@cli.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    help="Configuration file path",
)
@click.option("--schema-file", type=click.Path(exists=True), help="Avro schema file")
def validate(config: str | None, schema_file: str | None):
    """Validate configuration and schemas."""
    if config:
        try:
            app_config = AppConfig.from_file(config)
            click.echo(f"✓ Configuration file is valid: {config}")
            click.echo(f"  Kafka: {app_config.kafka.bootstrap_servers}")
            click.echo(f"  Schema Registry: {app_config.schema_registry.url}")
            click.echo(f"  Rate: {app_config.producer.rate_per_second} msg/s")
        except Exception as e:
            click.echo(f"✗ Configuration file is invalid: {e}", err=True)
            sys.exit(1)

    if schema_file:
        try:
            with open(schema_file) as f:
                schema = json.load(f)
            click.echo(f"✓ Schema file is valid: {schema_file}")
            click.echo(f"  Type: {schema.get('type')}")
            click.echo(f"  Name: {schema.get('name')}")
            click.echo(f"  Fields: {len(schema.get('fields', []))}")
        except Exception as e:
            click.echo(f"✗ Schema file is invalid: {e}", err=True)
            sys.exit(1)


@cli.command()
@click.option(
    "--schema-registry-url",
    "-s",
    required=True,
    help="Schema Registry URL",
)
@click.option(
    "--subject",
    required=True,
    help="Schema subject",
)
@click.option(
    "--schema-file",
    type=click.Path(exists=True),
    help="New schema file to register",
)
@click.option(
    "--check-compatibility",
    is_flag=True,
    help="Check compatibility before registering",
)
@click.option(
    "--list-versions",
    is_flag=True,
    help="List all schema versions",
)
@click.option(
    "--get-version",
    type=int,
    help="Get specific schema version",
)
@click.option(
    "--evolve",
    is_flag=True,
    help="Evolve schema with changes",
)
def schema(
    schema_registry_url,
    subject,
    schema_file,
    check_compatibility,
    list_versions,
    get_version,
    evolve
):
    """Manage schemas in Schema Registry."""
    evolution_manager = SchemaEvolutionManager(schema_registry_url)
    
    if list_versions:
        versions = evolution_manager.get_all_versions(subject)
        click.echo(f"Schema versions for subject '{subject}':")
        for version in versions:
            click.echo(f"  Version {version}")
        return
    
    if get_version:
        schema_data = evolution_manager.get_schema_by_id(get_version)
        if schema_data:
            click.echo(json.dumps(schema_data, indent=2))
        else:
            click.echo(f"Schema version {get_version} not found", err=True)
        return
    
    if schema_file:
        with open(schema_file) as f:
            schema_str = f.read()
        
        if check_compatibility:
            is_compatible = evolution_manager.check_compatibility(subject, schema_str)
            if is_compatible:
                click.echo("✓ Schema is compatible")
            else:
                click.echo("✗ Schema is not compatible", err=True)
                return
        
        # Register schema
        schema_id = evolution_manager.register_schema(subject, schema_str)
        click.echo(f"Schema registered with ID: {schema_id}")
    
    # Get latest schema
    latest_schema = evolution_manager.get_latest_schema(subject)
    if latest_schema:
        click.echo(f"\nLatest schema for '{subject}':")
        click.echo(json.dumps(latest_schema, indent=2))


@cli.command()
def list_generators():
    """List available data generators."""
    click.echo("Available generators:")
    click.echo()
    click.echo("faker")
    click.echo("  - Generates random data using the Faker library")
    click.echo("  - Supports custom schemas and field inference")
    click.echo("  - Can generate data based on Avro schemas")
    click.echo()
    click.echo("csv")
    click.echo("  - Reads data from CSV files")
    click.echo("  - Cycles through the file when reaching the end")
    click.echo("  - Maintains original data types")
    click.echo()
    click.echo("correlated")
    click.echo("  - Generates data with relationships between entities")
    click.echo("  - Supports master data and transactional data")
    click.echo("  - Ensures referential integrity for testing Flink joins")


@cli.command()
def list_formats():
    """List supported message formats."""
    click.echo("Supported formats:")
    click.echo()
    click.echo("json")
    click.echo("  - JSON-encoded messages")
    click.echo("  - Human-readable format")
    click.echo("  - No schema registry required")
    click.echo()
    click.echo("avro")
    click.echo("  - Apache Avro binary format")
    click.echo("  - Schema evolution support")
    click.echo("  - Requires schema registry")
    click.echo()
    click.echo("protobuf")
    click.echo("  - Protocol Buffers binary format")
    click.echo("  - Efficient serialization")
    click.echo("  - Requires schema registry and proto class")


# Import and add correlated commands
from testdatapy.cli_correlated import correlated
cli.add_command(correlated)

# Add proto schema management commands
from testdatapy.cli_proto import proto
cli.add_command(proto)

# Add cache and version management commands
from testdatapy.cli_cache import cache
cli.add_command(cache)

# Add batch operations commands
from testdatapy.cli_batch import batch
cli.add_command(batch)


def main():
    """Main entry point."""
    cli()


if __name__ == "__main__":
    main()
