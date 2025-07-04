"""Integration test configuration for pytest.

This module provides shared fixtures and configuration for all integration tests.
"""

import pytest
import time
import tempfile
from pathlib import Path
from typing import Dict, Any, Generator

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient

from testdatapy.schema.registry_manager import SchemaRegistryManager
from testdatapy.schema.manager import SchemaManager
from testdatapy.schema.compiler import ProtobufCompiler


@pytest.fixture(scope="session")
def kafka_config() -> Dict[str, str]:
    """Kafka configuration for integration tests."""
    return {
        "bootstrap.servers": "localhost:9092"
    }


@pytest.fixture(scope="session")
def schema_registry_config() -> Dict[str, str]:
    """Schema Registry configuration for integration tests."""
    return {
        "url": "http://localhost:8081"
    }


@pytest.fixture(scope="session")
def admin_client(kafka_config) -> AdminClient:
    """Create Kafka admin client for test management."""
    return AdminClient(kafka_config)


@pytest.fixture(scope="session")
def schema_registry_client(schema_registry_config) -> SchemaRegistryClient:
    """Create Schema Registry client for test management."""
    return SchemaRegistryClient(schema_registry_config)


@pytest.fixture(scope="session")
def integration_test_topics() -> list[str]:
    """List of topics used by integration tests."""
    return [
        "integration_test_customers",
        "integration_test_orders",
        "integration_test_payments",
        "integration_test_high_volume",
        "integration_test_schema_evolution",
        "integration_test_compatibility"
    ]


@pytest.fixture(scope="session", autouse=True)
def setup_integration_environment(
    admin_client: AdminClient,
    integration_test_topics: list[str]
) -> Generator[None, None, None]:
    """Set up and tear down integration test environment."""
    # Setup: Create test topics
    print("\\n🏗️  Setting up integration test environment...")
    
    # Delete existing topics if they exist
    try:
        admin_client.delete_topics(integration_test_topics, request_timeout=10)
        time.sleep(2)
    except Exception:
        pass  # Topics might not exist
    
    # Create fresh topics
    topics = []
    for topic in integration_test_topics:
        if "high_volume" in topic:
            # Multiple partitions for high volume tests
            topics.append(NewTopic(topic, num_partitions=3, replication_factor=1))
        else:
            topics.append(NewTopic(topic, num_partitions=1, replication_factor=1))
    
    admin_client.create_topics(topics)
    time.sleep(3)  # Allow time for topic creation
    
    print("✅ Integration test environment ready")
    
    yield
    
    # Teardown: Clean up test topics
    print("\\n🧹 Cleaning up integration test environment...")
    try:
        admin_client.delete_topics(integration_test_topics, request_timeout=10)
        print("✅ Integration test cleanup complete")
    except Exception as e:
        print(f"⚠️  Cleanup warning: {e}")


@pytest.fixture(scope="function")
def temp_proto_dir() -> Generator[Path, None, None]:
    """Create temporary directory for proto file operations."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture(scope="session")
def schema_manager() -> SchemaManager:
    """Create SchemaManager instance for testing."""
    return SchemaManager()


@pytest.fixture(scope="session")
def protobuf_compiler() -> ProtobufCompiler:
    """Create ProtobufCompiler instance for testing."""
    return ProtobufCompiler()


@pytest.fixture(scope="session")
def registry_manager(schema_registry_config) -> SchemaRegistryManager:
    """Create SchemaRegistryManager instance for testing."""
    return SchemaRegistryManager(
        schema_registry_url=schema_registry_config["url"]
    )


@pytest.fixture(scope="function")
def unique_test_id() -> str:
    """Generate unique test ID for each test function."""
    import uuid
    return str(uuid.uuid4())[:8]


@pytest.fixture(scope="session")
def customer_proto_file() -> Path:
    """Path to customer.proto file."""
    return Path(__file__).parent.parent.parent / "src" / "testdatapy" / "schemas" / "protobuf" / "customer.proto"


@pytest.fixture(scope="session")
def order_proto_file() -> Path:
    """Path to order.proto file."""
    return Path(__file__).parent.parent.parent / "src" / "testdatapy" / "schemas" / "protobuf" / "order.proto"


@pytest.fixture(scope="session")
def payment_proto_file() -> Path:
    """Path to payment.proto file."""
    return Path(__file__).parent.parent.parent / "src" / "testdatapy" / "schemas" / "protobuf" / "payment.proto"


@pytest.fixture(scope="function")
def consumer_config(kafka_config, unique_test_id) -> Dict[str, Any]:
    """Consumer configuration for testing."""
    config = kafka_config.copy()
    config.update({
        "group.id": f"integration_test_{unique_test_id}_{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    return config


@pytest.fixture(scope="function") 
def producer_config(kafka_config) -> Dict[str, Any]:
    """Producer configuration for testing."""
    config = kafka_config.copy()
    config.update({
        "acks": "all",
        "retries": 3,
        "batch.size": 16384,
        "linger.ms": 1,
    })
    return config


def pytest_configure(config):
    """Configure pytest for integration tests."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "e2e: mark test as end-to-end test"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add appropriate markers."""
    for item in items:
        # Mark all tests in integration directory as integration tests
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        
        # Mark specific tests as slow
        if any(keyword in item.name.lower() for keyword in ["high_volume", "performance", "bulk"]):
            item.add_marker(pytest.mark.slow)
        
        # Mark end-to-end tests
        if "e2e" in item.name.lower() or "suite" in item.name.lower():
            item.add_marker(pytest.mark.e2e)


@pytest.fixture(scope="session")
def integration_test_report():
    """Collect integration test results for reporting."""
    results = {
        "start_time": time.time(),
        "tests_run": [],
        "errors": [],
        "warnings": []
    }
    
    yield results
    
    results["end_time"] = time.time()
    results["duration"] = results["end_time"] - results["start_time"]
    
    # Print summary at the end of session
    print(f"\\n📊 Integration Test Session Summary:")
    print(f"   • Duration: {results['duration']:.2f} seconds")
    print(f"   • Tests run: {len(results['tests_run'])}")
    print(f"   • Errors: {len(results['errors'])}")
    print(f"   • Warnings: {len(results['warnings'])}")


# Utility functions for integration tests
def wait_for_kafka_topic(admin_client: AdminClient, topic: str, timeout: float = 30.0) -> bool:
    """Wait for a Kafka topic to be available."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            metadata = admin_client.list_topics(timeout=5)
            if topic in metadata.topics:
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def cleanup_test_subjects(sr_client: SchemaRegistryClient, prefix: str):
    """Clean up test subjects from Schema Registry."""
    try:
        subjects = sr_client.get_subjects()
        test_subjects = [s for s in subjects if s.startswith(prefix)]
        for subject in test_subjects:
            try:
                sr_client.delete_subject(subject)
            except Exception:
                pass
    except Exception:
        pass