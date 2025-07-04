# Integration Tests

This directory contains comprehensive end-to-end integration tests for the TestDataPy protobuf implementation. These tests validate the complete workflow from .proto files through compilation, schema registration, message production, and consumption.

## Test Suite Overview

### 🧪 Test Modules

#### 1. Proto-to-Kafka End-to-End (`test_proto_to_kafka_e2e.py`)
Tests the complete pipeline from protobuf schemas to Kafka message production and consumption:
- **Complete workflow testing**: .proto → compilation → registration → production → consumption
- **Correlated entity workflows**: Customer → Order → Payment relationships
- **High-volume message production**: Performance testing with 1000+ messages
- **Schema evolution compatibility**: Testing schema changes and compatibility
- **Error handling scenarios**: Invalid data, registry failures, topic issues
- **Cross-platform message formats**: Unicode, binary compatibility, platform-specific data

#### 2. Schema Registry Integration (`test_schema_registry_integration.py`)
Comprehensive testing of Schema Registry functionality:
- **Basic operations**: Registration, retrieval, versioning
- **Schema from file registration**: Direct .proto file registration
- **Subject management**: Naming conventions, listing, filtering
- **Compatibility checking**: Schema evolution and compatibility analysis
- **Multi-version management**: Version history and retrieval
- **Caching and performance**: Cache behavior and performance optimization
- **Error handling**: Invalid schemas, connection issues, non-existent resources
- **Metadata and references**: Schema metadata handling and reference management
- **Bulk operations**: Batch processing and concurrent operations
- **Validation integration**: Schema validation and compilation integration

#### 3. Cross-Platform Compatibility (`test_cross_platform_compatibility.py`)
Testing compatibility across different platforms and configurations:
- **Message serialization consistency**: Ensuring consistent binary output
- **Unicode and encoding compatibility**: Multi-language character support
- **Binary compatibility and endianness**: Cross-platform binary format validation
- **Different Kafka configurations**: Various producer/consumer configurations
- **Performance characteristics**: Testing across different message sizes and configurations
- **Environment variable configurations**: Testing different locale and environment settings
- **Cross-schema compatibility**: Multiple schema types in same application
- **Platform-specific characteristics**: OS-specific behavior validation

#### 4. Comprehensive Test Suite Runner (`test_e2e_suite_runner.py`)
Orchestrates and manages the complete test suite:
- **Environment validation**: Pre-flight checks for Kafka and Schema Registry
- **Coordinated test execution**: Running all test modules in correct order
- **Comprehensive reporting**: Detailed success/failure reporting with metrics
- **Result persistence**: JSON output for CI/CD integration
- **Performance monitoring**: Duration tracking and performance analysis

## 🚀 Running the Tests

### Prerequisites

1. **Docker Environment**: Start Kafka and Schema Registry
   ```bash
   make docker-up
   # OR
   docker-compose -f docker/docker-compose.yml up -d
   ```

2. **Python Dependencies**: Install test dependencies
   ```bash
   pip install -e .[dev]
   # OR
   uv pip install -e .[dev]
   ```

3. **Environment Validation**: Verify services are running
   ```bash
   # Check Kafka
   curl -f localhost:9092 || echo "Kafka not ready"
   
   # Check Schema Registry
   curl -f http://localhost:8081/subjects || echo "Schema Registry not ready"
   ```

### Running Individual Test Modules

```bash
# Run proto-to-kafka end-to-end tests
pytest tests/integration/test_proto_to_kafka_e2e.py -v

# Run Schema Registry integration tests
pytest tests/integration/test_schema_registry_integration.py -v

# Run cross-platform compatibility tests
pytest tests/integration/test_cross_platform_compatibility.py -v
```

### Running the Complete Test Suite

```bash
# Run all integration tests
pytest tests/integration/ -v

# Run with specific markers
pytest -m integration -v
pytest -m "integration and not slow" -v
pytest -m e2e -v

# Run the comprehensive suite runner
python tests/integration/test_e2e_suite_runner.py

# Run with custom configuration
python tests/integration/test_e2e_suite_runner.py \
  --kafka-servers localhost:9092 \
  --schema-registry http://localhost:8081 \
  --output results.json
```

### Running via Make Commands

```bash
# Run all integration tests
make test-integration

# Run with timeout
make test-integration TIMEOUT=300

# Run specific test file
pytest tests/integration/test_proto_to_kafka_e2e.py::TestProtoToKafkaE2E::test_complete_proto_to_kafka_workflow -v
```

## 📊 Test Coverage and Validation

### What Gets Tested

#### ✅ Core Functionality
- [x] Protobuf schema compilation from .proto files
- [x] Schema Registry registration and retrieval
- [x] Message production with correct binary serialization
- [x] Message consumption and deserialization
- [x] Schema evolution and compatibility checking
- [x] Error handling and graceful degradation

#### ✅ Performance and Scale
- [x] High-volume message production (1000+ messages/sec)
- [x] Large message handling (multi-KB protobuf messages)
- [x] Concurrent operations and thread safety
- [x] Memory usage and resource cleanup
- [x] Caching performance and efficiency

#### ✅ Compatibility and Portability
- [x] Unicode and multi-language character support
- [x] Cross-platform binary compatibility
- [x] Different Python versions and environments
- [x] Various Kafka and Schema Registry configurations
- [x] Docker vs local environment consistency

#### ✅ Real-World Scenarios
- [x] Correlated data workflows (Customer → Order → Payment)
- [x] Schema evolution in production-like scenarios
- [x] Error recovery and retry mechanisms
- [x] Topic management and partitioning
- [x] Multiple schema types in single application

### Expected Test Results

When all tests pass, you should see:
```
🎉 ALL TESTS PASSED! The protobuf integration is working correctly.

📊 Overall Statistics:
   • Total Test Modules: 3
   • Successful Modules: 3
   • Failed Modules: 0
   • Total Tests: ~50+
   • Passed: ~50+
   • Failed: 0
   • Skipped: 0
   • Success Rate: 100.0%
```

## 🔧 Configuration and Customization

### Environment Variables

```bash
# Kafka configuration
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export SCHEMA_REGISTRY_URL="http://localhost:8081"

# Test configuration
export TEST_TIMEOUT=300
export TEST_VERBOSE=true
export TEST_OUTPUT_FILE="integration_results.json"
```

### Custom Test Configuration

Create a `pytest.ini` file for custom settings:
```ini
[tool:pytest]
testpaths = tests/integration
markers =
    integration: Integration tests requiring Kafka and Schema Registry
    slow: Slow-running tests (> 30 seconds)
    e2e: End-to-end workflow tests
timeout = 300
addopts = -v --tb=short
```

### Docker Configuration

For custom Docker setups, modify `docker/docker-compose.yml`:
```yaml
services:
  kafka:
    ports:
      - "9092:9092"  # Adjust port if needed
  schema-registry:
    ports:
      - "8081:8081"  # Adjust port if needed
```

## 🐛 Troubleshooting

### Common Issues

#### Kafka/Schema Registry Not Available
```bash
# Check services are running
docker ps | grep -E "(kafka|schema-registry)"

# Check logs
docker logs testdatapy-kafka
docker logs testdatapy-schema-registry

# Restart services
make docker-down && make docker-up
```

#### Topic Creation Failures
```bash
# Manual topic creation
kafka-topics --create --topic test-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

#### Schema Registry Connection Issues
```bash
# Test Schema Registry connectivity
curl -f http://localhost:8081/subjects
curl -f http://localhost:8081/config
```

#### Python Import Errors
```bash
# Reinstall dependencies
pip uninstall testdatapy
pip install -e .[dev]

# Check protobuf compilation
python -c "from testdatapy.schemas.protobuf import customer_pb2; print('OK')"
```

### Test-Specific Debugging

#### Enable Verbose Output
```bash
pytest tests/integration/ -v -s --tb=long
```

#### Run Single Test with Debug
```bash
pytest tests/integration/test_proto_to_kafka_e2e.py::TestProtoToKafkaE2E::test_complete_proto_to_kafka_workflow -v -s
```

#### Check Test Environment
```bash
python -c "
from tests.integration.test_e2e_suite_runner import E2ETestSuiteRunner
runner = E2ETestSuiteRunner()
env = runner.check_environment()
print(env)
"
```

## 📈 CI/CD Integration

### GitHub Actions Example
```yaml
name: Integration Tests
on: [push, pull_request]

jobs:
  integration-tests:
    runs-on: ubuntu-latest
    services:
      kafka:
        image: confluentinc/cp-kafka:7.9.1
        ports:
          - 9092:9092
      schema-registry:
        image: confluentinc/cp-schema-registry:7.9.1
        ports:
          - 8081:8081
    
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          pip install -e .[dev]
      
      - name: Wait for services
        run: |
          timeout 60 bash -c 'until curl -f http://localhost:8081/subjects; do sleep 2; done'
      
      - name: Run integration tests
        run: |
          python tests/integration/test_e2e_suite_runner.py --output integration_results.json
      
      - name: Upload results
        uses: actions/upload-artifact@v3
        with:
          name: integration-test-results
          path: integration_results.json
```

### Makefile Integration
```makefile
.PHONY: test-integration-full
test-integration-full:
	@echo "🚀 Running comprehensive integration test suite..."
	docker-compose -f docker/docker-compose.yml up -d
	sleep 10
	python tests/integration/test_e2e_suite_runner.py --output results/integration_$(shell date +%Y%m%d_%H%M%S).json
	docker-compose -f docker/docker-compose.yml down
```

## 📝 Adding New Integration Tests

### Guidelines for New Tests

1. **Follow naming convention**: `test_<functionality>_integration.py`
2. **Use integration markers**: `@pytest.mark.integration`
3. **Include cleanup**: Ensure proper resource cleanup
4. **Add documentation**: Document what the test validates
5. **Consider performance**: Mark slow tests appropriately

### Example New Test Structure
```python
import pytest
from testdatapy.producers.protobuf_producer import ProtobufProducer

@pytest.mark.integration
class TestNewFunctionality:
    """Test new functionality integration."""
    
    def test_new_feature(self, kafka_config, unique_test_id):
        """Test description of what this validates."""
        # Test implementation
        pass
```

## 🎯 Success Criteria

The integration test suite validates that:

1. ✅ **End-to-End Workflow**: Complete .proto → Kafka pipeline works
2. ✅ **Schema Management**: All Schema Registry operations function correctly
3. ✅ **Cross-Platform**: Compatible across different environments
4. ✅ **Performance**: Meets throughput and latency requirements
5. ✅ **Reliability**: Handles errors gracefully and recovers properly
6. ✅ **Compatibility**: Works with various Kafka/Schema Registry configurations

When all tests pass, the protobuf implementation is ready for production use with confidence in its reliability, performance, and compatibility.