# TestDataPy

A powerful test data generation tool for Apache Kafka with support for JSON and Avro formats, featuring integration with Faker for realistic data generation and CSV file replay capabilities.

## Features

- 🚀 High-performance data generation with configurable rate limiting
- 📊 Multiple data formats: JSON and Apache Avro
- 🎲 Realistic data using Faker library with smart field inference  
- 📁 CSV file replay for reproducing production data patterns
- 🔒 Full mTLS support for secure Kafka clusters
- 🐳 Docker Compose setup for local development
- ☸️ Kubernetes-ready with example configurations
- 🧪 Comprehensive test coverage with unit and integration tests
- 💻 Intuitive CLI interface

## Installation

### Using UV (Recommended)

```bash
uv pip install -e .
```

### Using pip

```bash
pip install -e .
```

### Development Installation

```bash
uv pip install -e .[dev]
```

## Quick Start

### 1. Start Local Kafka Cluster

```bash
make docker-up
```

### 2. Generate JSON Data with Faker

```bash
testdatapy produce --topic test-topic --format json --count 100
```

### 3. Generate Avro Data from Schema

```bash
testdatapy produce \
  --topic customer-events \
  --format avro \
  --schema-file examples/schemas/customer.avsc \
  --rate 50
```

### 4. Replay CSV Data

```bash
testdatapy produce \
  --topic customer-data \
  --generator csv \
  --csv-file examples/data/CustomerMasterData.csv \
  --key-field CustomerID
```

## Configuration

### Configuration File

Create a JSON configuration file:

```json
{
  "bootstrap.servers": "localhost:9092",
  "schema.registry.url": "http://localhost:8081",
  "security.protocol": "PLAINTEXT",
  "rate_per_second": 10,
  "max_messages": 1000
}
```

Use with:

```bash
testdatapy produce --config config.json --topic my-topic
```

### Environment Variables

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export SCHEMA_REGISTRY_URL=http://localhost:8081
export PRODUCER_RATE_PER_SECOND=10
```

### mTLS Configuration

For secure Kafka clusters:

```json
{
  "bootstrap.servers": "kafka.example.com:9093",
  "security.protocol": "SSL",
  "ssl.ca.location": "/path/to/ca-cert",
  "ssl.certificate.location": "/path/to/client-cert",
  "ssl.key.location": "/path/to/client-key"
}
```

## Data Generators

### Faker Generator

Generates realistic fake data using the Faker library:

- Smart field inference based on field names
- Support for complex nested structures
- Configurable locales and random seeds
- Can generate from Avro schemas automatically

```bash
# Generate with seed for reproducible data
testdatapy produce --topic users --generator faker --seed 42

# Generate from Avro schema
testdatapy produce \
  --topic users \
  --format avro \
  --schema-file user.avsc
```

### CSV Generator

Replays data from CSV files:

- Maintains original data types
- Configurable cycling behavior
- Rate-limited playback
- Automatic key extraction

```bash
# Replay CSV with cycling
testdatapy produce \
  --topic events \
  --generator csv \
  --csv-file events.csv \
  --key-field event_id
```

## Message Formats

### JSON Format

- Human-readable
- No schema required
- Flexible structure

### Avro Format  

- Binary format with schema
- Schema evolution support
- Requires Schema Registry
- Compact size

## CLI Commands

### produce

Generate and send data to Kafka:

```bash
testdatapy produce [OPTIONS]

Options:
  -c, --config PATH         Configuration file path
  -t, --topic TEXT          Kafka topic (required)
  -f, --format [json|avro]  Message format [default: json]
  -g, --generator [faker|csv]  Generator type [default: faker]
  --schema-file PATH        Avro schema file
  --csv-file PATH          CSV file for csv generator
  --key-field TEXT         Field to use as message key
  --rate FLOAT             Messages per second [default: 10.0]
  --count INTEGER          Maximum messages to produce
  --duration FLOAT         Maximum duration in seconds
  --seed INTEGER           Random seed for reproducible data
  --dry-run                Print messages without producing
```

### validate

Validate configuration and schemas:

```bash
testdatapy validate --config config.json --schema-file schema.avsc
```

### list-generators

List available data generators:

```bash
testdatapy list-generators
```

### list-formats

List supported message formats:

```bash
testdatapy list-formats
```

## Examples

### Generate Customer Data

```bash
# Using Faker with Avro
testdatapy produce \
  --topic customers \
  --format avro \
  --schema-file examples/schemas/customer.avsc \
  --rate 100 \
  --count 10000

# Using CSV replay
testdatapy produce \
  --topic customers \
  --generator csv \
  --csv-file examples/data/CustomerMasterData.csv \
  --key-field CustomerID \
  --rate 50
```

### Kubernetes Deployment

```bash
# Using mTLS configuration
testdatapy produce \
  --config examples/config/k8s-mtls.json \
  --topic events \
  --format json \
  --rate 1000
```

### Dry Run Testing

```bash
# Test without sending to Kafka
testdatapy produce \
  --topic test \
  --count 5 \
  --dry-run
```

## Development

### Setup Development Environment

```bash
# Install with dev dependencies
uv pip install -e .[dev]

# Install pre-commit hooks
make dev-setup
```

### Running Tests

```bash
# Run all tests
make test

# Run unit tests only
make test-unit

# Run integration tests
make test-integration

# Check linting and formatting
make check
```

### Code Quality

```bash
# Format code
make format

# Run linters
make lint

# Type checking
make type-check
```

## Docker Support

### Local Development Stack

```bash
# Start Kafka, Zookeeper, and Schema Registry
make docker-up

# Stop all services
make docker-down
```

### Building Docker Image

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml .
COPY src/ src/

RUN pip install .

ENTRYPOINT ["testdatapy"]
```

## Architecture

### Component Overview

```
┌─────────────┐     ┌──────────────┐     ┌────────────┐
│  Generator  │────▶│   Producer   │────▶│   Kafka    │
└─────────────┘     └──────────────┘     └────────────┘
       │                    │                    │
       ▼                    ▼                    ▼
  ┌─────────┐        ┌──────────┐       ┌──────────┐
  │  Faker  │        │   JSON   │       │  Topic   │
  │   CSV   │        │   Avro   │       │          │
  └─────────┘        └──────────┘       └──────────┘
```

### Key Components

1. **Generators**: Create test data
   - `FakerGenerator`: Uses Faker library
   - `CSVGenerator`: Reads from CSV files

2. **Producers**: Send data to Kafka
   - `JsonProducer`: Produces JSON messages
   - `AvroProducer`: Produces Avro messages with Schema Registry

3. **Configuration**: Flexible configuration system
   - File-based configuration
   - Environment variables
   - CLI options

4. **Rate Limiting**: Control data generation rate
   - Token bucket algorithm
   - Configurable rates and bursts

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

MIT License - see LICENSE file for details

## Acknowledgments

- Built with [Confluent Kafka Python](https://github.com/confluentinc/confluent-kafka-python)
- Data generation powered by [Faker](https://github.com/joke2k/faker)
- CLI interface using [Click](https://github.com/pallets/click)
