# Correlated Data Generation with TestDataPy

This directory demonstrates advanced correlated data generation using TestDataPy's YAML-based configuration system with support for both JSON and Protocol Buffers (protobuf) formats.

## Overview

Generate realistic e-commerce data with proper relationships between customers, orders, and payments. All data structures are defined declaratively in YAML, using TestDataPy's correlation engine for:

- **Referential integrity**: Orders reference valid customers, payments reference valid orders
- **Realistic distributions**: Zipf distribution for customer behavior (some customers order more)
- **Temporal relationships**: Recency bias ensures recent orders are paid first
- **Field derivation**: Payment amounts automatically match order amounts
- **Format flexibility**: Support for JSON and binary protobuf with Schema Registry
- **Faker integration**: Realistic names, emails, and other data

## Quick Start

### Prerequisites

1. **Install TestDataPy**:

   ```bash
   # From project root
   pip install -e .
   # or
   uv pip install -e .
   ```

2. **Start Kafka with Schema Registry**:

   ```bash
   # From project root
   make docker-up
   
   # Verify services are running
   curl http://localhost:8081/subjects  # Schema Registry
   ```

3. **For Protobuf support, install additional dependencies**:
   ```bash
   pip install protobuf confluent-kafka[protobuf]
   
   # Check protoc compiler
   protoc --version
   # Install if needed (macOS): brew install protobuf
   ```

### Running Data Generation

#### JSON Format (Default)

```bash
cd docker-testdata-correlated

# Local development
./run-local.py

# Docker container
docker-compose up testdatapy-correlated

# Direct CLI
testdatapy correlated generate \
  --config configs/ecommerce-correlated.yaml \
  --bootstrap-servers localhost:9092
```

#### Protobuf Format

```bash
cd docker-testdata-correlated

# Generate data in protobuf format
python run-local-protobuf.py

# Dry run to preview data
python run-local-protobuf.py --dry-run

# Validate referential integrity
python run-local-protobuf.py --validate

# Custom configurations
python run-local-protobuf.py \
  --config configs/ecommerce-correlated.yaml \
  --kafka-config configs/local.json \
  --format protobuf
```

## Data Output and Formats

### Data Flow
```
customers (100 records) → orders (50 records) → payments (40 records)
     ↓                          ↓                      ↓
  Master Data              References            References
  (bulk loaded)            customers             orders
```

### Topics and Data Structure

1. **customers**: 100 customer records
   - Sequential IDs: `CUST_0001` to `CUST_0100`
   - Faker-generated names, emails, phone numbers
   - Address information (for protobuf: nested structure)
   - Tier classification (premium/regular/basic)

2. **orders**: 50 order records
   - Sequential IDs: `ORDER_00001` to `ORDER_00050`
   - References customers with Zipf distribution
   - Random amounts between $10-$1000
   - Status: "pending"

3. **payments**: 40 payment records
   - Sequential IDs: `PAY_000001` to `PAY_000040`
   - References recent orders (recency bias)
   - **Amount matches the referenced order exactly**
   - Random payment methods
   - Status: "completed"

### Format-Specific Features

| Feature | JSON | Protobuf |
|---------|------|----------|
| **Schema Registry** | Optional | Required |
| **Type Safety** | Runtime validation | Compile-time |
| **Message Size** | Larger (text) | Smaller (binary) |
| **Performance** | Good | Better |
| **Human Readable** | Yes | No (binary) |
| **Schema Evolution** | Limited | Full support |

## Configuration

### Main Configuration File

`configs/ecommerce-correlated.yaml` defines the data structure and relationships:

```yaml
master_data:
  customers:
    source: faker          # or "csv" for file-based data
    count: 100
    kafka_topic: customers
    id_field: customer_id
    bulk_load: true        # Load all master data at once
    schema:
      customer_id:
        type: string
        format: "CUST_{seq:04d}"
      name:
        type: faker
        method: name
      email:
        type: faker
        method: email
      # Address fields (for protobuf: mapped to nested Address message)
      street:
        type: faker
        method: street_address
      city:
        type: faker
        method: city
      postal_code:
        type: faker
        method: postcode
      country_code:
        type: faker
        method: country_code
      tier:
        type: choice
        choices: ["premium", "regular", "basic"]

transactional_data:
  orders:
    kafka_topic: orders
    rate_per_second: 10
    max_messages: 50
    track_recent: true     # Enable for payment references
    relationships:
      customer_id:
        references: customers.customer_id
        distribution: zipf  # Realistic customer behavior
        alpha: 1.5
    derived_fields:
      order_id:
        type: string
        format: "ORDER_{seq:05d}"
      total_amount:
        type: float
        min: 10.0
        max: 1000.0
    
  payments:
    kafka_topic: payments
    rate_per_second: 8
    max_messages: 40
    relationships:
      order_id:
        references: orders.order_id
        recency_bias: true  # Pay recent orders first
        max_delay_minutes: 30
    derived_fields:
      payment_id:
        type: string
        format: "PAY_{seq:06d}"
      amount:
        type: reference     # Copy from referenced order
        source: orders.total_amount
        via: order_id
      payment_method:
        type: choice
        choices: ["credit_card", "debit_card", "paypal", "bank_transfer"]
```

### Kafka Configuration

- `configs/local.json`: Local development with Schema Registry
  ```json
  {
    "bootstrap.servers": "localhost:9092",
    "schema.registry.url": "http://localhost:8081"
  }
  ```

## Protobuf Support

### Schema Files

Protobuf schemas are located in `src/testdatapy/schemas/protobuf/`:
- `customer.proto` - Customer message with nested Address
- `order.proto` - Order message structure  
- `payment.proto` - Payment message structure

### Compiling Schemas

Compile protobuf schemas before first use:
```bash
cd src/testdatapy/schemas/protobuf
protoc --python_out=. *.proto
```

This generates `*_pb2.py` files with Python protobuf classes.

### How Protobuf Works

1. **ProtobufProducer Class**: Handles protobuf serialization with Schema Registry
2. **Schema Registration**: Automatic registration with naming pattern `<topic>-value`
3. **Message Flow**: `Python Dict → Protobuf Message → Binary → Kafka`
4. **Nested Messages**: Address fields are automatically mapped to nested Address message

## Validation & Verification

### Configuration Validation

```bash
testdatapy correlated validate configs/ecommerce-correlated.yaml
```

### Data Verification

```bash
# Validate referential integrity
python scripts/validate-correlated.py

# Verify protobuf format and parsing
python verify-protobuf.py

# Check specific topic
python verify-protobuf.py --topic customers --max-messages 5
```

### View Sample Data

#### JSON Format
```bash
# View customers
docker exec testdatapy-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic customers \
  --from-beginning \
  --max-messages 3

# Check payment-order amount matching
docker exec testdatapy-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic payments \
  --from-beginning \
  --max-messages 5 | jq '{payment_id, order_id, amount}'
```

#### Protobuf Format
```bash
# View protobuf messages (requires Schema Registry)
kafka-protobuf-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic customers \
  --from-beginning \
  --property schema.registry.url=http://localhost:8081
```

## Project Structure

```
docker-testdata-correlated/
├── configs/
│   ├── ecommerce-correlated.yaml    # Main data configuration
│   ├── correlation-config-verbose.yaml  # Detailed example
│   ├── local.json                   # Kafka + Schema Registry config
│   └── kafka-config.json            # Docker environment config
├── scripts/
│   ├── produce-correlated.py        # Alternative YAML-based runner
│   ├── produce-correlated_csv.py    # CSV integration example
│   └── validate-correlated.py       # Data validation utility
├── protos/
│   ├── ecommerce.proto              # Legacy demo schema
│   └── ecommerce_pb2.py             # Compiled Python classes
├── run-local.py                     # JSON format runner
├── run-local-protobuf.py            # Protobuf format runner  
├── verify-protobuf.py               # Protobuf validation tool
├── demo-protobuf-producer.py        # API demonstration
├── working-protobuf-demo.py         # Working protobuf example
├── check-topics.py                  # Topic listing utility
├── clean-topics.py                  # Topic cleanup utility
└── docker-compose.yml               # Docker services
```

## Key Features

### 1. Payment-Order Correlation

Payments automatically reference order amounts using field derivation:
```yaml
amount:
  type: reference
  source: orders.total_amount
  via: order_id
```

### 2. Realistic Distributions

Customer selection uses Zipf distribution to simulate real behavior:
```yaml
customer_id:
  references: customers.customer_id
  distribution: zipf
  alpha: 1.5
```

### 3. Temporal Relationships

Payments prefer recent orders with recency bias:
```yaml
order_id:
  references: orders.order_id
  recency_bias: true
  max_delay_minutes: 30
```

### 4. Schema Registry Integration

Protobuf messages are automatically registered and versioned:
- Subjects: `customers-value`, `orders-value`, `payments-value`
- Schema evolution support
- Binary serialization with metadata

## Consuming Data

### Python Protobuf Consumer Example

```python
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Configure Schema Registry
schema_registry_client = SchemaRegistryClient({'url': 'http://localhost:8081'})

# Create deserializer (note: use specific protobuf class)
from testdatapy.schemas.protobuf import customer_pb2
deserializer = ProtobufDeserializer(
    customer_pb2.Customer,
    schema_registry_client
)

# Consumer setup
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'protobuf-consumer',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['customers'])

# Consume messages
while True:
    msg = consumer.poll(1.0)
    if msg is not None and not msg.error():
        ctx = SerializationContext(msg.topic(), MessageField.VALUE)
        customer = deserializer(msg.value(), ctx)
        print(f"Customer: {customer.name} ({customer.customer_id})")
        print(f"Address: {customer.address.street}, {customer.address.city}")
```

## Troubleshooting

### Common Issues

#### "testdatapy command not found"
Install TestDataPy from the project root:
```bash
pip install -e .
```

#### "Cannot connect to Kafka"
Ensure services are running:
```bash
make docker-up
# Check status
docker-compose ps
```

#### "Schema Registry connection failed"
Verify Schema Registry is accessible:
```bash
curl http://localhost:8081/subjects
docker-compose logs schema-registry
```

#### Protobuf Version Compatibility
If you see protobuf version warnings:
```bash
# Check versions
protoc --version
pip show protobuf

# Option 1: Recompile schemas
cd src/testdatapy/schemas/protobuf
protoc --python_out=. *.proto

# Option 2: Set environment variable
export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python
```

#### Empty Topics After Generation
Check logs for errors and verify topic creation:
```bash
python check-topics.py
# Clean and retry if needed
python clean-topics.py
```

#### Validation Failures
Clean topics and regenerate data:
```bash
python clean-topics.py
python run-local-protobuf.py --validate
```

### Data Format Issues

#### Messages appear as JSON instead of protobuf
This indicates fallback to JSON producer. Check:
1. Protobuf schemas are compiled
2. Schema Registry is accessible
3. No import errors in protobuf classes

#### Protobuf parsing failures
Use the verification tool to diagnose:
```bash
python verify-protobuf.py --topic customers
python debug-protobuf-serialization.py  # For detailed analysis
```

## Advanced Usage

### Custom Schema Evolution
Modify `.proto` files and observe schema compatibility:
```bash
# Edit customer.proto to add new fields
# Recompile
protoc --python_out=. customer.proto
# Generate data with new schema
python run-local-protobuf.py
```

### Performance Comparison
Compare JSON vs protobuf performance:
```bash
# Generate JSON data
time python run-local.py

# Generate protobuf data  
time python run-local-protobuf.py

# Check message sizes in Kafka
```

### Multi-Environment Configurations
Create environment-specific configs:
- `configs/ecommerce-dev.yaml`: Small datasets for development
- `configs/ecommerce-test.yaml`: Medium datasets for testing  
- `configs/ecommerce-prod.yaml`: Production-scale configurations

### Extending Schemas
Add new fields to any entity in the YAML configuration:
```yaml
derived_fields:
  shipping_address:
    type: faker
    method: address
  priority:
    type: choice
    choices: ["standard", "express", "overnight"]
  created_timestamp:
    type: timestamp
    format: iso8601
```

For protobuf, also update the corresponding `.proto` file and recompile.

## Benefits of Each Format

### JSON Format
- ✅ Human readable
- ✅ Easy debugging
- ✅ Simple tooling
- ✅ No compilation step
- ❌ Larger message size
- ❌ No compile-time type checking

### Protobuf Format  
- ✅ Compact binary format
- ✅ Strong type safety
- ✅ Schema evolution support
- ✅ Better performance
- ✅ Cross-language compatibility
- ❌ Requires schema compilation
- ❌ Not human readable
- ❌ Requires Schema Registry

Choose the format that best fits your use case and infrastructure requirements.