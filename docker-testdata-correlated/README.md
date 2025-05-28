# Correlated Data Generation with TestDataPy

This directory demonstrates advanced correlated data generation using TestDataPy's YAML-based configuration system.

## Overview

Generate realistic e-commerce data with proper relationships between customers, orders, and payments. All data structures are defined declaratively in YAML, using TestDataPy's correlation engine for:

- **Referential integrity**: Orders reference valid customers, payments reference valid orders
- **Realistic distributions**: Zipf distribution for customer behavior (some customers order more)
- **Temporal relationships**: Recency bias ensures recent orders are paid first
- **Field derivation**: Payment amounts automatically match order amounts
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

2. **Start Kafka**:

   ```bash
   # From project root
   make docker-up
   ```

### Running Data Generation

#### Option 1: Local Development

```bash
cd docker-testdata-correlated
./run-local.py
```

#### Option 2: Docker Container

```bash
docker-compose up testdatapy-correlated
```

#### Option 3: Direct CLI

```bash
testdatapy correlated generate \
  --config configs/ecommerce-correlated.yaml \
  --bootstrap-servers localhost:9092
```

#### Option 4: Kubernetes

```bash
kubectl apply -f ../k8s/correlated-job-yaml.yaml
```

## What Gets Generated

### Data Flow
```
customers (100 records) → orders (50 records) → payments (40 records)
     ↓                          ↓                      ↓
  Master Data              References            References
  (bulk loaded)            customers             orders
```

### Topics and Data

1. **customers**: 100 customer records
   - Sequential IDs: `CUST_0001` to `CUST_0100`
   - Faker-generated names, emails, phone numbers
   - Random countries

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

## Configuration

### Main Configuration File

`configs/ecommerce-correlated.yaml` defines:

```yaml
master_data:
  customers:
    source: faker          # or "csv" for file-based data
    count: 100
    kafka_topic: customers
    schema:
      customer_id:
        type: string
        format: "CUST_{seq:04d}"
      name:
        type: faker
        method: name

transactional_data:
  orders:
    kafka_topic: orders
    rate_per_second: 10
    track_recent: true     # Enable for payment references
    relationships:
      customer_id:
        references: customers.customer_id
        distribution: zipf  # Realistic customer behavior
    
  payments:
    kafka_topic: payments
    relationships:
      order_id:
        references: orders.order_id
        recency_bias: true  # Pay recent orders first
    derived_fields:
      amount:
        type: reference     # Copy from referenced order
        source: orders.total_amount
        via: order_id
```

### Kafka Configuration

- `configs/kafka-local.json`: Local development (localhost:9092)
- `configs/kafka-config.json`: Docker environment (kafka:29092)

## Validation & Verification

### Validate Configuration

```bash
testdatapy correlated validate configs/ecommerce-correlated.yaml
```

### Verify Generated Data

```bash
python scripts/validate-correlated.py
```

### View Sample Data

```bash
# List topics
docker exec testdatapy-kafka kafka-topics --bootstrap-server localhost:9092 --list

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

## Project Structure

```
docker-testdata-correlated/
├── configs/
│   ├── ecommerce-correlated.yaml    # Data structure definitions
│   ├── kafka-local.json             # Local Kafka config
│   └── kafka-config.json            # Docker Kafka config
├── scripts/
│   ├── produce-correlated.py        # YAML-based wrapper
│   ├── validate-correlated.py       # Data validation
│   ├── compare-approaches.py        # Legacy comparison
│   └── clean-topics.sh              # Topic cleanup utility
├── docker-compose.yml               # Docker setup
└── run-local.py                     # Local execution script
```

## Key Features

### 1. Payment-Order Correlation

Payments automatically reference order amounts using the `type: reference` field:
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

Payments prefer recent orders:

```yaml
order_id:
  references: orders.order_id
  recency_bias: true
  max_delay_minutes: 30
```

## Troubleshooting

### "testdatapy command not found"

Install TestDataPy from the project root:
```bash
pip install -e .
```

### "Cannot connect to Kafka"

Ensure Kafka is running:

```bash
make docker-up
# or
docker-compose -f ../docker/docker-compose.yml up -d
```

### Empty topics after generation

Check the generation logs for errors. The script provides detailed output including delivery confirmations.

### Validation failures

If validation shows mismatched IDs, clean topics and regenerate:
```bash
./scripts/clean-topics.sh
testdatapy correlated generate --config configs/ecommerce-correlated.yaml
```

## Advanced Usage

### Custom Configurations

Create variations for different environments:
- `configs/ecommerce-dev.yaml`: Small datasets for development
- `configs/ecommerce-test.yaml`: Medium datasets for testing
- `configs/ecommerce-prod.yaml`: Production-scale configurations

### Extending the Schema

Add new fields to any entity:

```yaml
derived_fields:
  shipping_address:
    type: faker
    method: address
  priority:
    type: choice
    choices: ["standard", "express", "overnight"]
```