#!/usr/bin/env python3
"""
Local integration test suite for protobuf functionality.

This script tests the complete protobuf workflow:
1. Clean topics
2. Generate and produce protobuf data
3. Validate messages are binary protobuf (not JSON)
4. Test correlated data relationships
5. Verify Schema Registry integration
"""

import sys
import time
import json
import subprocess
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from testdatapy.validators.protobuf_validator import validate_protobuf_production
from testdatapy.schemas.protobuf import customer_pb2, order_pb2, payment_pb2


def run_command(command, description):
    """Run a shell command and return the result."""
    print(f"\n{description}")
    print(f"Running: {command}")
    
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"❌ Failed: {result.stderr}")
        return False
    else:
        print(f"✅ Success: {result.stdout[:200]}...")
        return True


def test_basic_protobuf_cli():
    """Test basic protobuf CLI functionality."""
    print("\n" + "="*60)
    print("Testing Basic Protobuf CLI")
    print("="*60)
    
    # Test protobuf with main CLI
    commands = [
        (
            "python -m testdatapy.cli produce test_basic_protobuf "
            "--format protobuf "
            "--proto-class testdatapy.schemas.protobuf.customer_pb2.Customer "
            "--schema-registry-url http://localhost:8081 "
            "--generator faker "
            "--count 10 "
            "--rate 5",
            "Generate 10 customers with basic CLI"
        )
    ]
    
    for command, description in commands:
        if not run_command(command, description):
            return False
    
    return True


def test_correlated_protobuf():
    """Test correlated data generation with protobuf."""
    print("\n" + "="*60)
    print("Testing Correlated Protobuf Data")
    print("="*60)
    
    # Create temporary correlation config
    config_content = """
master_data:
  customers:
    source: faker
    count: 5
    kafka_topic: test_corr_customers
    id_field: customer_id
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
      tier:
        type: string
        choices: ["gold", "silver", "bronze"]

transactional_data:
  orders:
    kafka_topic: test_corr_orders
    rate_per_second: 10
    max_messages: 10
    relationships:
      customer_id:
        references: "customers.customer_id"
    derived_fields:
      order_id:
        type: string
        format: "ORDER_{seq:04d}"
      total_amount:
        type: float
        min: 10.0
        max: 1000.0
      status:
        type: string
        initial_value: "pending"
        
  payments:
    kafka_topic: test_corr_payments
    rate_per_second: 8
    max_messages: 10
    relationships:
      order_id:
        references: "orders.order_id"
        recency_bias: true
    derived_fields:
      payment_id:
        type: string
        format: "PAY_{seq:04d}"
      amount:
        type: reference
        source: "orders.total_amount"
        via: "order_id"
      payment_method:
        type: string
        choices: ["credit_card", "debit_card", "paypal"]
      status:
        type: string
        initial_value: "completed"
"""
    
    # Write config to temporary file
    config_path = "/tmp/test_correlated_config.yaml"
    with open(config_path, 'w') as f:
        f.write(config_content)
    
    # Test correlated data generation with protobuf
    command = (
        f"python -m testdatapy.cli_correlated correlated generate "
        f"--config {config_path} "
        f"--format protobuf "
        f"--schema-registry-url http://localhost:8081 "
        f"--bootstrap-servers localhost:9092"
    )
    
    return run_command(command, "Generate correlated protobuf data")


def test_protobuf_validation():
    """Test protobuf data validation."""
    print("\n" + "="*60)
    print("Testing Protobuf Data Validation")
    print("="*60)
    
    # Define topics to validate
    topics = [
        "test_basic_protobuf",
        "test_corr_customers", 
        "test_corr_orders",
        "test_corr_payments"
    ]
    
    proto_classes = {
        "test_basic_protobuf": customer_pb2.Customer,
        "test_corr_customers": customer_pb2.Customer,
        "test_corr_orders": order_pb2.Order,
        "test_corr_payments": payment_pb2.Payment
    }
    
    try:
        validate_protobuf_production(
            bootstrap_servers="localhost:9092",
            topics=topics,
            proto_classes=proto_classes,
            schema_registry_url="http://localhost:8081",
            sample_size=20
        )
        return True
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False


def clean_test_topics():
    """Clean up test topics."""
    print("\n" + "="*60)
    print("Cleaning Test Topics")
    print("="*60)
    
    topics = [
        "test_basic_protobuf",
        "test_corr_customers",
        "test_corr_orders", 
        "test_corr_payments"
    ]
    
    for topic in topics:
        command = f"docker exec testdatapy-kafka-1 kafka-topics --bootstrap-server localhost:9092 --delete --topic {topic}"
        run_command(command, f"Delete topic {topic}")
    
    time.sleep(2)  # Allow deletion to complete


def check_kafka_available():
    """Check if Kafka is available."""
    print("Checking Kafka availability...")
    
    # Check if Kafka is running
    result = subprocess.run(
        "docker exec testdatapy-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list",
        shell=True,
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print("✅ Kafka is available")
        return True
    else:
        print("❌ Kafka is not available. Please start with: make docker-up")
        return False


def check_schema_registry_available():
    """Check if Schema Registry is available."""
    print("Checking Schema Registry availability...")
    
    import requests
    try:
        response = requests.get("http://localhost:8081/subjects", timeout=5)
        if response.status_code == 200:
            print("✅ Schema Registry is available")
            return True
        else:
            print(f"❌ Schema Registry returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Schema Registry is not available: {e}")
        return False


def main():
    """Run the complete protobuf integration test suite."""
    print("Protobuf Integration Test Suite")
    print("="*60)
    
    # Check prerequisites
    if not check_kafka_available():
        sys.exit(1)
    
    if not check_schema_registry_available():
        sys.exit(1)
    
    # Clean topics first
    clean_test_topics()
    
    # Run tests
    tests = [
        ("Basic Protobuf CLI", test_basic_protobuf_cli),
        ("Correlated Protobuf", test_correlated_protobuf),
        ("Protobuf Validation", test_protobuf_validation)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\n{'='*60}")
        print(f"Running: {test_name}")
        print('='*60)
        
        try:
            result = test_func()
            results[test_name] = result
            
            if result:
                print(f"✅ {test_name}: PASSED")
            else:
                print(f"❌ {test_name}: FAILED")
                
        except Exception as e:
            print(f"❌ {test_name}: ERROR - {e}")
            results[test_name] = False
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name}: {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All protobuf tests passed! The implementation is working correctly.")
        sys.exit(0)
    else:
        print("\n⚠️  Some tests failed. Please check the output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()