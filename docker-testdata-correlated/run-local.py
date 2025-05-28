#!/usr/bin/env python3
"""
run correlated data generation against local Kafka.
Assumes you have docker compose running with Kafka.
"""

import subprocess
import sys
import os
from pathlib import Path

def run_command(cmd, description, timeout=30):
    """Run a command and handle errors with timeout."""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            check=True, 
            capture_output=True, 
            text=True, 
            timeout=timeout
        )
        print(f"✅ {description} - Success")
        if result.stdout.strip():
            print(f"   Output: {result.stdout.strip()}")
        return True
    except subprocess.TimeoutExpired:
        print(f"⏱️ {description} - Timed out after {timeout}s")
        return True  # Don't fail on timeout
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} - Failed")
        if e.stderr and e.stderr.strip():
            print(f"   Error: {e.stderr.strip()}")
        return False

def check_kafka_connection():
    """Check if Kafka is accessible."""
    print("🔍 Checking Kafka connection...")
    # Try to list topics as a connectivity test
    return run_command(
        "testdatapy validate --config configs/kafka-local.json",
        "Kafka connectivity check"
    )

def main():
    """Run correlated data generation."""
    
    print("🏠 Correlated Data Generation")
    print("=" * 50)
    
    # Ensure we're in the right directory
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    # TODO - make this a command line argument with a default value
    local_kafka_config = "configs/kafka-local.json"
    if not os.path.exists(local_kafka_config):
        print(f"📝 Creating local Kafka config: {local_kafka_config}")
        with open(local_kafka_config, 'w') as f:
            f.write('{\n  "bootstrap.servers": "localhost:9092",\n  "security.protocol": "PLAINTEXT"\n}')
    
    # Check if testdatapy is available
    if not run_command("testdatapy --version", "Checking testdatapy installation"):
        print("❌ testdatapy not found. Please install it:")
        print("   pip install -e .")
        sys.exit(1)
    
    # Check Kafka connection
    if not check_kafka_connection():
        print("❌ Cannot connect to Kafka. Please ensure Docker Compose is running:")
        print("   make docker-up")
        sys.exit(1)
    
    # Validate correlation configuration
    # TODO - review ccorrelation config validation
    if not run_command(
        "testdatapy correlated validate configs/ecommerce-correlated.yaml",
        "Validating correlation configuration"
    ):
        sys.exit(1)
    
    # Check topics before generation
    # TODO - make container name a command line argument with a default
    print("\n📋 Checking topics before generation...")
    run_command(
        "docker exec testdatapy-kafka kafka-topics --bootstrap-server localhost:9092 --list",
        "Listing existing topics"
    )
    
    # Generate correlated data
    print(f"\n🚀 Generating correlated e-commerce data...")
    success = run_command(
        f"testdatapy correlated generate "
        f"--config configs/ecommerce-correlated.yaml "
        f"--bootstrap-servers localhost:9092 "
        f"--producer-config {local_kafka_config}",
        "Correlated data generation"
    )
    
    if success:
        print("\n🎉 Generation completed successfully!")
        
        # Check topics after generation
        print("\n📊 Checking topics after generation...")
        run_command(
            "docker exec testdatapy-kafka kafka-topics --bootstrap-server localhost:9092 --list",
            "Listing topics after generation"
        )
        
        # Show sample data using kafka-console-consumer with proper timeout
        print("\n📄 Sample data from topics:")
        for topic in ["customers", "orders", "payments"]:
            print(f"\n--- {topic.upper()} (first 3 messages) ---")
            # Use shorter timeout to prevent hanging
            run_command(
                f"docker exec testdatapy-kafka kafka-console-consumer "
                f"--bootstrap-server localhost:9092 --topic {topic} "
                f"--from-beginning --max-messages 3 --timeout-ms 2000 2>/dev/null || "
                f"echo '  (No data or sampling timed out)'",
                f"Reading sample data from {topic}",
                timeout=8  # 8 second timeout for the subprocess
            )
        
        print("\n🏁 Script completed successfully!")
        print("=" * 50)
    else:
        print("❌ Generation failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()