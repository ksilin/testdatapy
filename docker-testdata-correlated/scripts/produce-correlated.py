#!/usr/bin/env python3
"""
Wrapper script for correlated data generation using YAML configuration.
"""

import subprocess
import sys
import os
import time

def main():
    """Run correlated data generation using the YAML configuration."""
    
    print("Starting correlated data generation...")
    print("=" * 50)
    
    # Wait a bit for Kafka to be fully ready in Docker environment
    # TODO - test first if kafka is ready before waiting
    if os.environ.get('DOCKER_ENV'):
        print("Waiting for Kafka to be ready...")
        time.sleep(5)
    
    cmd = [
        "testdatapy", "correlated", "generate",
        "--config", "/config/ecommerce-correlated.yaml",
        "--producer-config", "/config/kafka-config.json"
    ]
    
    print(f"Running command: {' '.join(cmd)}")
    print("=" * 50)
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
            text=True,
            capture_output=False  # Stream output directly
        )
        
        print("=" * 50)
        print("Correlated data production complete!")
        return 0
        
    except subprocess.CalledProcessError as e:
        print(f"Error running correlated generation: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())