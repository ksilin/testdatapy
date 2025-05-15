#!/usr/bin/env python3
"""Simple test runner to check our ReferencePool implementation."""

import sys
import os

# Add the src directory to Python path
sys.path.insert(0, '/Users/ksilin/Code/workspaces/confluent/python/testdatapy/src')

# Now import and run a simple test
try:
    from testdatapy.generators.reference_pool import ReferencePool
    
    # Test 1: Create empty pool
    pool = ReferencePool()
    assert pool.is_empty()
    assert pool.size() == 0
    print("✓ Test 1 passed: Create empty pool")
    
    # Test 2: Add references
    pool.add_references("customers", ["CUST_001", "CUST_002", "CUST_003"])
    assert not pool.is_empty()
    assert pool.size() == 3
    assert pool.has_type("customers")
    print("✓ Test 2 passed: Add references")
    
    # Test 3: Get random reference
    random_customer = pool.get_random("customers")
    assert random_customer in ["CUST_001", "CUST_002", "CUST_003"]
    print("✓ Test 3 passed: Get random reference")
    
    # Test 4: Get multiple random
    products = ["PROD_001", "PROD_002", "PROD_003", "PROD_004", "PROD_005"]
    pool.add_references("products", products)
    random_products = pool.get_random_multiple("products", count=3)
    assert len(random_products) == 3
    print("✓ Test 4 passed: Get multiple random references")
    
    # Test 5: Recent tracking
    pool.enable_recent_tracking("orders", window_size=3)
    pool.add_recent("orders", "ORDER_001")
    pool.add_recent("orders", "ORDER_002")
    recent = pool.get_recent("orders")
    assert recent == ["ORDER_001", "ORDER_002"]
    print("✓ Test 5 passed: Recent tracking")
    
    print("\nAll basic tests passed! 🎉")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
