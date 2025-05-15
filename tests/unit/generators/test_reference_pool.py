"""Tests for ReferencePool - the core component for managing ID references."""
import pytest
from unittest.mock import Mock
import threading
import time

from testdatapy.generators.reference_pool import ReferencePool


class TestReferencePool:
    """Test the ReferencePool class for managing correlated IDs."""
    
    def test_create_empty_pool(self):
        """Test creating an empty reference pool."""
        pool = ReferencePool()
        assert pool.is_empty()
        assert pool.size() == 0
    
    def test_add_references_single_type(self):
        """Test adding references of a single type."""
        pool = ReferencePool()
        customer_ids = ["CUST_001", "CUST_002", "CUST_003"]
        
        pool.add_references("customers", customer_ids)
        
        assert not pool.is_empty()
        assert pool.size() == 3
        assert pool.has_type("customers")
        assert not pool.has_type("products")
    
    def test_add_references_multiple_types(self):
        """Test adding references of multiple types."""
        pool = ReferencePool()
        customer_ids = ["CUST_001", "CUST_002"]
        product_ids = ["PROD_001", "PROD_002", "PROD_003"]
        
        pool.add_references("customers", customer_ids)
        pool.add_references("products", product_ids)
        
        assert pool.size() == 5
        assert pool.has_type("customers")
        assert pool.has_type("products")
        assert pool.get_type_count("customers") == 2
        assert pool.get_type_count("products") == 3
    
    def test_get_random_reference(self):
        """Test getting a random reference from the pool."""
        pool = ReferencePool()
        customer_ids = ["CUST_001", "CUST_002", "CUST_003"]
        pool.add_references("customers", customer_ids)
        
        # Get random customer
        random_customer = pool.get_random("customers")
        assert random_customer in customer_ids
        
        # Test invalid type
        with pytest.raises(ValueError):
            pool.get_random("invalid_type")
    
    def test_get_multiple_random_references(self):
        """Test getting multiple random references."""
        pool = ReferencePool()
        product_ids = ["PROD_001", "PROD_002", "PROD_003", "PROD_004", "PROD_005"]
        pool.add_references("products", product_ids)
        
        # Get 3 random products
        random_products = pool.get_random_multiple("products", count=3)
        assert len(random_products) == 3
        assert len(set(random_products)) == 3  # All unique
        assert all(p in product_ids for p in random_products)
        
        # Request more than available
        all_products = pool.get_random_multiple("products", count=10)
        assert len(all_products) == 5  # Should return all available


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
