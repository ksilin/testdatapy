"""Tests for the CorrelatedDataGenerator class."""
import pytest
from unittest.mock import Mock, patch
import time
from typing import Dict, Any, List

from testdatapy.generators.correlated_generator import CorrelatedDataGenerator
from testdatapy.generators.reference_pool import ReferencePool
from testdatapy.config.correlation_config import CorrelationConfig


class TestCorrelatedDataGenerator:
    """Test the CorrelatedDataGenerator class."""
    
    def test_create_generator_with_config(self):
        """Test creating a generator with configuration."""
        config_dict = {
            "master_data": {
                "customers": {
                    "kafka_topic": "customers",
                    "id_field": "customer_id"
                }
            },
            "transactional_data": {
                "orders": {
                    "kafka_topic": "orders",
                    "rate_per_second": 10,
                    "relationships": {
                        "customer_id": {
                            "references": "customers.customer_id"
                        }
                    }
                }
            }
        }
        
        config = CorrelationConfig(config_dict)
        ref_pool = ReferencePool()
        ref_pool.add_references("customers", ["CUST_001", "CUST_002"])
        
        generator = CorrelatedDataGenerator(
            entity_type="orders",
            config=config,
            reference_pool=ref_pool
        )
        
        assert generator.entity_type == "orders"
        assert generator.rate_per_second == 10
    
    def test_generate_with_simple_reference(self):
        """Test generating data with a simple reference."""
        config_dict = {
            "master_data": {
                "customers": {
                    "kafka_topic": "customers",
                    "id_field": "customer_id"
                }
            },
            "transactional_data": {
                "orders": {
                    "kafka_topic": "orders",
                    "rate_per_second": 10,
                    "relationships": {
                        "customer_id": {
                            "references": "customers.customer_id"
                        }
                    }
                }
            }
        }
        
        config = CorrelationConfig(config_dict)
        ref_pool = ReferencePool()
        ref_pool.add_references("customers", ["CUST_001", "CUST_002", "CUST_003"])
        
        generator = CorrelatedDataGenerator(
            entity_type="orders",
            config=config,
            reference_pool=ref_pool,
            max_messages=1
        )
        
        # Generate one order
        orders = list(generator.generate())
        assert len(orders) == 1
        
        order = orders[0]
        assert "customer_id" in order
        assert order["customer_id"] in ["CUST_001", "CUST_002", "CUST_003"]
    
    def test_generate_with_array_relationship(self):
        """Test generating data with array relationships."""
        config_dict = {
            "master_data": {
                "customers": {
                    "kafka_topic": "customers",
                    "id_field": "customer_id"
                },
                "products": {
                    "kafka_topic": "products",
                    "id_field": "product_id"
                }
            },
            "transactional_data": {
                "orders": {
                    "kafka_topic": "orders",
                    "rate_per_second": 10,
                    "relationships": {
                        "customer_id": {
                            "references": "customers.customer_id"
                        },
                        "order_items": {
                            "type": "array",
                            "min_items": 1,
                            "max_items": 3,
                            "item_schema": {
                                "product_id": {
                                    "references": "products.product_id"
                                },
                                "quantity": {
                                    "type": "integer",
                                    "min": 1,
                                    "max": 5
                                }
                            }
                        }
                    }
                }
            }
        }
        
        config = CorrelationConfig(config_dict)
        ref_pool = ReferencePool()
        ref_pool.add_references("customers", ["CUST_001"])
        ref_pool.add_references("products", ["PROD_001", "PROD_002", "PROD_003"])
        
        generator = CorrelatedDataGenerator(
            entity_type="orders",
            config=config,
            reference_pool=ref_pool,
            max_messages=1
        )
        
        orders = list(generator.generate())
        order = orders[0]
        
        assert "order_items" in order
        assert isinstance(order["order_items"], list)
        assert 1 <= len(order["order_items"]) <= 3
        
        for item in order["order_items"]:
            assert "product_id" in item
            assert item["product_id"] in ["PROD_001", "PROD_002", "PROD_003"]
            assert "quantity" in item
            assert 1 <= item["quantity"] <= 5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
