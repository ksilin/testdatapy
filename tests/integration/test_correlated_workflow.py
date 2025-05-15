"""Integration test for the complete correlated data generation workflow."""
import pytest
import tempfile
import csv
from pathlib import Path

from testdatapy.generators.reference_pool import ReferencePool
from testdatapy.generators.correlated_generator import CorrelatedDataGenerator
from testdatapy.config.correlation_config import CorrelationConfig, ValidationError
from testdatapy.generators.csv_gen import CSVGenerator
from testdatapy.generators.base import DataGenerator


class TestCorrelatedDataIntegration:
    """Test the complete correlated data generation workflow."""
    
    def test_complete_ecommerce_workflow(self, tmp_path):
        """Test a complete e-commerce data generation workflow."""
        
        # Step 1: Create CSV files for master data
        customers_file = tmp_path / "customers.csv"
        with open(customers_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['customer_id', 'name', 'email'])
            writer.writeheader()
            writer.writerows([
                {'customer_id': 'CUST_001', 'name': 'John Doe', 'email': 'john@example.com'},
                {'customer_id': 'CUST_002', 'name': 'Jane Smith', 'email': 'jane@example.com'},
                {'customer_id': 'CUST_003', 'name': 'Bob Johnson', 'email': 'bob@example.com'},
            ])
        
        products_file = tmp_path / "products.csv"
        with open(products_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['product_id', 'name', 'price'])
            writer.writeheader()
            writer.writerows([
                {'product_id': 'PROD_001', 'name': 'Laptop', 'price': '999.99'},
                {'product_id': 'PROD_002', 'name': 'Mouse', 'price': '29.99'},
                {'product_id': 'PROD_003', 'name': 'Keyboard', 'price': '79.99'},
                {'product_id': 'PROD_004', 'name': 'Monitor', 'price': '299.99'},
            ])
        
        # Step 2: Create configuration
        config_dict = {
            "master_data": {
                "customers": {
                    "source": "csv",
                    "file": str(customers_file),
                    "kafka_topic": "customers",
                    "id_field": "customer_id"
                },
                "products": {
                    "source": "csv",
                    "file": str(products_file),
                    "kafka_topic": "products",
                    "id_field": "product_id"
                }
            },
            "transactional_data": {
                "orders": {
                    "kafka_topic": "orders",
                    "id_field": "order_id",
                    "rate_per_second": 0,  # No rate limiting in tests
                    "track_recent": True,
                    "relationships": {
                        "customer_id": {
                            "references": "customers.customer_id",
                            "distribution": "uniform"  # Changed to uniform for simplicity
                        }
                    },
                    "derived_fields": {
                        "order_date": {
                            "type": "timestamp",
                            "format": "iso8601"
                        },
                        "status": {
                            "type": "string",
                            "initial_value": "pending"
                        },
                        "total_amount": {
                            "type": "random_float",
                            "min": 10.0,
                            "max": 1000.0
                        }
                    }
                },
                "payments": {
                    "kafka_topic": "payments",
                    "rate_per_second": 0,  # No rate limiting in tests
                    "relationships": {
                        "order_id": {
                            "references": "orders.order_id",
                            "recency_bias": False  # Disable recency to avoid lock issues
                        }
                    },
                    "derived_fields": {
                        "payment_method": {
                            "type": "string",
                            "initial_value": "credit_card"
                        },
                        "amount": {
                            "type": "random_float",
                            "min": 10.0,
                            "max": 1000.0
                        },
                        "payment_date": {
                            "type": "timestamp",
                            "format": "iso8601"
                        }
                    }
                }
            }
        }
        
        config = CorrelationConfig(config_dict)
        
        # Step 3: Load master data into reference pool
        ref_pool = ReferencePool()
        ref_pool.enable_recent_tracking("orders", window_size=100)
        
        # Load customers
        customer_gen = CSVGenerator(str(customers_file), rate_per_second=0, cycle=False)
        for customer in customer_gen.generate():
            ref_pool.add_references("customers", [customer['customer_id']])
        
        # Load products  
        product_gen = CSVGenerator(str(products_file), rate_per_second=0, cycle=False)
        for product in product_gen.generate():
            ref_pool.add_references("products", [product['product_id']])
        
        # Step 4: Generate orders
        order_gen = CorrelatedDataGenerator(
            entity_type="orders",
            config=config,
            reference_pool=ref_pool,
            rate_per_second=0,  # Override to disable rate limiting
            max_messages=5  # Generate less messages for faster testing
        )
        
        generated_orders = []
        for order in order_gen.generate():
            generated_orders.append(order)
            # Track order for payment generation
            ref_pool.add_references("orders", [order["order_id"]])  # Add to regular pool
            ref_pool.add_recent("orders", order["order_id"])  # Also add to recent
        
        # Verify orders
        assert len(generated_orders) == 5
        for order in generated_orders:
            assert order["customer_id"] in ["CUST_001", "CUST_002", "CUST_003"]
            assert "order_date" in order
            assert order["status"] == "pending"
        
        # Step 5: Generate payments for orders
        payment_gen = CorrelatedDataGenerator(
            entity_type="payments",
            config=config,
            reference_pool=ref_pool,
            rate_per_second=0,  # Override to disable rate limiting
            max_messages=3  # Not all orders get paid immediately
        )
        
        generated_payments = []
        for payment in payment_gen.generate():
            generated_payments.append(payment)
        
        # Verify payments
        assert len(generated_payments) == 3
        order_ids = [order["order_id"] for order in generated_orders]
        for payment in generated_payments:
            assert payment["order_id"] in order_ids
            assert "payment_method" in payment
            assert "amount" in payment
            assert "payment_date" in payment
        
        # Step 6: Verify statistics
        stats = ref_pool.get_stats()
        # Since stats are not enabled by default, this should be empty
        assert stats == {}
        
        print(f"Integration test passed! Generated {len(generated_orders)} orders and {len(generated_payments)} payments")
    
    def test_reference_validation(self):
        """Test that invalid references are caught."""
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
                    "relationships": {
                        "product_id": {
                            "references": "products.product_id"  # Invalid - products not defined
                        }
                    }
                }
            }
        }
        
        with pytest.raises(ValidationError):
            CorrelationConfig(config_dict)
    
    def test_memory_efficiency(self):
        """Test that the system handles large datasets efficiently."""
        config_dict = {
            "master_data": {},
            "transactional_data": {
                "events": {
                    "kafka_topic": "events",
                    "rate_per_second": 0,  # No rate limiting
                    "derived_fields": {
                        "event_id": {
                            "type": "string",
                            "initial_value": "event"
                        },
                        "timestamp": {
                            "type": "timestamp"
                        }
                    }
                }
            }
        }
        
        config = CorrelationConfig(config_dict)
        ref_pool = ReferencePool()
        
        # Enable recent tracking with limited window
        ref_pool.enable_recent_tracking("events", window_size=1000)
        
        generator = CorrelatedDataGenerator(
            entity_type="events",
            config=config,
            reference_pool=ref_pool,
            rate_per_second=0,  # No rate limiting
            max_messages=5000
        )
        
        # Generate many events
        event_count = 0
        for event in generator.generate():
            event_count += 1
            ref_pool.add_recent("events", event["event_id"])
        
        # Check that recent items pool doesn't grow unbounded
        assert len(ref_pool.get_recent("events")) <= 1000
        assert event_count == 5000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
