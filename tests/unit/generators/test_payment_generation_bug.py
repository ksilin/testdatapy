#!/usr/bin/env python3
"""
Test to demonstrate payment generation bug - payments should be generated 
but are currently missing from the correlated data generation workflow.
"""

import pytest
import yaml
from unittest.mock import Mock, patch
from testdatapy.generators.correlated_generator import CorrelatedDataGenerator
from testdatapy.generators.reference_pool import ReferencePool
from testdatapy.config.correlation_config import CorrelationConfig


class TestPaymentGenerationBug:
    """Test that demonstrates the payment generation bug."""
    
    @pytest.fixture
    def test_config(self):
        """Load the ecommerce config for testing."""
        config_data = {
            "master_data": {
                "customers": {
                    "source": "faker",
                    "count": 5,
                    "kafka_topic": "customers",
                    "id_field": "customer_id",
                    "bulk_load": True,
                    "schema": {
                        "customer_id": {"type": "string", "format": "CUST_{seq:04d}"},
                        "name": {"type": "faker", "method": "name"},
                        "email": {"type": "faker", "method": "email"}
                    }
                }
            },
            "transactional_data": {
                "orders": {
                    "kafka_topic": "orders",
                    "rate_per_second": 10,
                    "max_messages": 3,
                    "track_recent": True,
                    "relationships": {
                        "customer_id": {
                            "references": "customers.customer_id",
                            "distribution": "uniform"
                        }
                    },
                    "derived_fields": {
                        "order_id": {"type": "string", "format": "ORDER_{seq:05d}"},
                        "total_amount": {"type": "float", "min": 10.0, "max": 1000.0}
                    }
                },
                "payments": {
                    "kafka_topic": "payments", 
                    "rate_per_second": 8,
                    "max_messages": 2,
                    "relationships": {
                        "order_id": {
                            "references": "orders.order_id",
                            "recency_bias": True
                        }
                    },
                    "derived_fields": {
                        "payment_id": {"type": "string", "format": "PAY_{seq:06d}"},
                        "amount": {
                            "type": "reference",
                            "source": "orders.total_amount", 
                            "via": "order_id"
                        },
                        "payment_method": {
                            "type": "choice",
                            "choices": ["credit_card", "paypal"]
                        }
                    }
                }
            }
        }
        return CorrelationConfig(config_data)
    
    @pytest.fixture
    def reference_pool(self):
        """Create a reference pool for testing."""
        return ReferencePool()
    
    def test_payments_should_be_generated_after_orders(self, test_config, reference_pool):
        """
        Test that payments are generated after orders exist.
        This test should pass but currently fails because payments aren't generated.
        """
        # Mock producer
        mock_producer = Mock()
        
        # Generate customers first
        customers_generator = CorrelatedDataGenerator(
            entity_type="customers",
            config=test_config,
            reference_pool=reference_pool
        )
        
        # Generate some customers
        customer_records = []
        for record in customers_generator.generate():
            customer_records.append(record)
            if len(customer_records) >= 5:
                break
        
        assert len(customer_records) == 5
        for record in customer_records:
            assert record is not None
            assert "customer_id" in record
        
        # Generate orders
        orders_generator = CorrelatedDataGenerator(
            entity_type="orders", 
            config=test_config,
            reference_pool=reference_pool
        )
        
        generated_orders = []
        for record in orders_generator.generate():
            assert record is not None
            assert "order_id" in record
            assert "customer_id" in record
            generated_orders.append(record)
            if len(generated_orders) >= 3:
                break
        
        # Verify orders were tracked in reference pool
        assert reference_pool.has_type("orders"), "Orders should be available as references"
        order_count = reference_pool.get_type_count("orders")
        assert order_count > 0, f"Expected orders in reference pool, got {order_count}"
        
        # Generate payments - THIS SHOULD WORK but currently may fail
        payments_generator = CorrelatedDataGenerator(
            entity_type="payments",
            config=test_config, 
            reference_pool=reference_pool
        )
        
        generated_payments = []
        try:
            for record in payments_generator.generate():
                assert record is not None, "Payment record should be generated"
                assert "payment_id" in record, "Payment should have payment_id"
                assert "order_id" in record, "Payment should reference order_id" 
                assert "amount" in record, "Payment should have amount"
                assert "payment_method" in record, "Payment should have payment_method"
                
                # Verify the order_id references a real order
                assert record["order_id"] in [o["order_id"] for o in generated_orders], \
                    f"Payment order_id {record['order_id']} should reference existing order"
                
                generated_payments.append(record)
                if len(generated_payments) >= 2:
                    break
                
        except Exception as e:
            pytest.fail(f"Payment generation failed: {e}")
        
        # Verify we generated the expected number of payments
        assert len(generated_payments) == 2, f"Expected 2 payments, got {len(generated_payments)}"
        
        # Verify all fields have actual values (not None)
        for payment in generated_payments:
            assert payment["amount"] is not None, f"Payment amount should not be None: {payment}"
            assert payment["payment_method"] is not None, f"Payment method should not be None: {payment}"
            assert isinstance(payment["amount"], (int, float)), f"Amount should be numeric: {payment}"
            assert payment["payment_method"] in ["credit_card", "paypal"], f"Invalid payment method: {payment}"
        
        print(f"✅ Generated {len(generated_payments)} payments successfully")
        for payment in generated_payments:
            print(f"   Payment: {payment}")
    
    def test_payment_generation_workflow_integration(self, test_config, reference_pool):
        """
        Test the full workflow: customers -> orders -> payments
        to ensure the entire pipeline works.
        """
        # This test should demonstrate the complete workflow works end-to-end
        entities_generated = {"customers": 0, "orders": 0, "payments": 0}
        
        # Generate customers
        customers_gen = CorrelatedDataGenerator("customers", test_config, reference_pool)
        for record in customers_gen.generate():
            assert record is not None
            entities_generated["customers"] += 1
            if entities_generated["customers"] >= 5:
                break
        
        # Generate orders  
        orders_gen = CorrelatedDataGenerator("orders", test_config, reference_pool)
        for record in orders_gen.generate():
            assert record is not None
            entities_generated["orders"] += 1
            if entities_generated["orders"] >= 3:
                break
        
        # Generate payments
        payments_gen = CorrelatedDataGenerator("payments", test_config, reference_pool)
        for record in payments_gen.generate():
            assert record is not None
            entities_generated["payments"] += 1
            if entities_generated["payments"] >= 2:
                break
        
        # Verify counts
        assert entities_generated["customers"] == 5
        assert entities_generated["orders"] == 3  
        assert entities_generated["payments"] == 2
        
        print(f"✅ Full workflow completed: {entities_generated}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])