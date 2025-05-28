"""
Unit test that demonstrates the reference tracking bug.
This test should FAIL before the fix and PASS after the fix.
"""

import pytest
import yaml
from pathlib import Path

from testdatapy.generators.reference_pool import ReferencePool
from testdatapy.generators.correlated_generator import CorrelatedDataGenerator
from testdatapy.config.correlation_config import CorrelationConfig


class TestReferenceTracking:
    """Test reference tracking between correlated generators."""
    
    def test_orders_should_be_available_for_payments_reference(self):
        """
        FAILING TEST: Demonstrate that orders are not tracked for payments to reference.
        
        This test creates orders with track_recent=True, then tries to generate payments
        that reference those orders. This should work but currently fails.
        """
        # Load test configuration
        config_path = Path(__file__).parent.parent / "config" / "test_reference_tracking_bug.yaml"
        config = CorrelationConfig.from_yaml_file(str(config_path))
        
        # Create reference pool
        ref_pool = ReferencePool()
        ref_pool.enable_stats()
        
        # Step 1: Load master data (customers)
        ref_pool.add_references("customers", ["CUST_0001", "CUST_0002", "CUST_0003"])
        
        # Step 2: Generate orders (this should add to reference pool)
        orders_generator = CorrelatedDataGenerator(
            entity_type="orders",
            config=config,
            reference_pool=ref_pool
        )
        
        generated_orders = []
        order_ids = []
        
        for order in orders_generator.generate():
            generated_orders.append(order)
            order_ids.append(order.get("order_id"))
            
            # Break after getting test orders
            if len(generated_orders) >= 3:
                break
        
        # Verify orders were generated
        assert len(generated_orders) == 3
        assert all(order.get("customer_id") for order in generated_orders)
        assert all(order.get("order_id") for order in generated_orders)
        
        # DEBUG: Check what's in the reference pool
        print(f"\nDEBUG - Generated orders: {order_ids}")
        print(f"DEBUG - Reference pool contents: {ref_pool._references}")
        print(f"DEBUG - Recent items: {ref_pool._recent_items}")
        
        # Step 3: Try to generate payments (this should work but will fail)
        payments_generator = CorrelatedDataGenerator(
            entity_type="payments", 
            config=config,
            reference_pool=ref_pool
        )
        
        # This should work: payments should be able to reference generated orders
        # But it will fail with "No references found for type: orders"
        generated_payments = []
        
        try:
            for payment in payments_generator.generate():
                generated_payments.append(payment)
                
                # Break after getting test payments
                if len(generated_payments) >= 2:
                    break
                    
            # If we get here, the bug is fixed!
            assert len(generated_payments) == 2
            assert all(payment.get("order_id") for payment in generated_payments)
            assert all(payment.get("payment_id") for payment in generated_payments)
            
            # Verify payments reference actual order IDs
            payment_order_ids = [p.get("order_id") for p in generated_payments]
            assert all(oid in order_ids for oid in payment_order_ids)
            
        except ValueError as e:
            if "No references found for type: orders" in str(e):
                # This is the expected failure - the bug!
                pytest.fail(
                    f"BUG CONFIRMED: Orders were generated but not available for payments reference. "
                    f"Error: {e}. "
                    f"Generated order IDs: {order_ids}, "
                    f"Reference pool: {ref_pool._references}, "
                    f"Recent items: {ref_pool._recent_items}"
                )
            else:
                # Some other error
                raise
    
    def test_reference_pool_tracking_orders_individually(self):
        """
        Test that individual order generation should add to main reference pool.
        
        This test verifies the specific mechanism that should work.
        """
        ref_pool = ReferencePool()
        
        # Add some test customers
        ref_pool.add_references("customers", ["CUST_0001", "CUST_0002"])
        
        # Verify customers are available
        assert ref_pool.get_type_count("customers") == 2
        
        # The bug: when we generate orders with CorrelatedDataGenerator,
        # they should be available for subsequent reference
        # But currently they're only added to recent tracking, not main pool
        
        # Simulate what CorrelatedDataGenerator does (the buggy behavior)
        ref_pool.enable_recent_tracking("orders", window_size=100)
        
        # This is what currently happens (only recent tracking)
        ref_pool.add_recent("orders", "ORDER_00001")
        ref_pool.add_recent("orders", "ORDER_00002")
        
        # Check: orders are in recent tracking
        recent_orders = ref_pool.get_recent("orders")
        assert len(recent_orders) == 2
        assert "ORDER_00001" in recent_orders
        
        # But orders are NOT in main reference pool
        orders_count = ref_pool.get_type_count("orders")
        assert orders_count == 0  # This is the bug!
        
        # When payments try to get random order reference, it fails
        with pytest.raises(ValueError, match="No references found for type: orders"):
            ref_pool.get_random("orders")
        
        # The fix: orders should also be added to main pool
        # ref_pool.add_references("orders", ["ORDER_00001", "ORDER_00002"])
        # Then this would work:
        # order_ref = ref_pool.get_random("orders")
        # assert order_ref in ["ORDER_00001", "ORDER_00002"]
    
    def test_field_population_completeness(self):
        """
        Test that all fields are properly populated in generated data.
        This ensures our fix doesn't break field generation.
        """
        config_path = Path(__file__).parent.parent / "config" / "test_reference_tracking_bug.yaml"
        config = CorrelationConfig.from_yaml_file(str(config_path))
        
        ref_pool = ReferencePool()
        ref_pool.add_references("customers", ["CUST_0001", "CUST_0002"])
        
        orders_generator = CorrelatedDataGenerator(
            entity_type="orders",
            config=config, 
            reference_pool=ref_pool
        )
        
        # Generate one order and verify all fields are populated
        order = next(orders_generator.generate())
        
        # Check required fields are present and not None/empty
        assert order.get("order_id"), f"order_id missing or empty: {order}"
        assert order.get("customer_id"), f"customer_id missing or empty: {order}"
        assert order.get("order_date"), f"order_date missing or empty: {order}"
        assert order.get("total_amount") is not None, f"total_amount missing: {order}"
        
        # Check field types and formats
        assert order["order_id"].startswith("ORDER_"), f"order_id format wrong: {order['order_id']}"
        assert order["customer_id"] in ["CUST_0001", "CUST_0002"], f"customer_id not from pool: {order['customer_id']}"
        assert isinstance(order["total_amount"], float), f"total_amount not float: {type(order['total_amount'])}"
        assert 10.0 <= order["total_amount"] <= 100.0, f"total_amount out of range: {order['total_amount']}"
        
        print(f"✅ Generated order with all fields: {order}")
