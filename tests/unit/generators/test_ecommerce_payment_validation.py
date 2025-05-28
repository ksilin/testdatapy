#!/usr/bin/env python3
"""
Validation test to ensure the ecommerce configuration generates proper payment data.
"""

import pytest
import yaml
from pathlib import Path
from testdatapy.generators.correlated_generator import CorrelatedDataGenerator
from testdatapy.generators.reference_pool import ReferencePool
from testdatapy.config.correlation_config import CorrelationConfig


class TestEcommercePaymentValidation:
    """Test using the actual ecommerce configuration."""
    
    @pytest.fixture
    def ecommerce_config(self):
        """Load the actual ecommerce configuration."""
        config_path = Path(__file__).parent.parent.parent.parent / "docker-testdata-correlated" / "configs" / "ecommerce-correlated.yaml"
        
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        return CorrelationConfig(config_data)
    
    @pytest.fixture
    def reference_pool(self):
        """Create a reference pool for testing."""
        return ReferencePool()
    
    def test_ecommerce_payment_generation_end_to_end(self, ecommerce_config, reference_pool):
        """
        Test the complete e-commerce workflow with actual configuration:
        customers -> orders -> payments
        """
        # Step 1: Generate customers (Note: customers may use UUID instead of formatted IDs in master_data)
        customers_gen = CorrelatedDataGenerator("customers", ecommerce_config, reference_pool)
        customer_count = 0
        for record in customers_gen.generate():
            assert record is not None
            assert "customer_id" in record
            # Customer ID may be UUID or formatted string depending on master_data implementation
            assert isinstance(record["customer_id"], str)
            assert len(record["customer_id"]) > 0
            customer_count += 1
            if customer_count >= 10:  # Generate 10 customers
                break
        
        assert customer_count == 10, f"Expected 10 customers, got {customer_count}"
        
        # Step 2: Generate orders 
        orders_gen = CorrelatedDataGenerator("orders", ecommerce_config, reference_pool)
        generated_orders = []
        for record in orders_gen.generate():
            assert record is not None
            assert "order_id" in record
            assert "customer_id" in record
            assert record["order_id"].startswith("ORDER_")
            # Customer ID format depends on master_data vs transactional_data handling
            assert isinstance(record["customer_id"], str) and len(record["customer_id"]) > 0
            assert "total_amount" in record
            assert isinstance(record["total_amount"], (int, float))
            generated_orders.append(record)
            if len(generated_orders) >= 5:  # Generate 5 orders
                break
        
        assert len(generated_orders) == 5, f"Expected 5 orders, got {len(generated_orders)}"
        
        # Step 3: Generate payments - THE KEY TEST
        payments_gen = CorrelatedDataGenerator("payments", ecommerce_config, reference_pool)
        generated_payments = []
        for record in payments_gen.generate():
            assert record is not None, "Payment should be generated"
            
            # Verify payment structure
            assert "payment_id" in record, "Payment should have payment_id"
            assert "order_id" in record, "Payment should have order_id"
            assert "amount" in record, "Payment should have amount"
            assert "payment_method" in record, "Payment should have payment_method"
            
            # Verify payment field values
            assert record["payment_id"].startswith("PAY_"), f"Invalid payment_id format: {record['payment_id']}"
            assert record["order_id"].startswith("ORDER_"), f"Invalid order_id format: {record['order_id']}"
            assert record["amount"] is not None, f"Payment amount should not be None: {record}"
            assert record["payment_method"] is not None, f"Payment method should not be None: {record}"
            
            # Verify payment method is valid
            valid_methods = ["credit_card", "debit_card", "paypal", "bank_transfer"]
            assert record["payment_method"] in valid_methods, f"Invalid payment method: {record['payment_method']}"
            
            # Verify amount is numeric
            assert isinstance(record["amount"], (int, float)), f"Amount should be numeric: {record['amount']}"
            assert record["amount"] > 0, f"Amount should be positive: {record['amount']}"
            
            # Verify order_id references exist
            order_ids = [o["order_id"] for o in generated_orders]
            assert record["order_id"] in order_ids, f"Payment order_id {record['order_id']} should reference existing order"
            
            generated_payments.append(record)
            if len(generated_payments) >= 3:  # Generate 3 payments
                break
        
        assert len(generated_payments) == 3, f"Expected 3 payments, got {len(generated_payments)}"
        
        print(f"✅ E-commerce workflow validation successful!")
        print(f"   Generated: {customer_count} customers, {len(generated_orders)} orders, {len(generated_payments)} payments")
        
        # Show sample data
        print(f"\n📄 Sample payment data:")
        for i, payment in enumerate(generated_payments):
            print(f"   Payment {i+1}: {payment}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])