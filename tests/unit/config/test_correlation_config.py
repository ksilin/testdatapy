"""Tests for correlation configuration loading and validation."""
import pytest
from typing import Dict, Any
import yaml
import tempfile
import os

from testdatapy.config.correlation_config import CorrelationConfig, ValidationError


class TestCorrelationConfig:
    """Test the CorrelationConfig class for loading correlation configurations."""
    
    def test_load_minimal_config(self):
        """Test loading a minimal valid configuration."""
        config_dict = {
            "master_data": {
                "customers": {
                    "source": "csv",
                    "file": "customers.csv",
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
        
        assert config.has_master_type("customers")
        assert config.has_transaction_type("orders")
        assert config.get_master_config("customers")["source"] == "csv"
        assert config.get_transaction_config("orders")["rate_per_second"] == 10
    
    def test_validate_references(self):
        """Test that references are validated properly."""
        config_dict = {
            "master_data": {
                "customers": {
                    "source": "csv",
                    "file": "customers.csv",
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
                            "references": "invalid_type.invalid_field"  # Invalid reference
                        }
                    }
                }
            }
        }
        
        # Should raise validation error
        with pytest.raises(ValidationError):
            CorrelationConfig(config_dict)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
