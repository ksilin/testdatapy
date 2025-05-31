"""Tests for MasterDataGenerator - handles bulk loading of master data."""
import pytest
from unittest.mock import Mock, patch
import tempfile
import csv
from pathlib import Path

from testdatapy.generators.master_data_generator import MasterDataGenerator
from testdatapy.generators.reference_pool import ReferencePool
from testdatapy.config.correlation_config import CorrelationConfig
from testdatapy.producers.base import KafkaProducer


class TestMasterDataGenerator:
    """Test the MasterDataGenerator class."""
    
    def test_create_generator(self):
        """Test creating a master data generator."""
        config_dict = {
            "master_data": {
                "customers": {
                    "source": "csv",
                    "file": "customers.csv",
                    "kafka_topic": "customers",
                    "id_field": "customer_id"
                }
            }
        }
        
        config = CorrelationConfig(config_dict)
        ref_pool = ReferencePool()
        
        generator = MasterDataGenerator(
            config=config,
            reference_pool=ref_pool
        )
        
        assert generator.config == config
        assert generator.reference_pool == ref_pool
    
    def test_load_from_csv(self, tmp_path):
        """Test loading master data from CSV files."""
        # Create test CSV
        csv_file = tmp_path / "customers.csv"
        with open(csv_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['customer_id', 'name'])
            writer.writeheader()
            writer.writerows([
                {'customer_id': 'CUST_001', 'name': 'John'},
                {'customer_id': 'CUST_002', 'name': 'Jane'},
            ])
        
        config_dict = {
            "master_data": {
                "customers": {
                    "source": "csv",
                    "file": str(csv_file),
                    "kafka_topic": "customers",
                    "id_field": "customer_id"
                }
            }
        }
        
        config = CorrelationConfig(config_dict)
        ref_pool = ReferencePool()
        
        generator = MasterDataGenerator(config=config, reference_pool=ref_pool)
        
        # Load the data
        generator.load_all()
        
        # Check that references were added to pool
        assert ref_pool.has_type("customers")
        assert ref_pool.get_type_count("customers") == 2
        
        # Check that data was loaded
        assert len(generator.loaded_data["customers"]) == 2
    
    def test_generate_with_faker(self):
        """Test generating master data with Faker."""
        config_dict = {
            "master_data": {
                "customers": {
                    "source": "faker",
                    "count": 5,
                    "kafka_topic": "customers",
                    "id_field": "customer_id",
                    "schema": {
                        "customer_id": {
                            "type": "string",
                            "format": "CUST_{seq:04d}"
                        },
                        "name": {
                            "type": "faker",
                            "method": "name"
                        },
                        "email": {
                            "type": "faker",
                            "method": "email"
                        }
                    }
                }
            }
        }
        
        config = CorrelationConfig(config_dict)
        ref_pool = ReferencePool()
        
        generator = MasterDataGenerator(config=config, reference_pool=ref_pool)
        
        # Generate the data
        generator.load_all()
        
        # Check results
        assert ref_pool.get_type_count("customers") == 5
        assert len(generator.loaded_data["customers"]) == 5
        
        # Check data format
        customer = generator.loaded_data["customers"][0]
        assert customer["customer_id"].startswith("CUST_")
        assert "@" in customer["email"]
    
    def test_bulk_produce(self):
        """Test bulk producing master data to Kafka."""
        config_dict = {
            "master_data": {
                "customers": {
                    "source": "faker",
                    "count": 3,
                    "kafka_topic": "customers",
                    "id_field": "customer_id",
                    "bulk_load": True
                }
            }
        }
        
        config = CorrelationConfig(config_dict)
        ref_pool = ReferencePool()
        
        # Mock producer
        mock_producer = Mock(spec=KafkaProducer)
        mock_producer.bootstrap_servers = "localhost:9092"
        
        generator = MasterDataGenerator(
            config=config,
            reference_pool=ref_pool,
            producer=mock_producer
        )
        
        # Load and produce
        generator.load_all()
        generator.produce_all()
        
        # Check that produce was called
        assert mock_producer.produce.call_count == 3
        mock_producer.flush.assert_called_once()
    
    def test_produce_only_bulk_enabled(self):
        """Test that only bulk-enabled entities are produced."""
        config_dict = {
            "master_data": {
                "customers": {
                    "source": "faker",
                    "count": 2,
                    "kafka_topic": "customers",
                    "bulk_load": True
                },
                "products": {
                    "source": "faker", 
                    "count": 2,
                    "kafka_topic": "products",
                    "bulk_load": False  # Not bulk loaded
                }
            }
        }
        
        config = CorrelationConfig(config_dict)
        ref_pool = ReferencePool()
        mock_producer = Mock(spec=KafkaProducer)
        mock_producer.bootstrap_servers = "localhost:9092"
        
        generator = MasterDataGenerator(
            config=config,
            reference_pool=ref_pool,
            producer=mock_producer
        )
        
        generator.load_all()
        generator.produce_all()
        
        # Only customers should be produced
        assert mock_producer.produce.call_count == 2
        
        # But both should be in reference pool
        assert ref_pool.get_type_count("customers") == 2
        assert ref_pool.get_type_count("products") == 2
    
    def test_sequential_id_generation(self):
        """Test sequential ID generation."""
        config_dict = {
            "master_data": {
                "products": {
                    "source": "faker",
                    "count": 3,
                    "kafka_topic": "products",
                    "id_field": "product_id",
                    "schema": {
                        "product_id": {
                            "type": "string",
                            "format": "PROD_{seq:05d}"
                        }
                    }
                }
            }
        }
        
        config = CorrelationConfig(config_dict)
        ref_pool = ReferencePool()
        
        generator = MasterDataGenerator(config=config, reference_pool=ref_pool)
        generator.load_all()
        
        # Check sequential IDs
        products = generator.loaded_data["products"]
        assert products[0]["product_id"] == "PROD_00001"
        assert products[1]["product_id"] == "PROD_00002"
        assert products[2]["product_id"] == "PROD_00003"
    
    def test_error_handling_missing_file(self):
        """Test error handling for missing CSV file."""
        config_dict = {
            "master_data": {
                "customers": {
                    "source": "csv",
                    "file": "nonexistent.csv",
                    "kafka_topic": "customers"
                }
            }
        }
        
        config = CorrelationConfig(config_dict)
        ref_pool = ReferencePool()
        
        generator = MasterDataGenerator(config=config, reference_pool=ref_pool)
        
        with pytest.raises(FileNotFoundError):
            generator.load_all()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
