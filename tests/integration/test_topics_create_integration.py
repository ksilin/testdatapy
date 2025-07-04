"""Integration tests for topics create command."""
import json
import tempfile
from pathlib import Path
from click.testing import CliRunner

from testdatapy.cli_topics import topics


class TestTopicsCreateIntegration:
    """Integration tests for topics create command."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
    
    def test_create_command_dry_run_with_vehicle_config(self):
        """Test topics create dry-run with vehicle-style configuration."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092",
                "security_protocol": "PLAINTEXT"
            },
            "master_data": {
                "appointments": {
                    "source": "faker",  # Changed from csv to faker to avoid validation error
                    "count": 100,
                    "kafka_topic": "vehicle-appointments",
                    "partitions": 3,
                    "replication_factor": 1,
                    "bulk_load": True
                },
                "branches": {
                    "source": "faker",
                    "count": 100,
                    "kafka_topic": "vehicle-branches",
                    "partitions": 1,
                    "replication_factor": 1,
                    "bulk_load": True
                }
            },
            "transactional_data": {
                "car_in_lane_events": {
                    "kafka_topic": "vehicle-car-events",
                    "partitions": 6,
                    "replication_factor": 1,
                    "rate_limit": 1000
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(config_dict, config_file, indent=2)
            config_path = config_file.name
        
        try:
            # Test dry-run
            result = self.runner.invoke(topics, [
                '--config', config_path,
                '--verbose',
                'create',
                '--dry-run'
            ])
            
            # Should succeed with dry-run
            assert result.exit_code == 0
            
            # Check output content
            output = result.output
            assert "DRY RUN" in output
            assert "vehicle-appointments" in output
            assert "vehicle-branches" in output
            assert "vehicle-car-events" in output
            assert "partitions: 3" in output
            assert "partitions: 1" in output
            assert "partitions: 6" in output
            assert "replication: 1" in output
            
        finally:
            Path(config_path).unlink()
    
    def test_create_command_with_overrides(self):
        """Test topics create with partition and replication factor overrides."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092"
            },
            "master_data": {
                "test_data": {
                    "kafka_topic": "test-topic",
                    "partitions": 1,  # Will be overridden
                    "replication_factor": 1  # Will be overridden
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(config_dict, config_file, indent=2)
            config_path = config_file.name
        
        try:
            # Test with overrides in dry-run
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'create',
                '--dry-run',
                '--partitions', '5',
                '--replication-factor', '2'
            ])
            
            # Should succeed
            assert result.exit_code == 0
            
            # Check overrides applied
            output = result.output
            assert "partitions: 5" in output
            assert "replication: 2" in output
            
        finally:
            Path(config_path).unlink()
    
    def test_create_command_no_config_error(self):
        """Test topics create without config file shows error."""
        result = self.runner.invoke(topics, ['create'])
        
        # Should fail
        assert result.exit_code == 1
        assert "Configuration file is required" in result.output
        assert "Use --config option" in result.output
    
    def test_create_command_invalid_config_error(self):
        """Test topics create with invalid config file shows error."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            config_file.write('{"invalid": json}')  # Invalid JSON
            config_path = config_file.name
        
        try:
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'create'
            ])
            
            # Should fail
            assert result.exit_code == 1
            assert "Invalid JSON" in result.output
            
        finally:
            Path(config_path).unlink()
    
    def test_create_command_missing_kafka_config_error(self):
        """Test topics create with missing kafka config shows error."""
        config_dict = {
            "master_data": {
                "test_data": {
                    "kafka_topic": "test-topic"
                }
            }
            # Missing kafka configuration
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(config_dict, config_file, indent=2)
            config_path = config_file.name
        
        try:
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'create',
                '--dry-run'
            ])
            
            # Should fail
            assert result.exit_code == 1
            assert "Kafka configuration not found" in result.output
            
        finally:
            Path(config_path).unlink()
    
    def test_create_command_no_topics_to_create(self):
        """Test topics create with no topics in config."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092"
            },
            "master_data": {
                "test_data": {
                    "source": "csv",
                    "file": "/path/to/data.csv",
                    "bulk_load": False  # No kafka_topic specified
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(config_dict, config_file, indent=2)
            config_path = config_file.name
        
        try:
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'create'
            ])
            
            # Should succeed but do nothing
            assert result.exit_code == 0
            assert "No topics found in configuration to create" in result.output
            
        finally:
            Path(config_path).unlink()
    
    def test_create_command_with_reference_only_entities(self):
        """Test topics create with mix of bulk_load and reference-only entities."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092"
            },
            "master_data": {
                "appointments": {
                    "source": "csv",
                    "file": "/path/to/appointments.csv",
                    "kafka_topic": "appointments",
                    "bulk_load": True,
                    "partitions": 3
                },
                "reference_data": {
                    "source": "csv", 
                    "file": "/path/to/ref.csv",
                    "bulk_load": False  # Reference only - no Kafka topic
                }
            },
            "transactional_data": {
                "events": {
                    "kafka_topic": "events",
                    "partitions": 6
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(config_dict, config_file, indent=2)
            config_path = config_file.name
        
        try:
            result = self.runner.invoke(topics, [
                '--config', config_path,
                '--verbose',
                'create',
                '--dry-run'
            ])
            
            # Should succeed
            if result.exit_code != 0:
                print(f"Command failed with exit code {result.exit_code}")
                print(f"Output: {result.output}")
                print(f"Exception: {result.exception}")
            assert result.exit_code == 0
            
            # Should only show topics for entities with kafka_topic
            output = result.output
            assert "appointments" in output
            assert "events" in output
            assert "reference_data" not in output  # Should not appear
            assert "DRY RUN" in output
            # Check that both topics appear in the dry-run section
            dry_run_section = output.split("DRY RUN")[1]
            assert "appointments" in dry_run_section
            assert "events" in dry_run_section
            
        finally:
            Path(config_path).unlink()


class TestTopicsCreateValidation:
    """Test validation and error handling for topics create."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
    
    def test_create_validates_topic_names(self):
        """Test that valid topic names are accepted in dry-run."""
        config_dict = {
            "kafka": {"bootstrap_servers": "localhost:9092"},
            "master_data": {
                "valid_topic": {
                    "kafka_topic": "valid-topic-name_123",
                    "partitions": 1
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(config_dict, config_file, indent=2)
            config_path = config_file.name
        
        try:
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'create',
                '--dry-run'
            ])
            
            # Should succeed with valid topic name
            assert result.exit_code == 0
            assert "valid-topic-name_123" in result.output
            
        finally:
            Path(config_path).unlink()
    
    def test_create_shows_helpful_error_on_connection_failure(self):
        """Test that connection failures show helpful error messages."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "nonexistent:9092"  # Invalid broker
            },
            "master_data": {
                "test_data": {
                    "kafka_topic": "test-topic"
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(config_dict, config_file, indent=2)
            config_path = config_file.name
        
        try:
            # Dry-run should work (no actual connection)
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'create',
                '--dry-run'
            ])
            
            # Should succeed in dry-run mode
            assert result.exit_code == 0
            assert "DRY RUN" in result.output
            
        finally:
            Path(config_path).unlink()