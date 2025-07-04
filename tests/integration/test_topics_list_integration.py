"""Integration tests for topics list command."""
import json
import tempfile
from pathlib import Path
from click.testing import CliRunner

from testdatapy.cli_topics import topics


class TestTopicsListIntegration:
    """Integration tests for topics list command."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
    
    def test_list_command_shows_topic_status(self):
        """Test topics list shows status of configured topics."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092"
            },
            "master_data": {
                "appointments": {
                    "kafka_topic": "test-appointments",
                    "partitions": 3,
                    "replication_factor": 1
                }
            },
            "transactional_data": {
                "events": {
                    "kafka_topic": "test-events", 
                    "partitions": 6,
                    "replication_factor": 1
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(config_dict, config_file, indent=2)
            config_path = config_file.name
        
        try:
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'list'
            ])
            
            # Should succeed
            assert result.exit_code == 0
            
            # Check output content
            output = result.output
            assert "Topic Status" in output
            assert "test-appointments" in output
            assert "test-events" in output
            assert "Type: master_data" in output
            assert "Type: transactional_data" in output
            assert "Entity: appointments" in output
            assert "Entity: events" in output
            
            # Topics likely don't exist in test environment, should show status
            assert "❌" in output or "✅" in output  # Should show some status icons
            
        finally:
            Path(config_path).unlink()
    
    def test_list_command_with_show_config_option(self):
        """Test topics list with --show-config option."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092"
            },
            "master_data": {
                "test_data": {
                    "kafka_topic": "config-test-topic",
                    "partitions": 2,
                    "replication_factor": 1
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(config_dict, config_file, indent=2)
            config_path = config_file.name
        
        try:
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'list',
                '--show-config'
            ])
            
            # Should succeed
            assert result.exit_code == 0
            
            # Check output content
            output = result.output
            assert "config-test-topic" in output
            assert "Type: master_data" in output
            
            # Should include partition and replication info
            assert "Partitions:" in output or "Configured Partitions:" in output
            
        finally:
            Path(config_path).unlink()
    
    def test_list_command_with_verbose_output(self):
        """Test topics list with verbose output."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092"
            },
            "master_data": {
                "verbose_test": {
                    "kafka_topic": "verbose-test-topic"
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
                'list'
            ])
            
            # Should succeed
            assert result.exit_code == 0
            
            # Check verbose output
            output = result.output
            assert "Verbose mode enabled" in output
            assert "Loading configuration from:" in output
            assert "Creating Kafka AdminClient..." in output
            assert "Fetching cluster metadata..." in output
            
        finally:
            Path(config_path).unlink()
    
    def test_list_command_no_topics_configured(self):
        """Test topics list when no topics are configured."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092"
            },
            "master_data": {
                "reference_only": {
                    "source": "csv",
                    "file": "/path/to/ref.csv",
                    "bulk_load": False  # No kafka_topic
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(config_dict, config_file, indent=2)
            config_path = config_file.name
        
        try:
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'list'
            ])
            
            # Should succeed
            assert result.exit_code == 0
            
            # Should indicate no topics found
            output = result.output
            assert "No topics found in configuration" in output
            
        finally:
            Path(config_path).unlink()
    
    def test_list_command_mixed_existing_and_missing_topics(self):
        """Test topics list with mix of existing and non-existing topics."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092"
            },
            "master_data": {
                "existing_data": {
                    "kafka_topic": "probably-missing-topic-1",
                    "partitions": 1
                },
                "missing_data": {
                    "kafka_topic": "probably-missing-topic-2", 
                    "partitions": 2
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(config_dict, config_file, indent=2)
            config_path = config_file.name
        
        try:
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'list'
            ])
            
            # Should succeed
            assert result.exit_code == 0
            
            # Should show both topics with their status
            output = result.output
            assert "probably-missing-topic-1" in output
            assert "probably-missing-topic-2" in output
            assert "Total: 2" in output
            
            # Should show configured settings for missing topics
            assert "Configured Partitions:" in output
            
        finally:
            Path(config_path).unlink()
    
    def test_list_command_displays_partition_and_replication_info(self):
        """Test that list command shows partition and replication information."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092"
            },
            "master_data": {
                "topic_with_config": {
                    "kafka_topic": "configured-topic",
                    "partitions": 5,
                    "replication_factor": 2
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(config_dict, config_file, indent=2)
            config_path = config_file.name
        
        try:
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'list'
            ])
            
            # Should succeed
            assert result.exit_code == 0
            
            # Should show partition and replication info
            output = result.output
            assert "configured-topic" in output
            
            # For non-existing topics, should show configured settings
            if "Topic does not exist" in output:
                assert "Configured Partitions: 5" in output
                assert "Configured Replication Factor: 2" in output
            
        finally:
            Path(config_path).unlink()


class TestTopicsListValidation:
    """Test validation and error handling for topics list."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
    
    def test_list_command_no_config_file_error(self):
        """Test list without config file shows error."""
        result = self.runner.invoke(topics, ['list'])
        
        # Should fail
        assert result.exit_code == 1
        assert "Configuration file is required for topic listing" in result.output
    
    def test_list_command_invalid_config_file_error(self):
        """Test list with invalid config file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            config_file.write('{"invalid": json}')  # Invalid JSON
            config_path = config_file.name
        
        try:
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'list'
            ])
            
            # Should fail
            assert result.exit_code == 1
            assert "Invalid JSON in configuration file" in result.output
            
        finally:
            Path(config_path).unlink()
    
    def test_list_command_missing_kafka_config_error(self):
        """Test list with missing kafka configuration."""
        config_dict = {
            "master_data": {
                "test_data": {
                    "kafka_topic": "test-topic"
                }
            }
            # Missing kafka config
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(config_dict, config_file, indent=2)
            config_path = config_file.name
        
        try:
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'list'
            ])
            
            # Should fail
            assert result.exit_code == 1
            assert "Kafka configuration not found" in result.output
            
        finally:
            Path(config_path).unlink()
    
    def test_list_command_connection_timeout_handling(self):
        """Test list command handles connection timeouts gracefully."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "nonexistent:9092"  # Will timeout
            },
            "master_data": {
                "test_data": {
                    "kafka_topic": "timeout-test-topic"
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(config_dict, config_file, indent=2)
            config_path = config_file.name
        
        try:
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'list'
            ])
            
            # Should fail with appropriate error
            assert result.exit_code == 1
            assert "Kafka operation failed" in result.output
            
        finally:
            Path(config_path).unlink()