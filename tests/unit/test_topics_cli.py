"""Tests for topics CLI commands and Kafka AdminClient integration."""
import pytest
from unittest.mock import Mock, patch, MagicMock
from click.testing import CliRunner
from confluent_kafka.admin import AdminClient, ConfigResource, RESOURCE_TOPIC
from confluent_kafka import KafkaException

from testdatapy.cli_topics import topics, create_admin_client
from testdatapy.config.correlation_config import CorrelationConfig


class TestTopicsCLI:
    """Test topics CLI command group."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
    
    def test_topics_command_group_exists(self):
        """Test that topics command group is available."""
        result = self.runner.invoke(topics, ['--help'])
        assert result.exit_code == 0
        assert 'topics' in result.output.lower()
        assert 'manage kafka topics' in result.output.lower()
    
    def test_topics_subcommands_available(self):
        """Test that expected subcommands are available."""
        result = self.runner.invoke(topics, ['--help'])
        assert result.exit_code == 0
        
        # Check for expected subcommands
        assert 'create' in result.output
        assert 'cleanup' in result.output
        assert 'list' in result.output


class TestAdminClientCreation:
    """Test AdminClient creation and configuration."""
    
    def test_create_admin_client_basic_config(self):
        """Test AdminClient creation with basic configuration."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092"
            }
        }
        correlation_config = CorrelationConfig(config_dict)
        
        with patch('testdatapy.cli_topics.AdminClient') as mock_admin_client:
            mock_client = Mock()
            mock_admin_client.return_value = mock_client
            
            client = create_admin_client(correlation_config)
            
            # Verify AdminClient was called with correct config
            mock_admin_client.assert_called_once()
            call_args = mock_admin_client.call_args[0][0]
            assert call_args['bootstrap.servers'] == 'localhost:9092'
            assert client == mock_client
    
    def test_create_admin_client_with_sasl_config(self):
        """Test AdminClient creation with SASL configuration."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092",
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "PLAIN",
                "sasl_username": "test_user",
                "sasl_password": "test_password"
            }
        }
        correlation_config = CorrelationConfig(config_dict)
        
        with patch('testdatapy.cli_topics.AdminClient') as mock_admin_client:
            mock_client = Mock()
            mock_admin_client.return_value = mock_client
            
            client = create_admin_client(correlation_config)
            
            # Verify AdminClient was called with SASL config
            call_args = mock_admin_client.call_args[0][0]
            assert call_args['bootstrap.servers'] == 'localhost:9092'
            assert call_args['security.protocol'] == 'SASL_SSL'
            assert call_args['sasl.mechanism'] == 'PLAIN'
            assert call_args['sasl.username'] == 'test_user'
            assert call_args['sasl.password'] == 'test_password'
    
    def test_create_admin_client_with_ssl_config(self):
        """Test AdminClient creation with SSL configuration."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092",
                "security_protocol": "SSL",
                "ssl_ca_location": "/path/to/ca.pem",
                "ssl_certificate_location": "/path/to/cert.pem",
                "ssl_key_location": "/path/to/key.pem"
            }
        }
        correlation_config = CorrelationConfig(config_dict)
        
        with patch('testdatapy.cli_topics.AdminClient') as mock_admin_client:
            mock_client = Mock()
            mock_admin_client.return_value = mock_client
            
            client = create_admin_client(correlation_config)
            
            # Verify AdminClient was called with SSL config
            call_args = mock_admin_client.call_args[0][0]
            assert call_args['bootstrap.servers'] == 'localhost:9092'
            assert call_args['security.protocol'] == 'SSL'
            assert call_args['ssl.ca.location'] == '/path/to/ca.pem'
            assert call_args['ssl.certificate.location'] == '/path/to/cert.pem'
            assert call_args['ssl.key.location'] == '/path/to/key.pem'
    
    def test_create_admin_client_kafka_exception_handling(self):
        """Test error handling when AdminClient creation fails."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "invalid:9092"
            }
        }
        correlation_config = CorrelationConfig(config_dict)
        
        with patch('testdatapy.cli_topics.AdminClient') as mock_admin_client:
            mock_admin_client.side_effect = KafkaException("Connection failed")
            
            with pytest.raises(KafkaException, match="Connection failed"):
                create_admin_client(correlation_config)
    
    def test_create_admin_client_missing_kafka_config(self):
        """Test error handling when Kafka configuration is missing."""
        config_dict = {}  # No kafka config
        correlation_config = CorrelationConfig(config_dict)
        
        with pytest.raises(ValueError, match="Kafka configuration not found"):
            create_admin_client(correlation_config)
    
    def test_create_admin_client_missing_bootstrap_servers(self):
        """Test error handling when bootstrap_servers is missing."""
        config_dict = {
            "kafka": {
                "security_protocol": "PLAINTEXT"  # Kafka config exists but missing bootstrap_servers
            }
        }
        correlation_config = CorrelationConfig(config_dict)
        
        with pytest.raises(ValueError, match="bootstrap_servers not found"):
            create_admin_client(correlation_config)
    
    def test_create_admin_client_additional_properties(self):
        """Test AdminClient creation with additional properties."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092",
                "request_timeout_ms": 30000,
                "acks": "all",
                "retries": 3
            }
        }
        correlation_config = CorrelationConfig(config_dict)
        
        with patch('testdatapy.cli_topics.AdminClient') as mock_admin_client:
            mock_client = Mock()
            mock_admin_client.return_value = mock_client
            
            client = create_admin_client(correlation_config)
            
            # Verify AdminClient was called with additional properties
            call_args = mock_admin_client.call_args[0][0]
            assert call_args['bootstrap.servers'] == 'localhost:9092'
            assert call_args['request.timeout.ms'] == 30000
            # Note: acks and retries are producer configs, not admin configs
            # They should be filtered out or ignored
    
    def test_admin_client_config_key_conversion(self):
        """Test that underscore keys are converted to dot notation."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092",
                "request_timeout_ms": 30000,
                "api_version_request": "true"
            }
        }
        correlation_config = CorrelationConfig(config_dict)
        
        with patch('testdatapy.cli_topics.AdminClient') as mock_admin_client:
            mock_client = Mock()
            mock_admin_client.return_value = mock_client
            
            create_admin_client(correlation_config)
            
            # Verify key conversion
            call_args = mock_admin_client.call_args[0][0]
            assert 'bootstrap.servers' in call_args
            assert 'request.timeout.ms' in call_args
            assert 'api.version.request' in call_args
            
            # Original underscore keys should not be present
            assert 'bootstrap_servers' not in call_args
            assert 'request_timeout_ms' not in call_args
            assert 'api_version_request' not in call_args


class TestTopicsCreateCommand:
    """Test topics create subcommand."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
    
    def test_create_command_exists(self):
        """Test that create subcommand exists."""
        result = self.runner.invoke(topics, ['create', '--help'])
        assert result.exit_code == 0
        assert 'create' in result.output.lower()


class TestTopicsCleanupCommand:
    """Test topics cleanup subcommand."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
    
    def test_cleanup_command_exists(self):
        """Test that cleanup subcommand exists."""
        result = self.runner.invoke(topics, ['cleanup', '--help'])
        assert result.exit_code == 0
        assert 'cleanup' in result.output.lower()


class TestTopicsListCommand:
    """Test topics list subcommand."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
    
    def test_list_command_exists(self):
        """Test that list subcommand exists."""
        result = self.runner.invoke(topics, ['list', '--help'])
        assert result.exit_code == 0
        assert 'list' in result.output.lower()


class TestCLIIntegration:
    """Test CLI integration scenarios."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
    
    def test_topics_group_integration_with_main_cli(self):
        """Test that topics group integrates with main CLI."""
        # This test would need to be in the main CLI test file
        # or we'd need to import and test the main CLI here
        pass
    
    def test_error_handling_with_invalid_config(self):
        """Test error handling with invalid configuration file."""
        with self.runner.isolated_filesystem():
            # Create invalid config file
            with open('invalid_config.json', 'w') as f:
                f.write('{"invalid": "json"')  # Invalid JSON
            
            result = self.runner.invoke(topics, [
                '--config', 'invalid_config.json', 
                'list'
            ])
            
            # Should handle JSON parsing error gracefully
            assert result.exit_code != 0
            assert 'error' in result.output.lower()
    
    def test_verbose_output_option(self):
        """Test verbose output option."""
        result = self.runner.invoke(topics, ['--verbose', '--help'])
        assert result.exit_code == 0
        # Verify verbose option is available


class TestConfigurationParsing:
    """Test configuration parsing for topic operations."""
    
    def test_parse_kafka_config_from_correlation_config(self):
        """Test parsing Kafka config from CorrelationConfig."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "broker1:9092,broker2:9092",
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "PLAIN",
                "sasl_username": "user",
                "sasl_password": "pass"
            },
            "master_data": {
                "appointments": {
                    "kafka_topic": "appointments",
                    "partitions": 3,
                    "replication_factor": 2
                }
            },
            "transactional_data": {
                "carinlane": {
                    "kafka_topic": "car_events",
                    "partitions": 6,
                    "replication_factor": 3
                }
            }
        }
        
        correlation_config = CorrelationConfig(config_dict)
        
        # Test that we can extract Kafka config
        kafka_config = correlation_config.config.get("kafka", {})
        assert kafka_config["bootstrap_servers"] == "broker1:9092,broker2:9092"
        assert kafka_config["security_protocol"] == "SASL_SSL"
        
        # Test that we can extract topic configurations
        master_data = correlation_config.config.get("master_data", {})
        assert "appointments" in master_data
        assert master_data["appointments"]["kafka_topic"] == "appointments"
        assert master_data["appointments"]["partitions"] == 3