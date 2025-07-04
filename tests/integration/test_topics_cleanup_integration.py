"""Integration tests for topics cleanup command."""
import json
import tempfile
from pathlib import Path
from click.testing import CliRunner

from testdatapy.cli_topics import topics


class TestTopicsCleanupIntegration:
    """Integration tests for topics cleanup command."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
    
    def test_cleanup_command_delete_strategy_dry_run(self):
        """Test topics cleanup with delete strategy in dry-run mode."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092"
            },
            "master_data": {
                "appointments": {
                    "kafka_topic": "test-appointments",
                    "partitions": 3
                }
            },
            "transactional_data": {
                "events": {
                    "kafka_topic": "test-events",
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
                'cleanup',
                '--dry-run',
                '--strategy', 'delete'
            ])
            
            # Should succeed
            assert result.exit_code == 0
            
            # Check output content
            output = result.output
            assert "DRY RUN - Would delete the following topics:" in output
            assert "test-appointments" in output
            assert "test-events" in output
            
        finally:
            Path(config_path).unlink()
    
    def test_cleanup_command_truncate_strategy_dry_run(self):
        """Test topics cleanup with truncate strategy in dry-run mode."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092"
            },
            "master_data": {
                "test_data": {
                    "kafka_topic": "test-truncate-topic",
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
                'cleanup',
                '--dry-run',
                '--strategy', 'truncate'
            ])
            
            # Should succeed
            assert result.exit_code == 0
            
            # Check output content
            output = result.output
            assert "DRY RUN - Would truncate the following topics:" in output
            assert "test-truncate-topic" in output
            
        finally:
            Path(config_path).unlink()
    
    def test_cleanup_command_default_strategy_is_delete(self):
        """Test that default strategy is delete."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092"
            },
            "master_data": {
                "test_data": {
                    "kafka_topic": "test-default-topic"
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(config_dict, config_file, indent=2)
            config_path = config_file.name
        
        try:
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'cleanup',
                '--dry-run'  # No strategy specified, should default to delete
            ])
            
            # Should succeed
            assert result.exit_code == 0
            
            # Should use delete strategy by default
            output = result.output
            assert "DRY RUN - Would delete the following topics:" in output
            assert "test-default-topic" in output
            
        finally:
            Path(config_path).unlink()
    
    def test_cleanup_command_with_verbose_output(self):
        """Test cleanup command with verbose output."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092"
            },
            "master_data": {
                "appointments": {
                    "kafka_topic": "verbose-test-topic",
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
                '--verbose',
                'cleanup',
                '--dry-run',
                '--strategy', 'delete'
            ])
            
            # Should succeed
            assert result.exit_code == 0
            
            # Check verbose output
            output = result.output
            assert "Verbose mode enabled" in output
            assert "Loading configuration from:" in output
            assert "Creating Kafka AdminClient..." in output
            assert "Master data topic to delete:" in output
            
        finally:
            Path(config_path).unlink()
    
    def test_cleanup_command_no_topics_to_cleanup(self):
        """Test cleanup when no topics are configured."""
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
                'cleanup',
                '--dry-run'
            ])
            
            # Should succeed
            assert result.exit_code == 0
            
            # Should indicate no topics to cleanup
            output = result.output
            assert "No topics found in configuration to cleanup" in output
            
        finally:
            Path(config_path).unlink()
    
    def test_cleanup_command_confirmation_prompt(self):
        """Test cleanup command shows confirmation prompt."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092"
            },
            "master_data": {
                "test_data": {
                    "kafka_topic": "confirmation-test-topic"
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(config_dict, config_file, indent=2)
            config_path = config_file.name
        
        try:
            # Test with 'n' input to cancel
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'cleanup',
                '--strategy', 'delete'
            ], input='n\n')
            
            # Should succeed but cancel operation
            assert result.exit_code == 0
            
            # Should show confirmation prompt and cancellation
            output = result.output
            assert "This will permanently delete" in output
            assert "confirmation-test-topic" in output
            assert "Are you sure you want to proceed?" in output
            assert "Operation cancelled" in output
            
        finally:
            Path(config_path).unlink()
    
    def test_cleanup_command_truncate_confirmation_prompt(self):
        """Test cleanup command shows truncate-specific confirmation prompt."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "localhost:9092"
            },
            "master_data": {
                "test_data": {
                    "kafka_topic": "truncate-confirmation-test"
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(config_dict, config_file, indent=2)
            config_path = config_file.name
        
        try:
            # Test with 'n' input to cancel
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'cleanup',
                '--strategy', 'truncate'
            ], input='n\n')
            
            # Should succeed but cancel operation
            assert result.exit_code == 0
            
            # Should show truncate-specific confirmation prompt
            output = result.output
            assert "This will truncate (clear all data from)" in output
            assert "truncate-confirmation-test" in output
            assert "Operation cancelled" in output
            
        finally:
            Path(config_path).unlink()
    
    def test_cleanup_command_skip_confirmation_with_confirm_flag(self):
        """Test cleanup command skips confirmation with --confirm flag."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "nonexistent:9092"  # Will fail at AdminClient creation
            },
            "master_data": {
                "test_data": {
                    "kafka_topic": "confirm-test-topic"
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(config_dict, config_file, indent=2)
            config_path = config_file.name
        
        try:
            # With --confirm, should not show confirmation prompt
            # Will fail at AdminClient creation stage, but that's expected
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'cleanup',
                '--confirm',
                '--strategy', 'delete'
            ])
            
            # Should NOT show confirmation prompt
            output = result.output
            assert "Are you sure you want to proceed?" not in output
            assert "Operation cancelled" not in output
            
            # Should proceed to deletion attempt (proving --confirm worked)
            assert "Deleting 1 topics..." in output
            
            # The operation should attempt to delete the topic (might fail due to timeout)
            assert "confirm-test-topic" in output
            
        finally:
            Path(config_path).unlink()


class TestTopicsCleanupValidation:
    """Test validation and error handling for topics cleanup."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
    
    def test_cleanup_invalid_strategy_option(self):
        """Test cleanup with invalid strategy option."""
        config_dict = {
            "kafka": {"bootstrap_servers": "localhost:9092"},
            "master_data": {"test": {"kafka_topic": "test-topic"}}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(config_dict, config_file, indent=2)
            config_path = config_file.name
        
        try:
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'cleanup',
                '--strategy', 'invalid_strategy'
            ])
            
            # Should fail with invalid choice
            assert result.exit_code == 2
            assert "Invalid value for '--strategy'" in result.output
            
        finally:
            Path(config_path).unlink()
    
    def test_cleanup_no_config_file_error(self):
        """Test cleanup without config file shows error."""
        result = self.runner.invoke(topics, ['cleanup'])
        
        # Should fail
        assert result.exit_code == 1
        assert "Configuration file is required for topic cleanup" in result.output
    
    def test_cleanup_missing_config_file_error(self):
        """Test cleanup with non-existent config file."""
        result = self.runner.invoke(topics, [
            '--config', 'nonexistent_config.json',
            'cleanup',
            '--dry-run'
        ])
        
        # Should fail (exit code 2 for Click argument errors)
        assert result.exit_code == 2
        assert "does not exist" in result.output
    
    def test_cleanup_invalid_json_config_error(self):
        """Test cleanup with invalid JSON config."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            config_file.write('{"invalid": json}')  # Invalid JSON
            config_path = config_file.name
        
        try:
            result = self.runner.invoke(topics, [
                '--config', config_path,
                'cleanup',
                '--dry-run'
            ])
            
            # Should fail
            assert result.exit_code == 1
            assert "Invalid JSON in configuration file" in result.output
            
        finally:
            Path(config_path).unlink()
    
    def test_cleanup_missing_kafka_config_error(self):
        """Test cleanup with missing kafka configuration."""
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
                'cleanup',
                '--dry-run'
            ])
            
            # Should fail
            assert result.exit_code == 1
            assert "Kafka configuration not found" in result.output
            
        finally:
            Path(config_path).unlink()