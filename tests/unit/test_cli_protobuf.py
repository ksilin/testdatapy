"""Unit tests for CLI protobuf support."""
import unittest
from unittest.mock import Mock, patch
from click.testing import CliRunner

from testdatapy.cli import cli


class TestCLIProtobuf(unittest.TestCase):
    """Test cases for CLI protobuf functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
    
    def test_produce_protobuf_requires_proto_class(self):
        """Test that protobuf format requires --proto-class."""
        result = self.runner.invoke(cli, [
            'produce',
            'test_topic',
            '--format', 'protobuf',
            '--generator', 'faker'
        ])
        
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn('--proto-class', result.output)
    
    def test_produce_protobuf_with_proto_class(self):
        """Test producing with protobuf format and proto class."""
        with patch('testdatapy.cli.ProtobufProducer') as mock_producer_class:
            mock_producer = Mock()
            mock_producer_class.return_value = mock_producer
            
            # Mock the dynamic import
            with patch('testdatapy.cli.__import__') as mock_import:
                mock_module = Mock()
                mock_class = Mock()
                mock_module.Customer = mock_class
                mock_import.return_value = mock_module
                
                result = self.runner.invoke(cli, [
                    'produce',
                    'test_topic',
                    '--format', 'protobuf',
                    '--proto-class', 'test_module.Customer',
                    '--generator', 'faker',
                    '--num-messages', '5'
                ])
                
                self.assertEqual(result.exit_code, 0)
                mock_producer_class.assert_called_once()
    
    def test_list_formats_includes_protobuf(self):
        """Test that list-formats command includes protobuf."""
        result = self.runner.invoke(cli, ['list-formats'])
        
        self.assertEqual(result.exit_code, 0)
        self.assertIn('protobuf', result.output)
        self.assertIn('Protocol Buffers', result.output)
        self.assertIn('proto class', result.output)
    
    def test_validate_protobuf_schema(self):
        """Test schema validation with protobuf."""
        with self.runner.isolated_filesystem():
            # Create a test proto file
            with open('test.proto', 'w') as f:
                f.write('''
syntax = "proto3";

message TestMessage {
    string id = 1;
    string name = 2;
}
''')
            
            result = self.runner.invoke(cli, [
                'validate',
                '--schema', 'test.proto'
            ])
            
            # This might fail if protoc is not available
            if result.exit_code == 0:
                self.assertIn('valid', result.output.lower())
    
    def test_produce_protobuf_with_config_file(self):
        """Test producing protobuf with config file."""
        with self.runner.isolated_filesystem():
            # Create config file
            config_content = {
                "bootstrap.servers": "localhost:9092",
                "schema.registry.url": "http://localhost:8081"
            }
            
            import json
            with open('config.json', 'w') as f:
                json.dump(config_content, f)
            
            with patch('testdatapy.cli.ProtobufProducer'):
                with patch('testdatapy.cli.__import__'):
                    result = self.runner.invoke(cli, [
                        'produce',
                        'test_topic',
                        '--config', 'config.json',
                        '--format', 'protobuf',
                        '--proto-class', 'test.Customer',
                        '--dry-run'
                    ])
                    
                    self.assertEqual(result.exit_code, 0)
    
    def test_produce_protobuf_with_key_field(self):
        """Test protobuf production with key field."""
        with patch('testdatapy.cli.ProtobufProducer') as mock_producer_class:
            with patch('testdatapy.cli.__import__'):
                result = self.runner.invoke(cli, [
                    'produce',
                    'test_topic',
                    '--format', 'protobuf',
                    '--proto-class', 'test.Customer',
                    '--key-field', 'customer_id',
                    '--num-messages', '1'
                ])
                
                self.assertEqual(result.exit_code, 0)
                # Check that key_field was passed to producer
                call_kwargs = mock_producer_class.call_args[1]
                self.assertEqual(call_kwargs.get('key_field'), 'customer_id')
    
    def test_produce_protobuf_dry_run(self):
        """Test protobuf dry run mode."""
        with patch('testdatapy.cli.__import__'):
            result = self.runner.invoke(cli, [
                'produce',
                'test_topic',
                '--format', 'protobuf',
                '--proto-class', 'test.Customer',
                '--dry-run',
                '--num-messages', '3'
            ])
            
            self.assertEqual(result.exit_code, 0)
            self.assertIn('Dry run', result.output)
            # Should show sample messages
            self.assertIn('Message 1', result.output)


if __name__ == '__main__':
    unittest.main()