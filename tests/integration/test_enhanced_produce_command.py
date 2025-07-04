"""Integration tests for enhanced produce command with protobuf options."""
import subprocess
import unittest
import tempfile
from pathlib import Path

import pytest


@pytest.mark.integration
class TestEnhancedProduceCommand(unittest.TestCase):
    """Integration tests for enhanced produce command."""
    
    def test_produce_command_help_shows_new_options(self):
        """Test that help shows all new protobuf options."""
        result = subprocess.run(
            ["python", "-m", "testdatapy.cli", "produce", "--help"],
            capture_output=True,
            text=True
        )
        
        self.assertEqual(result.returncode, 0)
        help_text = result.stdout
        
        # Verify new options are present
        self.assertIn("--proto-module", help_text)
        self.assertIn("--schema-path", help_text)
        self.assertIn("--auto-register", help_text)
        self.assertIn("Pre-compiled protobuf module", help_text)
        self.assertIn("Custom schema directory", help_text)
        self.assertIn("Automatically register protobuf schemas", help_text)

    def test_backward_compatibility_json_format(self):
        """Test that existing JSON format still works."""
        result = subprocess.run([
            "python", "-m", "testdatapy.cli", "produce",
            "--topic", "test",
            "--format", "json", 
            "--generator", "faker",
            "--count", "1",
            "--dry-run"
        ], capture_output=True, text=True)
        
        self.assertEqual(result.returncode, 0)
        self.assertIn("Starting data generation", result.stdout)
        self.assertIn("Format: json", result.stdout)
        self.assertIn("CustomerID", result.stdout)

    def test_proto_module_option(self):
        """Test new proto-module option works."""
        result = subprocess.run([
            "python", "-m", "testdatapy.cli", "produce",
            "--topic", "test",
            "--format", "protobuf",
            "--proto-module", "customer_pb2",
            "--count", "1", 
            "--dry-run"
        ], capture_output=True, text=True)
        
        self.assertEqual(result.returncode, 0)
        self.assertIn("Format: protobuf", result.stdout)
        self.assertIn("CustomerID", result.stdout)

    def test_proto_file_option(self):
        """Test new proto-file option works."""
        proto_file = Path("src/testdatapy/schemas/protobuf/customer.proto")
        if proto_file.exists():
            result = subprocess.run([
                "python", "-m", "testdatapy.cli", "produce",
                "--topic", "test",
                "--format", "protobuf",
                "--proto-file", str(proto_file),
                "--count", "1",
                "--dry-run"
            ], capture_output=True, text=True)
            
            self.assertEqual(result.returncode, 0)
            self.assertIn("Format: protobuf", result.stdout)

    def test_proto_class_option_backward_compatibility(self):
        """Test existing proto-class option still works."""
        result = subprocess.run([
            "python", "-m", "testdatapy.cli", "produce", 
            "--topic", "test",
            "--format", "protobuf",
            "--proto-class", "customer_pb2.Customer",
            "--count", "1",
            "--dry-run"
        ], capture_output=True, text=True)
        
        self.assertEqual(result.returncode, 0)
        self.assertIn("Format: protobuf", result.stdout)

    def test_schema_path_option(self):
        """Test new schema-path option works."""
        result = subprocess.run([
            "python", "-m", "testdatapy.cli", "produce",
            "--topic", "test", 
            "--format", "protobuf",
            "--proto-module", "customer_pb2",
            "--schema-path", "src/testdatapy/schemas/protobuf",
            "--count", "1",
            "--dry-run"
        ], capture_output=True, text=True)
        
        self.assertEqual(result.returncode, 0)
        self.assertIn("Format: protobuf", result.stdout)

    def test_multiple_schema_paths(self):
        """Test multiple schema-path options work."""
        result = subprocess.run([
            "python", "-m", "testdatapy.cli", "produce",
            "--topic", "test",
            "--format", "protobuf", 
            "--proto-module", "customer_pb2",
            "--schema-path", "src/testdatapy/schemas/protobuf",
            "--schema-path", "src/testdatapy/schemas",
            "--count", "1",
            "--dry-run"
        ], capture_output=True, text=True)
        
        self.assertEqual(result.returncode, 0)
        self.assertIn("Format: protobuf", result.stdout)

    def test_validation_errors(self):
        """Test validation errors for protobuf options."""
        # Test missing protobuf specification
        result = subprocess.run([
            "python", "-m", "testdatapy.cli", "produce",
            "--topic", "test",
            "--format", "protobuf",
            "--count", "1",
            "--dry-run"
        ], capture_output=True, text=True)
        
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("requires one of", result.stderr)

    def test_conflicting_options_validation(self):
        """Test validation prevents conflicting protobuf options."""
        result = subprocess.run([
            "python", "-m", "testdatapy.cli", "produce",
            "--topic", "test",
            "--format", "protobuf",
            "--proto-class", "customer_pb2.Customer",
            "--proto-module", "customer_pb2",
            "--count", "1",
            "--dry-run"
        ], capture_output=True, text=True)
        
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Can only specify one of", result.stderr)

    def test_auto_register_option_default(self):
        """Test auto-register defaults to False."""
        result = subprocess.run([
            "python", "-m", "testdatapy.cli", "produce",
            "--topic", "test",
            "--format", "protobuf",
            "--proto-module", "customer_pb2", 
            "--count", "1",
            "--dry-run"
        ], capture_output=True, text=True)
        
        self.assertEqual(result.returncode, 0)
        # Should not see schema registration message in dry-run mode
        self.assertNotIn("Registered protobuf schema", result.stdout)

    def test_produce_command_maintains_all_existing_options(self):
        """Test that all existing options are still available."""
        result = subprocess.run([
            "python", "-m", "testdatapy.cli", "produce", "--help"
        ], capture_output=True, text=True)
        
        help_text = result.stdout
        
        # Verify existing options are still present
        existing_options = [
            "--config", "--topic", "--format", "--generator",
            "--schema-file", "--csv-file", "--key-field", 
            "--rate", "--count", "--duration", "--seed",
            "--dry-run", "--metrics", "--health",
            "--auto-create-topic"
        ]
        
        for option in existing_options:
            self.assertIn(option, help_text, f"Missing existing option: {option}")


if __name__ == '__main__':
    unittest.main()