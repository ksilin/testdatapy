"""Comprehensive integration tests for Schema Registry operations.

This module tests the complete Schema Registry functionality including
registration, retrieval, compatibility checking, evolution, and caching.
"""

import json
import time
import tempfile
import unittest
from pathlib import Path
from typing import Dict, List, Any
import uuid

import pytest
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

from testdatapy.schema.registry_manager import SchemaRegistryManager
from testdatapy.schema.compiler import ProtobufCompiler
from testdatapy.schema.exceptions import SchemaRegistrationError, SchemaValidationError


@pytest.mark.integration
class TestSchemaRegistryIntegration(unittest.TestCase):
    """Integration tests for Schema Registry operations."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.schema_registry_url = "http://localhost:8081"
        cls.sr_client = SchemaRegistryClient({"url": cls.schema_registry_url})
        
        # Initialize schema management components
        cls.registry_manager = SchemaRegistryManager(
            schema_registry_url=cls.schema_registry_url
        )
        cls.protobuf_compiler = ProtobufCompiler()
        
        # Test subjects prefix to avoid conflicts
        cls.test_prefix = f"integration_test_{int(time.time())}"
        cls.test_subjects = []
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        # Clean up test subjects
        for subject in cls.test_subjects:
            try:
                cls.sr_client.delete_subject(subject)
            except Exception:
                pass
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_run_id = str(uuid.uuid4())[:8]
        
    def tearDown(self):
        """Clean up test fixtures."""
        pass
    
    def _get_test_subject(self, suffix: str = "") -> str:
        """Generate a unique test subject name."""
        subject = f"{self.test_prefix}_{self.test_run_id}_{suffix}"
        self.test_subjects.append(subject)
        return subject
    
    def _get_customer_proto_content(self) -> str:
        """Get customer.proto file content."""
        proto_file = Path(__file__).parent.parent.parent / "src" / "testdatapy" / "schemas" / "protobuf" / "customer.proto"
        return proto_file.read_text()
    
    def _get_order_proto_content(self) -> str:
        """Get order.proto file content."""
        proto_file = Path(__file__).parent.parent.parent / "src" / "testdatapy" / "schemas" / "protobuf" / "order.proto"
        return proto_file.read_text()
    
    def test_basic_schema_registration_and_retrieval(self):
        """Test basic schema registration and retrieval operations."""
        subject = self._get_test_subject("basic_reg")
        schema_content = self._get_customer_proto_content()
        
        # Test registration
        registration_result = self.registry_manager.register_protobuf_schema(
            subject=subject,
            schema_content=schema_content
        )
        
        self.assertTrue(registration_result['success'])
        self.assertGreater(registration_result['schema_id'], 0)
        self.assertEqual(registration_result['version'], 1)
        self.assertIn('subject_versions', registration_result)
        
        # Test retrieval by ID
        retrieved_by_id = self.registry_manager.get_schema_by_id(
            registration_result['schema_id']
        )
        self.assertTrue(retrieved_by_id['success'])
        self.assertIn('Customer', retrieved_by_id['schema_content'])
        
        # Test retrieval by subject and version
        retrieved_by_subject = self.registry_manager.get_schema_by_subject(
            subject=subject,
            version=1
        )
        self.assertTrue(retrieved_by_subject['success'])
        self.assertEqual(retrieved_by_subject['version'], 1)
        self.assertIn('Customer', retrieved_by_subject['schema_content'])
        
        # Test latest version retrieval
        latest_schema = self.registry_manager.get_latest_schema(subject)
        self.assertTrue(latest_schema['success'])
        self.assertEqual(latest_schema['version'], 1)
        self.assertEqual(latest_schema['schema_id'], registration_result['schema_id'])
    
    def test_schema_from_file_registration(self):
        """Test schema registration from .proto files."""
        subject = self._get_test_subject("from_file")
        proto_file = Path(__file__).parent.parent.parent / "src" / "testdatapy" / "schemas" / "protobuf" / "customer.proto"
        
        # Test registration from file
        registration_result = self.registry_manager.register_protobuf_schema_from_file(
            proto_file_path=proto_file,
            topic="test_topic",
            message_name="Customer"
        )
        
        self.assertTrue(registration_result['success'])
        self.assertGreater(registration_result['schema_id'], 0)
        self.assertIn('subject', registration_result)
        self.assertTrue(registration_result['subject'].endswith('-value'))
        
        # Verify the schema was registered correctly
        subject_used = registration_result['subject']
        self.test_subjects.append(subject_used)
        
        retrieved_schema = self.registry_manager.get_latest_schema(subject_used)
        self.assertTrue(retrieved_schema['success'])
        self.assertIn('Customer', retrieved_schema['schema_content'])
    
    def test_subject_management_and_naming_conventions(self):
        """Test subject management and naming conventions."""
        base_topic = f"test_topic_{self.test_run_id}"
        
        # Test value subject naming
        value_subject = self.registry_manager._get_subject_name(base_topic, "value")
        self.assertEqual(value_subject, f"{base_topic}-value")
        
        # Test key subject naming
        key_subject = self.registry_manager._get_subject_name(base_topic, "key")
        self.assertEqual(key_subject, f"{base_topic}-key")
        
        # Test custom subject naming
        custom_subject = self.registry_manager._get_subject_name(base_topic, "custom", "MyMessage")
        self.assertEqual(custom_subject, f"{base_topic}-custom-MyMessage")
        
        # Test listing subjects
        proto_file = Path(__file__).parent.parent.parent / "src" / "testdatapy" / "schemas" / "protobuf" / "customer.proto"
        
        # Register schemas for different subjects
        for i in range(3):
            topic = f"{base_topic}_{i}"
            result = self.registry_manager.register_protobuf_schema_from_file(
                proto_file_path=proto_file,
                topic=topic
            )
            self.assertTrue(result['success'])
            self.test_subjects.append(result['subject'])
        
        # List all subjects
        all_subjects = self.registry_manager.list_subjects()
        self.assertTrue(all_subjects['success'])
        
        # Check that our test subjects are in the list
        subject_names = all_subjects['subjects']
        for i in range(3):
            expected_subject = f"{base_topic}_{i}-value"
            self.assertIn(expected_subject, subject_names)
    
    def test_schema_compatibility_checking(self):
        """Test schema compatibility checking and evolution."""
        subject = self._get_test_subject("compatibility")
        
        # Register initial schema
        initial_schema = self._get_customer_proto_content()
        registration_result = self.registry_manager.register_protobuf_schema(
            subject=subject,
            schema_content=initial_schema
        )
        self.assertTrue(registration_result['success'])
        initial_version = registration_result['version']
        
        # Test compatibility with same schema (should be compatible)
        compatibility_result = self.registry_manager.check_schema_compatibility(
            subject=subject,
            schema_content=initial_schema,
            version='latest'
        )
        self.assertTrue(compatibility_result['compatible'])
        self.assertEqual(compatibility_result['level'], 'FULL')
        
        # Test compatibility with order schema (should be incompatible)
        order_schema = self._get_order_proto_content()
        incompatible_result = self.registry_manager.check_schema_compatibility(
            subject=subject,
            schema_content=order_schema,
            version='latest'
        )
        # This should be incompatible since it's a completely different message type
        self.assertFalse(incompatible_result['compatible'])
        
        # Test evolution recommendations
        evolution_result = self.registry_manager.analyze_schema_evolution(
            old_schema_content=initial_schema,
            new_schema_content=initial_schema  # Same schema
        )
        self.assertTrue(evolution_result['compatible'])
        self.assertEqual(evolution_result['compatibility_level'], 'FULL')
        self.assertEqual(len(evolution_result['breaking_changes']), 0)
    
    def test_multi_version_schema_management(self):
        """Test multi-version schema management."""
        subject = self._get_test_subject("multi_version")
        
        # Register first version
        schema_v1 = self._get_customer_proto_content()
        result_v1 = self.registry_manager.register_protobuf_schema(
            subject=subject,
            schema_content=schema_v1
        )
        self.assertTrue(result_v1['success'])
        self.assertEqual(result_v1['version'], 1)
        
        # Try to register the same schema again (should return existing version)
        result_duplicate = self.registry_manager.register_protobuf_schema(
            subject=subject,
            schema_content=schema_v1
        )
        self.assertTrue(result_duplicate['success'])
        self.assertEqual(result_duplicate['version'], 1)  # Should be same version
        self.assertEqual(result_duplicate['schema_id'], result_v1['schema_id'])
        
        # Get all versions
        versions_result = self.registry_manager.get_subject_versions(subject)
        self.assertTrue(versions_result['success'])
        self.assertIn(1, versions_result['versions'])
        
        # Get schema by specific version
        schema_v1_retrieved = self.registry_manager.get_schema_by_subject(
            subject=subject,
            version=1
        )
        self.assertTrue(schema_v1_retrieved['success'])
        self.assertEqual(schema_v1_retrieved['version'], 1)
        self.assertIn('Customer', schema_v1_retrieved['schema_content'])
    
    def test_schema_caching_and_performance(self):
        """Test schema caching mechanisms and performance."""
        subject = self._get_test_subject("caching")
        schema_content = self._get_customer_proto_content()
        
        # Register schema
        registration_result = self.registry_manager.register_protobuf_schema(
            subject=subject,
            schema_content=schema_content
        )
        self.assertTrue(registration_result['success'])
        schema_id = registration_result['schema_id']
        
        # First retrieval (should cache)
        start_time = time.time()
        first_retrieval = self.registry_manager.get_schema_by_id(schema_id)
        first_time = time.time() - start_time
        
        self.assertTrue(first_retrieval['success'])
        
        # Second retrieval (should use cache and be faster)
        start_time = time.time()
        second_retrieval = self.registry_manager.get_schema_by_id(schema_id)
        second_time = time.time() - start_time
        
        self.assertTrue(second_retrieval['success'])
        self.assertEqual(first_retrieval['schema_content'], second_retrieval['schema_content'])
        
        # Cache should make second retrieval faster (allow some tolerance for network variation)
        # In practice, cached retrieval should be much faster
        self.assertLessEqual(second_time, first_time + 0.1)  # Allow 100ms tolerance
        
        # Test cache statistics
        cache_stats = self.registry_manager.get_cache_statistics()
        self.assertIn('schema_cache', cache_stats)
        self.assertIn('subject_cache', cache_stats)
        self.assertGreaterEqual(cache_stats['schema_cache']['hits'], 1)
        
        # Test cache clearing
        self.registry_manager.clear_cache()
        cleared_stats = self.registry_manager.get_cache_statistics()
        self.assertEqual(cleared_stats['schema_cache']['size'], 0)
        self.assertEqual(cleared_stats['subject_cache']['size'], 0)
    
    def test_error_handling_scenarios(self):
        """Test various error handling scenarios."""
        
        # Test 1: Invalid schema content
        subject = self._get_test_subject("invalid_schema")
        invalid_schema = "invalid protobuf syntax {"
        
        with self.assertRaises((SchemaRegistrationError, Exception)):
            self.registry_manager.register_protobuf_schema(
                subject=subject,
                schema_content=invalid_schema
            )
        
        # Test 2: Non-existent schema retrieval
        non_existent_result = self.registry_manager.get_schema_by_id(99999999)
        self.assertFalse(non_existent_result['success'])
        self.assertIn('error', non_existent_result)
        
        # Test 3: Non-existent subject
        non_existent_subject = self.registry_manager.get_latest_schema("non_existent_subject_12345")
        self.assertFalse(non_existent_subject['success'])
        self.assertIn('error', non_existent_subject)
        
        # Test 4: Invalid version number
        valid_subject = self._get_test_subject("valid_for_invalid_version")
        valid_schema = self._get_customer_proto_content()
        
        # Register a valid schema first
        registration_result = self.registry_manager.register_protobuf_schema(
            subject=valid_subject,
            schema_content=valid_schema
        )
        self.assertTrue(registration_result['success'])
        
        # Try to get invalid version
        invalid_version_result = self.registry_manager.get_schema_by_subject(
            subject=valid_subject,
            version=999
        )
        self.assertFalse(invalid_version_result['success'])
        
        # Test 5: Schema Registry connection issues
        invalid_registry_manager = SchemaRegistryManager(
            schema_registry_url="http://invalid-host:8081"
        )
        
        connection_error_result = invalid_registry_manager.register_protobuf_schema(
            subject="test_subject",
            schema_content=valid_schema
        )
        self.assertFalse(connection_error_result['success'])
        self.assertIn('error', connection_error_result)
    
    def test_schema_metadata_and_references(self):
        """Test schema metadata handling and reference management."""
        subject = self._get_test_subject("metadata")
        schema_content = self._get_customer_proto_content()
        
        # Register schema with metadata
        registration_result = self.registry_manager.register_protobuf_schema(
            subject=subject,
            schema_content=schema_content
        )
        self.assertTrue(registration_result['success'])
        schema_id = registration_result['schema_id']
        
        # Get schema with metadata
        schema_with_metadata = self.registry_manager.get_schema_by_id(
            schema_id,
            include_metadata=True
        )
        self.assertTrue(schema_with_metadata['success'])
        self.assertIn('metadata', schema_with_metadata)
        
        metadata = schema_with_metadata['metadata']
        self.assertIn('schema_type', metadata)
        self.assertEqual(metadata['schema_type'], 'PROTOBUF')
        self.assertIn('registration_time', metadata)
        
        # Test subject metadata
        subject_info = self.registry_manager.get_subject_info(subject)
        self.assertTrue(subject_info['success'])
        self.assertEqual(subject_info['subject'], subject)
        self.assertIn('versions', subject_info)
        self.assertGreater(len(subject_info['versions']), 0)
    
    def test_bulk_operations_and_batch_processing(self):
        """Test bulk operations and batch processing."""
        base_subject = self._get_test_subject("bulk")
        schema_content = self._get_customer_proto_content()
        
        # Test bulk registration
        subjects_to_register = []
        for i in range(5):
            subject = f"{base_subject}_{i}"
            subjects_to_register.append(subject)
            self.test_subjects.append(subject)
        
        # Register multiple schemas
        successful_registrations = 0
        registration_results = []
        
        for subject in subjects_to_register:
            result = self.registry_manager.register_protobuf_schema(
                subject=subject,
                schema_content=schema_content
            )
            registration_results.append(result)
            if result['success']:
                successful_registrations += 1
        
        self.assertEqual(successful_registrations, len(subjects_to_register))
        
        # Test bulk retrieval
        schema_ids = [result['schema_id'] for result in registration_results if result['success']]
        
        retrieved_schemas = []
        for schema_id in schema_ids:
            result = self.registry_manager.get_schema_by_id(schema_id)
            if result['success']:
                retrieved_schemas.append(result)
        
        self.assertEqual(len(retrieved_schemas), len(schema_ids))
        
        # Verify all retrieved schemas have the same content
        for schema in retrieved_schemas:
            self.assertIn('Customer', schema['schema_content'])
    
    def test_schema_validation_and_compilation_integration(self):
        """Test integration between schema validation and compilation."""
        subject = self._get_test_subject("validation")
        
        # Test valid proto schema
        valid_proto_file = Path(__file__).parent.parent.parent / "src" / "testdatapy" / "schemas" / "protobuf" / "customer.proto"
        
        # Validate schema before registration
        validation_result = self.registry_manager.validate_protobuf_schema(
            schema_content=valid_proto_file.read_text()
        )
        self.assertTrue(validation_result['valid'])
        self.assertEqual(len(validation_result['errors']), 0)
        
        # Register validated schema
        registration_result = self.registry_manager.register_protobuf_schema_from_file(
            proto_file_path=valid_proto_file,
            topic=f"validation_topic_{self.test_run_id}"
        )
        self.assertTrue(registration_result['success'])
        self.test_subjects.append(registration_result['subject'])
        
        # Test compilation integration
        with tempfile.TemporaryDirectory() as temp_dir:
            compilation_result = self.protobuf_compiler.compile_proto_file(
                proto_file=valid_proto_file,
                output_dir=temp_dir
            )
            self.assertTrue(compilation_result.success)
            
            # Verify compiled files exist
            compiled_py_file = Path(temp_dir) / "customer_pb2.py"
            self.assertTrue(compiled_py_file.exists())
    
    def test_concurrent_schema_operations(self):
        """Test concurrent schema registry operations."""
        import threading
        import concurrent.futures
        
        base_subject = self._get_test_subject("concurrent")
        schema_content = self._get_customer_proto_content()
        
        def register_schema(subject_suffix: int) -> Dict[str, Any]:
            """Register a schema with given suffix."""
            subject = f"{base_subject}_{subject_suffix}"
            self.test_subjects.append(subject)
            return self.registry_manager.register_protobuf_schema(
                subject=subject,
                schema_content=schema_content
            )
        
        # Run concurrent registrations
        num_concurrent = 5
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_concurrent) as executor:
            futures = [executor.submit(register_schema, i) for i in range(num_concurrent)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        # All registrations should succeed
        successful_results = [r for r in results if r['success']]
        self.assertEqual(len(successful_results), num_concurrent)
        
        # All should have different schema IDs (different subjects)
        schema_ids = [r['schema_id'] for r in successful_results]
        self.assertEqual(len(set(schema_ids)), num_concurrent)
        
        # Test concurrent retrievals
        def retrieve_schema(schema_id: int) -> Dict[str, Any]:
            """Retrieve schema by ID."""
            return self.registry_manager.get_schema_by_id(schema_id)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_concurrent) as executor:
            retrieval_futures = [executor.submit(retrieve_schema, sid) for sid in schema_ids]
            retrieval_results = [future.result() for future in concurrent.futures.as_completed(retrieval_futures)]
        
        # All retrievals should succeed
        successful_retrievals = [r for r in retrieval_results if r['success']]
        self.assertEqual(len(successful_retrievals), num_concurrent)


if __name__ == '__main__':
    unittest.main()