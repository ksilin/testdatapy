"""Performance tests for schema compilation operations.

This module tests the performance characteristics of protobuf schema compilation,
including timing, memory usage, and scalability under various conditions.
"""

import gc
import os
import psutil
import tempfile
import time
import unittest
from pathlib import Path
from typing import Dict, List, Any
import uuid

import pytest

from testdatapy.schema.compiler import ProtobufCompiler
from testdatapy.schema.manager import SchemaManager
from testdatapy.schema.exceptions import CompilationError


class PerformanceTimer:
    """Context manager for measuring performance."""
    
    def __init__(self, description: str = ""):
        self.description = description
        self.start_time = None
        self.end_time = None
        self.duration = None
        self.start_memory = None
        self.end_memory = None
        self.memory_delta = None
    
    def __enter__(self):
        gc.collect()  # Clean up before measurement
        self.start_memory = psutil.Process().memory_info().rss
        self.start_time = time.perf_counter()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.perf_counter()
        self.end_memory = psutil.Process().memory_info().rss
        self.duration = self.end_time - self.start_time
        self.memory_delta = self.end_memory - self.start_memory
    
    def get_stats(self) -> Dict[str, Any]:
        return {
            "description": self.description,
            "duration": self.duration,
            "memory_delta_mb": self.memory_delta / (1024 * 1024) if self.memory_delta else 0,
            "start_memory_mb": self.start_memory / (1024 * 1024) if self.start_memory else 0,
            "end_memory_mb": self.end_memory / (1024 * 1024) if self.end_memory else 0
        }


@pytest.mark.performance
class TestSchemaCompilationPerformance(unittest.TestCase):
    """Performance tests for schema compilation."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.compiler = ProtobufCompiler()
        cls.schema_manager = SchemaManager()
        cls.test_schemas_dir = Path(__file__).parent.parent.parent / "src" / "testdatapy" / "schemas" / "protobuf"
        cls.performance_results = []
        
        # Performance thresholds (can be adjusted based on requirements)
        cls.compilation_time_threshold = 2.0  # seconds
        cls.memory_threshold = 50.0  # MB
        cls.bulk_compilation_threshold = 10.0  # seconds for 10 schemas
        
        print(f"🎯 Running performance tests with thresholds:")
        print(f"   • Single compilation: < {cls.compilation_time_threshold}s")
        print(f"   • Memory usage: < {cls.memory_threshold}MB per compilation")
        print(f"   • Bulk compilation: < {cls.bulk_compilation_threshold}s for 10 schemas")
    
    @classmethod
    def tearDownClass(cls):
        """Print performance summary."""
        if cls.performance_results:
            print(f"\\n📊 PERFORMANCE TEST SUMMARY")
            print("=" * 60)
            
            total_tests = len(cls.performance_results)
            avg_duration = sum(r['duration'] for r in cls.performance_results) / total_tests
            max_duration = max(r['duration'] for r in cls.performance_results)
            avg_memory = sum(r['memory_delta_mb'] for r in cls.performance_results) / total_tests
            max_memory = max(r['memory_delta_mb'] for r in cls.performance_results)
            
            print(f"Total Tests: {total_tests}")
            print(f"Average Duration: {avg_duration:.3f}s")
            print(f"Maximum Duration: {max_duration:.3f}s")
            print(f"Average Memory Delta: {avg_memory:.2f}MB")
            print(f"Maximum Memory Delta: {max_memory:.2f}MB")
            
            # Performance summary
            fast_tests = len([r for r in cls.performance_results if r['duration'] < cls.compilation_time_threshold])
            memory_efficient = len([r for r in cls.performance_results if r['memory_delta_mb'] < cls.memory_threshold])
            
            print(f"\\nPerformance Compliance:")
            print(f"Speed compliant: {fast_tests}/{total_tests} ({fast_tests/total_tests*100:.1f}%)")
            print(f"Memory compliant: {memory_efficient}/{total_tests} ({memory_efficient/total_tests*100:.1f}%)")
            
            print("=" * 60)
    
    def _record_performance(self, timer: PerformanceTimer):
        """Record performance results for summary."""
        stats = timer.get_stats()
        self.performance_results.append(stats)
        
        # Print individual test results
        print(f"   {stats['description']}: {stats['duration']:.3f}s, "
              f"{stats['memory_delta_mb']:.2f}MB delta")
        
        return stats
    
    def _create_test_proto(self, name: str, complexity: str = "simple") -> Path:
        """Create a test proto file with specified complexity."""
        temp_dir = Path(tempfile.mkdtemp())
        proto_file = temp_dir / f"{name}.proto"
        
        if complexity == "simple":
            content = f'''syntax = "proto3";
package test.performance;

message {name} {{
  string id = 1;
  string name = 2;
  string email = 3;
}}
'''
        elif complexity == "medium":
            content = f'''syntax = "proto3";
package test.performance;

message {name} {{
  string id = 1;
  string name = 2;
  string email = 3;
  repeated string tags = 4;
  
  message Address {{
    string street = 1;
    string city = 2;
    string postal_code = 3;
    string country = 4;
  }}
  
  Address address = 5;
  int64 created_at = 6;
  int64 updated_at = 7;
  bool active = 8;
  double score = 9;
  
  enum Status {{
    UNKNOWN = 0;
    ACTIVE = 1;
    INACTIVE = 2;
    SUSPENDED = 3;
  }}
  
  Status status = 10;
}}
'''
        elif complexity == "complex":
            content = f'''syntax = "proto3";
package test.performance;

message {name} {{
  string id = 1;
  string name = 2;
  string email = 3;
  repeated string tags = 4;
  map<string, string> metadata = 5;
  
  message Address {{
    string street = 1;
    string city = 2;
    string postal_code = 3;
    string country = 4;
    double latitude = 5;
    double longitude = 6;
    repeated string landmarks = 7;
  }}
  
  repeated Address addresses = 6;
  
  message ContactInfo {{
    string phone = 1;
    string mobile = 2;
    string fax = 3;
    string website = 4;
    repeated string social_media = 5;
  }}
  
  ContactInfo contact = 7;
  
  message Preferences {{
    bool email_notifications = 1;
    bool sms_notifications = 2;
    string language = 3;
    string timezone = 4;
    repeated string interests = 5;
    map<string, bool> feature_flags = 6;
  }}
  
  Preferences preferences = 8;
  
  message Audit {{
    string created_by = 1;
    int64 created_at = 2;
    string updated_by = 3;
    int64 updated_at = 4;
    int32 version = 5;
    repeated string changes = 6;
  }}
  
  Audit audit = 9;
  
  enum Status {{
    UNKNOWN = 0;
    PENDING = 1;
    ACTIVE = 2;
    INACTIVE = 3;
    SUSPENDED = 4;
    DELETED = 5;
  }}
  
  Status status = 10;
  
  enum Priority {{
    LOW = 0;
    MEDIUM = 1;
    HIGH = 2;
    CRITICAL = 3;
  }}
  
  Priority priority = 11;
  
  oneof verification {{
    string email_verification = 12;
    string phone_verification = 13;
    string document_verification = 14;
  }}
  
  repeated int32 numbers = 15;
  repeated double scores = 16;
  repeated bool flags = 17;
  bytes binary_data = 18;
}}
'''
        else:
            raise ValueError(f"Unknown complexity level: {complexity}")
        
        proto_file.write_text(content)
        return proto_file
    
    def test_single_schema_compilation_performance(self):
        """Test performance of compiling a single schema."""
        proto_file = self._create_test_proto("SinglePerfTest", "medium")
        
        with PerformanceTimer("Single schema compilation") as timer:
            with tempfile.TemporaryDirectory() as output_dir:
                result = self.compiler.compile_proto(str(proto_file), output_dir)
                self.assertTrue(result['success'])
        
        stats = self._record_performance(timer)
        
        # Performance assertions
        self.assertLess(stats['duration'], self.compilation_time_threshold,
                       f"Single compilation took too long: {stats['duration']:.3f}s")
        self.assertLess(stats['memory_delta_mb'], self.memory_threshold,
                       f"Single compilation used too much memory: {stats['memory_delta_mb']:.2f}MB")
    
    def test_multiple_complexity_levels(self):
        """Test compilation performance across different complexity levels."""
        complexities = ["simple", "medium", "complex"]
        results = {}
        
        for complexity in complexities:
            proto_file = self._create_test_proto(f"ComplexityTest{complexity.title()}", complexity)
            
            with PerformanceTimer(f"{complexity.title()} schema compilation") as timer:
                with tempfile.TemporaryDirectory() as output_dir:
                    result = self.compiler.compile_proto(str(proto_file), output_dir)
                    self.assertTrue(result['success'])
            
            stats = self._record_performance(timer)
            results[complexity] = stats
        
        # Verify that complexity correlates with compilation time
        self.assertLess(results['simple']['duration'], results['medium']['duration'],
                       "Simple schema should compile faster than medium")
        self.assertLess(results['medium']['duration'], results['complex']['duration'],
                       "Medium schema should compile faster than complex")
        
        # All should still be within reasonable bounds
        for complexity, stats in results.items():
            self.assertLess(stats['duration'], self.compilation_time_threshold * 2,
                           f"{complexity} compilation exceeded extended threshold")
    
    def test_bulk_compilation_performance(self):
        """Test performance of bulk compilation operations."""
        num_schemas = 10
        proto_files = []
        
        # Create multiple schemas
        for i in range(num_schemas):
            proto_file = self._create_test_proto(f"BulkTest{i:02d}", "medium")
            proto_files.append(proto_file)
        
        with PerformanceTimer(f"Bulk compilation ({num_schemas} schemas)") as timer:
            with tempfile.TemporaryDirectory() as output_dir:
                for proto_file in proto_files:
                    result = self.compiler.compile_proto(str(proto_file), output_dir)
                    self.assertTrue(result['success'])
        
        stats = self._record_performance(timer)
        
        # Performance assertions
        self.assertLess(stats['duration'], self.bulk_compilation_threshold,
                       f"Bulk compilation took too long: {stats['duration']:.3f}s")
        
        # Calculate per-schema average
        avg_per_schema = stats['duration'] / num_schemas
        self.assertLess(avg_per_schema, self.compilation_time_threshold,
                       f"Average per-schema time too high: {avg_per_schema:.3f}s")
    
    def test_repeated_compilation_caching_effects(self):
        """Test performance improvements from repeated compilation (potential caching)."""
        proto_file = self._create_test_proto("CacheTest", "medium")
        first_run_time = None
        subsequent_times = []
        
        # First compilation
        with PerformanceTimer("First compilation (cold)") as timer:
            with tempfile.TemporaryDirectory() as output_dir:
                result = self.compiler.compile_proto(str(proto_file), output_dir)
                self.assertTrue(result['success'])
        
        stats = self._record_performance(timer)
        first_run_time = stats['duration']
        
        # Subsequent compilations
        for i in range(3):
            with PerformanceTimer(f"Compilation {i+2} (warm)") as timer:
                with tempfile.TemporaryDirectory() as output_dir:
                    result = self.compiler.compile_proto(str(proto_file), output_dir)
                    self.assertTrue(result['success'])
            
            stats = self._record_performance(timer)
            subsequent_times.append(stats['duration'])
        
        # Analyze caching effects
        avg_subsequent = sum(subsequent_times) / len(subsequent_times)
        max_subsequent = max(subsequent_times)
        
        print(f"\\n   Caching analysis:")
        print(f"   • First run: {first_run_time:.3f}s")
        print(f"   • Avg subsequent: {avg_subsequent:.3f}s")
        print(f"   • Max subsequent: {max_subsequent:.3f}s")
        print(f"   • Improvement: {((first_run_time - avg_subsequent) / first_run_time * 100):.1f}%")
    
    def test_large_schema_compilation(self):
        """Test compilation of very large schemas."""
        # Create a schema with many fields
        temp_dir = Path(tempfile.mkdtemp())
        proto_file = temp_dir / "large_schema.proto"
        
        # Generate a large schema with 100 fields
        fields = []
        for i in range(100):
            fields.append(f"  string field_{i:03d} = {i + 1};")
        
        content = f'''syntax = "proto3";
package test.performance;

message LargeSchema {{
{chr(10).join(fields)}
}}
'''
        proto_file.write_text(content)
        
        with PerformanceTimer("Large schema compilation (100 fields)") as timer:
            with tempfile.TemporaryDirectory() as output_dir:
                result = self.compiler.compile_proto(str(proto_file), output_dir)
                self.assertTrue(result['success'])
        
        stats = self._record_performance(timer)
        
        # Large schemas should still compile reasonably quickly
        self.assertLess(stats['duration'], self.compilation_time_threshold * 3,
                       f"Large schema compilation took too long: {stats['duration']:.3f}s")
    
    def test_concurrent_compilation_performance(self):
        """Test performance under concurrent compilation load."""
        import concurrent.futures
        import threading
        
        num_concurrent = 4
        schemas_per_thread = 3
        
        def compile_schemas(thread_id: int) -> List[float]:
            """Compile multiple schemas in a thread."""
            times = []
            for i in range(schemas_per_thread):
                proto_file = self._create_test_proto(f"ConcurrentTest{thread_id}_{i}", "medium")
                
                start_time = time.perf_counter()
                with tempfile.TemporaryDirectory() as output_dir:
                    compiler = ProtobufCompiler()  # Each thread gets its own compiler
                    result = compiler.compile_proto(str(proto_file), output_dir)
                    self.assertTrue(result['success'])
                end_time = time.perf_counter()
                
                times.append(end_time - start_time)
            return times
        
        with PerformanceTimer(f"Concurrent compilation ({num_concurrent} threads)") as timer:
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_concurrent) as executor:
                futures = [executor.submit(compile_schemas, i) for i in range(num_concurrent)]
                all_times = []
                for future in concurrent.futures.as_completed(futures):
                    all_times.extend(future.result())
        
        stats = self._record_performance(timer)
        
        # Analyze concurrent performance
        total_schemas = num_concurrent * schemas_per_thread
        avg_time_per_schema = sum(all_times) / len(all_times)
        max_time_per_schema = max(all_times)
        
        print(f"\\n   Concurrent compilation analysis:")
        print(f"   • Total schemas: {total_schemas}")
        print(f"   • Threads: {num_concurrent}")
        print(f"   • Avg time per schema: {avg_time_per_schema:.3f}s")
        print(f"   • Max time per schema: {max_time_per_schema:.3f}s")
        print(f"   • Total wall time: {stats['duration']:.3f}s")
        
        # Performance assertions
        self.assertLess(avg_time_per_schema, self.compilation_time_threshold,
                       f"Concurrent average per-schema time too high: {avg_time_per_schema:.3f}s")
        self.assertLess(max_time_per_schema, self.compilation_time_threshold * 2,
                       f"Concurrent max per-schema time too high: {max_time_per_schema:.3f}s")
    
    def test_memory_usage_under_load(self):
        """Test memory usage patterns under sustained load."""
        initial_memory = psutil.Process().memory_info().rss
        peak_memory = initial_memory
        memory_samples = []
        
        num_iterations = 20
        
        for i in range(num_iterations):
            proto_file = self._create_test_proto(f"MemoryTest{i:02d}", "complex")
            
            # Measure memory before compilation
            pre_memory = psutil.Process().memory_info().rss
            
            with tempfile.TemporaryDirectory() as output_dir:
                result = self.compiler.compile_proto(str(proto_file), output_dir)
                self.assertTrue(result['success'])
            
            # Measure memory after compilation
            post_memory = psutil.Process().memory_info().rss
            peak_memory = max(peak_memory, post_memory)
            
            memory_delta = (post_memory - pre_memory) / (1024 * 1024)  # MB
            memory_samples.append(memory_delta)
            
            # Force garbage collection
            gc.collect()
        
        # Analyze memory usage
        avg_memory_per_compilation = sum(memory_samples) / len(memory_samples)
        max_memory_per_compilation = max(memory_samples)
        peak_total_memory = (peak_memory - initial_memory) / (1024 * 1024)  # MB
        
        print(f"\\n   Memory usage analysis:")
        print(f"   • Iterations: {num_iterations}")
        print(f"   • Avg memory per compilation: {avg_memory_per_compilation:.2f}MB")
        print(f"   • Max memory per compilation: {max_memory_per_compilation:.2f}MB")
        print(f"   • Peak total memory increase: {peak_total_memory:.2f}MB")
        
        # Memory assertions
        self.assertLess(avg_memory_per_compilation, self.memory_threshold,
                       f"Average memory usage too high: {avg_memory_per_compilation:.2f}MB")
        self.assertLess(peak_total_memory, self.memory_threshold * 5,
                       f"Peak memory usage too high: {peak_total_memory:.2f}MB")
    
    def test_error_handling_performance(self):
        """Test performance of error handling paths."""
        # Create invalid proto file
        temp_dir = Path(tempfile.mkdtemp())
        invalid_proto = temp_dir / "invalid.proto"
        invalid_proto.write_text("invalid protobuf syntax {")
        
        with PerformanceTimer("Error handling compilation") as timer:
            with tempfile.TemporaryDirectory() as output_dir:
                with self.assertRaises(CompilationError):
                    self.compiler.compile_proto(str(invalid_proto), output_dir)
        
        stats = self._record_performance(timer)
        
        # Error handling should be fast
        self.assertLess(stats['duration'], self.compilation_time_threshold / 2,
                       f"Error handling took too long: {stats['duration']:.3f}s")
    
    def test_real_world_schemas_performance(self):
        """Test performance with real-world schema files from the project."""
        real_schemas = [
            "customer.proto",
            "order.proto", 
            "payment.proto",
            "product.proto",
            "delivery.proto"
        ]
        
        total_time = 0
        schema_times = {}
        
        for schema_name in real_schemas:
            schema_path = self.test_schemas_dir / schema_name
            if not schema_path.exists():
                self.skipTest(f"Schema file not found: {schema_path}")
            
            with PerformanceTimer(f"Real schema: {schema_name}") as timer:
                with tempfile.TemporaryDirectory() as output_dir:
                    result = self.compiler.compile_proto(str(schema_path), output_dir)
                    self.assertTrue(result['success'])
            
            stats = self._record_performance(timer)
            schema_times[schema_name] = stats['duration']
            total_time += stats['duration']
        
        # Analyze real-world performance
        avg_real_world_time = total_time / len(real_schemas)
        max_real_world_time = max(schema_times.values())
        
        print(f"\\n   Real-world schema analysis:")
        print(f"   • Schemas tested: {len(real_schemas)}")
        print(f"   • Total time: {total_time:.3f}s")
        print(f"   • Average time: {avg_real_world_time:.3f}s")
        print(f"   • Max time: {max_real_world_time:.3f}s")
        
        # Performance assertions for real schemas
        self.assertLess(avg_real_world_time, self.compilation_time_threshold,
                       f"Real-world average compilation time too high: {avg_real_world_time:.3f}s")
        self.assertLess(max_real_world_time, self.compilation_time_threshold * 1.5,
                       f"Real-world max compilation time too high: {max_real_world_time:.3f}s")


if __name__ == '__main__':
    unittest.main()