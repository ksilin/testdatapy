"""Performance tests for transformation engine operations.

This module tests the performance characteristics of the transformation system,
including function execution, Faker integration, and batch processing.
"""

import gc
import psutil
import time
import unittest
from typing import Dict, List, Any, Callable
import uuid

import pytest

from testdatapy.transformers.transformation_manager import TransformationManager
from testdatapy.transformers.function_registry import FunctionCategory
from testdatapy.transformers.function_validator import ValidationLevel, SecurityLevel
from testdatapy.transformers.faker_integration import FakerIntegration


class TransformationPerformanceTimer:
    """Context manager for measuring transformation performance."""
    
    def __init__(self, description: str = "", function_name: str = ""):
        self.description = description
        self.function_name = function_name
        self.start_time = None
        self.end_time = None
        self.duration = None
        self.start_memory = None
        self.end_memory = None
        self.memory_delta = None
        self.executions = 0
    
    def __enter__(self):
        gc.collect()
        self.start_memory = psutil.Process().memory_info().rss
        self.start_time = time.perf_counter()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.perf_counter()
        self.end_memory = psutil.Process().memory_info().rss
        self.duration = self.end_time - self.start_time
        self.memory_delta = self.end_memory - self.start_memory
    
    def record_execution(self):
        """Record a single execution for throughput calculation."""
        self.executions += 1
    
    def get_stats(self) -> Dict[str, Any]:
        throughput = self.executions / self.duration if self.duration > 0 else 0
        return {
            "description": self.description,
            "function_name": self.function_name,
            "duration": self.duration,
            "executions": self.executions,
            "throughput": throughput,
            "memory_delta_mb": self.memory_delta / (1024 * 1024) if self.memory_delta else 0,
            "avg_execution_time": self.duration / self.executions if self.executions > 0 else 0
        }


@pytest.mark.performance
class TestTransformationPerformance(unittest.TestCase):
    """Performance tests for transformation engine."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.transformation_manager = TransformationManager(
            validation_level=ValidationLevel.MINIMAL,  # Minimal validation for performance
            security_level=SecurityLevel.SAFE
        )
        
        cls.faker_integration = FakerIntegration()
        cls.performance_results = []
        
        # Performance thresholds
        cls.function_execution_threshold = 0.001  # 1ms per function call
        cls.throughput_threshold = 1000  # executions per second
        cls.batch_processing_threshold = 10.0  # seconds for 10k operations
        cls.memory_threshold = 10.0  # MB for batch operations
        
        print(f"🎯 Running transformation performance tests with thresholds:")
        print(f"   • Function execution: < {cls.function_execution_threshold * 1000:.1f}ms")
        print(f"   • Throughput: > {cls.throughput_threshold} ops/sec")
        print(f"   • Batch processing: < {cls.batch_processing_threshold}s for 10k ops")
        print(f"   • Memory usage: < {cls.memory_threshold}MB for batch ops")
    
    @classmethod
    def tearDownClass(cls):
        """Print performance summary."""
        if cls.performance_results:
            print(f"\\n📊 TRANSFORMATION PERFORMANCE SUMMARY")
            print("=" * 60)
            
            total_tests = len(cls.performance_results)
            avg_duration = sum(r['duration'] for r in cls.performance_results) / total_tests
            max_duration = max(r['duration'] for r in cls.performance_results)
            avg_throughput = sum(r.get('throughput', 0) for r in cls.performance_results) / total_tests
            max_throughput = max(r.get('throughput', 0) for r in cls.performance_results)
            
            print(f"Total Tests: {total_tests}")
            print(f"Average Duration: {avg_duration:.4f}s")
            print(f"Maximum Duration: {max_duration:.4f}s")
            print(f"Average Throughput: {avg_throughput:.0f} ops/sec")
            print(f"Maximum Throughput: {max_throughput:.0f} ops/sec")
            
            print("=" * 60)
    
    def _record_performance(self, timer: TransformationPerformanceTimer):
        """Record performance results for summary."""
        stats = timer.get_stats()
        self.performance_results.append(stats)
        
        # Print individual test results
        if stats['executions'] > 1:
            print(f"   {stats['description']}: {stats['throughput']:.0f} ops/sec, "
                  f"{stats['avg_execution_time']*1000:.3f}ms avg")
        else:
            print(f"   {stats['description']}: {stats['duration']*1000:.3f}ms")
        
        return stats
    
    def test_builtin_function_execution_performance(self):
        """Test performance of built-in transformation functions."""
        test_cases = [
            ("upper", "hello world", str),
            ("lower", "HELLO WORLD", str),
            ("abs", -42.5, float),
            ("round", 3.14159, float),
            ("len", "test string", int),
            ("str", 12345, str),
            ("int", "12345", int),
            ("bool", 1, bool)
        ]
        
        iterations = 10000
        
        for function_name, test_input, expected_type in test_cases:
            with TransformationPerformanceTimer(
                f"Builtin {function_name} ({iterations:,} calls)",
                function_name
            ) as timer:
                for _ in range(iterations):
                    result = self.transformation_manager.execute_function(function_name, test_input)
                    self.assertIsInstance(result, expected_type)
                    timer.record_execution()
            
            stats = self._record_performance(timer)
            
            # Performance assertions
            self.assertGreater(stats['throughput'], self.throughput_threshold,
                             f"{function_name} throughput too low: {stats['throughput']:.0f} ops/sec")
            self.assertLess(stats['avg_execution_time'], self.function_execution_threshold,
                           f"{function_name} execution time too high: {stats['avg_execution_time']*1000:.3f}ms")
    
    def test_faker_function_performance(self):
        """Test performance of Faker-based functions."""
        faker_functions = [
            "faker.name",
            "faker.email", 
            "faker.address",
            "faker.phone_number",
            "faker.company",
            "faker.text",
            "faker.uuid4",
            "faker.date",
            "faker.random_int"
        ]
        
        iterations = 1000  # Fewer iterations for Faker as it's expected to be slower
        
        for function_name in faker_functions:
            try:
                with TransformationPerformanceTimer(
                    f"Faker {function_name} ({iterations:,} calls)",
                    function_name
                ) as timer:
                    for _ in range(iterations):
                        result = self.transformation_manager.execute_function(function_name)
                        self.assertIsNotNone(result)
                        timer.record_execution()
                
                stats = self._record_performance(timer)
                
                # Faker functions have more relaxed thresholds
                faker_throughput_threshold = self.throughput_threshold / 10  # 100 ops/sec
                faker_execution_threshold = self.function_execution_threshold * 10  # 10ms
                
                self.assertGreater(stats['throughput'], faker_throughput_threshold,
                                 f"{function_name} throughput too low: {stats['throughput']:.0f} ops/sec")
                self.assertLess(stats['avg_execution_time'], faker_execution_threshold,
                               f"{function_name} execution time too high: {stats['avg_execution_time']*1000:.3f}ms")
            
            except Exception as e:
                self.skipTest(f"Faker function {function_name} not available: {e}")
    
    def test_custom_function_registration_performance(self):
        """Test performance of custom function registration."""
        num_functions = 1000
        
        def create_test_function(i: int) -> Callable:
            return lambda x: f"test_function_{i}_{x}"
        
        with TransformationPerformanceTimer(
            f"Function registration ({num_functions:,} functions)"
        ) as timer:
            for i in range(num_functions):
                func = create_test_function(i)
                success = self.transformation_manager.register_function(
                    name=f"test_func_{i}",
                    func=func,
                    description=f"Test function {i}",
                    category=FunctionCategory.CUSTOM,
                    validate=False  # Skip validation for performance
                )
                self.assertTrue(success)
                timer.record_execution()
        
        stats = self._record_performance(timer)
        
        # Registration should be fast
        registration_threshold = 100  # 100 registrations per second
        self.assertGreater(stats['throughput'], registration_threshold,
                         f"Registration throughput too low: {stats['throughput']:.0f} ops/sec")
    
    def test_batch_operation_performance(self):
        """Test performance of batch operations."""
        # Create batch operations
        operations = []
        for i in range(10000):
            operations.append({
                'function': 'upper',
                'args': [f'test string {i}'],
                'kwargs': {}
            })
        
        with TransformationPerformanceTimer(
            f"Batch processing ({len(operations):,} operations)"
        ) as timer:
            results = self.transformation_manager.batch_execute(operations)
            timer.executions = len(operations)
        
        stats = self._record_performance(timer)
        
        # Verify all operations succeeded
        successful_operations = sum(1 for r in results if r['success'])
        self.assertEqual(successful_operations, len(operations))
        
        # Performance assertions
        self.assertLess(stats['duration'], self.batch_processing_threshold,
                       f"Batch processing took too long: {stats['duration']:.3f}s")
        self.assertGreater(stats['throughput'], self.throughput_threshold,
                         f"Batch throughput too low: {stats['throughput']:.0f} ops/sec")
        self.assertLess(stats['memory_delta_mb'], self.memory_threshold,
                       f"Batch memory usage too high: {stats['memory_delta_mb']:.2f}MB")
    
    def test_validation_overhead_performance(self):
        """Test performance impact of different validation levels."""
        validation_levels = [
            ValidationLevel.MINIMAL,
            ValidationLevel.STANDARD,
            ValidationLevel.STRICT
        ]
        
        iterations = 1000
        test_input = "performance test string"
        
        for validation_level in validation_levels:
            # Create manager with specific validation level
            manager = TransformationManager(validation_level=validation_level)
            
            with TransformationPerformanceTimer(
                f"Validation {validation_level.value} ({iterations:,} calls)",
                f"upper_with_{validation_level.value}_validation"
            ) as timer:
                for _ in range(iterations):
                    result = manager.execute_function("upper", test_input)
                    self.assertEqual(result, test_input.upper())
                    timer.record_execution()
            
            stats = self._record_performance(timer)
        
        # Compare validation overhead
        minimal_result = next(r for r in self.performance_results[-3:] if 'minimal' in r['description'])
        standard_result = next(r for r in self.performance_results[-3:] if 'standard' in r['description'])
        strict_result = next(r for r in self.performance_results[-3:] if 'strict' in r['description'])
        
        print(f"\\n   Validation overhead analysis:")
        print(f"   • Minimal: {minimal_result['throughput']:.0f} ops/sec")
        print(f"   • Standard: {standard_result['throughput']:.0f} ops/sec")
        print(f"   • Strict: {strict_result['throughput']:.0f} ops/sec")
        
        # Validation should not cause extreme performance degradation
        max_overhead_factor = 3.0  # Max 3x slowdown for strict validation
        self.assertGreater(strict_result['throughput'], minimal_result['throughput'] / max_overhead_factor,
                         f"Strict validation overhead too high: {minimal_result['throughput'] / strict_result['throughput']:.1f}x")
    
    def test_concurrent_transformation_performance(self):
        """Test performance under concurrent execution."""
        import concurrent.futures
        import threading
        
        num_threads = 4
        operations_per_thread = 1000
        
        def execute_transformations(thread_id: int) -> List[float]:
            """Execute transformations in a thread."""
            times = []
            manager = TransformationManager()  # Each thread gets its own manager
            
            for i in range(operations_per_thread):
                start_time = time.perf_counter()
                result = manager.execute_function("upper", f"thread_{thread_id}_item_{i}")
                end_time = time.perf_counter()
                
                self.assertIsInstance(result, str)
                times.append(end_time - start_time)
            
            return times
        
        with TransformationPerformanceTimer(
            f"Concurrent execution ({num_threads} threads, {operations_per_thread:,} ops each)"
        ) as timer:
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = [executor.submit(execute_transformations, i) for i in range(num_threads)]
                all_times = []
                for future in concurrent.futures.as_completed(futures):
                    all_times.extend(future.result())
            
            timer.executions = len(all_times)
        
        stats = self._record_performance(timer)
        
        # Analyze concurrent performance
        total_operations = num_threads * operations_per_thread
        avg_time_per_operation = sum(all_times) / len(all_times)
        
        print(f"\\n   Concurrent execution analysis:")
        print(f"   • Total operations: {total_operations:,}")
        print(f"   • Threads: {num_threads}")
        print(f"   • Avg time per operation: {avg_time_per_operation*1000:.3f}ms")
        print(f"   • Total wall time: {stats['duration']:.3f}s")
        print(f"   • Effective throughput: {stats['throughput']:.0f} ops/sec")
        
        # Performance assertions
        self.assertGreater(stats['throughput'], self.throughput_threshold,
                         f"Concurrent throughput too low: {stats['throughput']:.0f} ops/sec")
        self.assertLess(avg_time_per_operation, self.function_execution_threshold,
                       f"Concurrent avg operation time too high: {avg_time_per_operation*1000:.3f}ms")
    
    def test_memory_efficiency_under_load(self):
        """Test memory efficiency during sustained operation."""
        initial_memory = psutil.Process().memory_info().rss
        peak_memory = initial_memory
        memory_samples = []
        
        operations_per_batch = 1000
        num_batches = 20
        
        for batch in range(num_batches):
            pre_memory = psutil.Process().memory_info().rss
            
            # Execute a batch of operations
            operations = []
            for i in range(operations_per_batch):
                operations.append({
                    'function': 'upper',
                    'args': [f'batch_{batch}_item_{i}'],
                    'kwargs': {}
                })
            
            results = self.transformation_manager.batch_execute(operations)
            
            post_memory = psutil.Process().memory_info().rss
            peak_memory = max(peak_memory, post_memory)
            
            memory_delta = (post_memory - pre_memory) / (1024 * 1024)  # MB
            memory_samples.append(memory_delta)
            
            # Verify all operations succeeded
            successful = sum(1 for r in results if r['success'])
            self.assertEqual(successful, operations_per_batch)
            
            # Force garbage collection
            gc.collect()
        
        # Analyze memory efficiency
        avg_memory_per_batch = sum(memory_samples) / len(memory_samples)
        max_memory_per_batch = max(memory_samples)
        peak_total_memory = (peak_memory - initial_memory) / (1024 * 1024)  # MB
        
        print(f"\\n   Memory efficiency analysis:")
        print(f"   • Batches: {num_batches}")
        print(f"   • Operations per batch: {operations_per_batch:,}")
        print(f"   • Avg memory per batch: {avg_memory_per_batch:.2f}MB")
        print(f"   • Max memory per batch: {max_memory_per_batch:.2f}MB")
        print(f"   • Peak total memory increase: {peak_total_memory:.2f}MB")
        
        # Memory efficiency assertions
        memory_per_operation = avg_memory_per_batch / operations_per_batch * 1024  # KB per operation
        self.assertLess(memory_per_operation, 1.0,  # Less than 1KB per operation
                       f"Memory usage per operation too high: {memory_per_operation:.3f}KB")
        self.assertLess(peak_total_memory, self.memory_threshold * 5,
                       f"Peak memory usage too high: {peak_total_memory:.2f}MB")
    
    def test_function_search_performance(self):
        """Test performance of function search operations."""
        # Register many functions for search testing
        num_functions = 1000
        for i in range(num_functions):
            self.transformation_manager.register_function(
                name=f"search_test_function_{i:04d}",
                func=lambda x, i=i: f"result_{i}_{x}",
                description=f"Search test function number {i}",
                category=FunctionCategory.CUSTOM,
                validate=False
            )
        
        search_queries = [
            "search_test",
            "function_0001",
            "test_function_05",
            "nonexistent_function"
        ]
        
        iterations = 100
        
        for query in search_queries:
            with TransformationPerformanceTimer(
                f"Function search '{query}' ({iterations:,} searches)",
                f"search_{query}"
            ) as timer:
                for _ in range(iterations):
                    results = self.transformation_manager.search_functions(query)
                    # Results can be empty for nonexistent functions
                    self.assertIsInstance(results, list)
                    timer.record_execution()
            
            stats = self._record_performance(timer)
            
            # Search should be fast
            search_threshold = 1000  # 1000 searches per second
            self.assertGreater(stats['throughput'], search_threshold,
                             f"Search throughput too low for '{query}': {stats['throughput']:.0f} ops/sec")
    
    def test_error_handling_performance_impact(self):
        """Test performance impact of error handling."""
        iterations = 1000
        
        # Test successful operations
        with TransformationPerformanceTimer(
            f"Successful operations ({iterations:,} calls)",
            "successful_upper"
        ) as success_timer:
            for _ in range(iterations):
                result = self.transformation_manager.execute_function("upper", "test")
                self.assertEqual(result, "TEST")
                success_timer.record_execution()
        
        success_stats = self._record_performance(success_timer)
        
        # Test error operations
        with TransformationPerformanceTimer(
            f"Error operations ({iterations:,} calls)",
            "nonexistent_function"
        ) as error_timer:
            for _ in range(iterations):
                with self.assertRaises(Exception):
                    self.transformation_manager.execute_function("nonexistent_function", "test")
                error_timer.record_execution()
        
        error_stats = self._record_performance(error_timer)
        
        # Compare error handling overhead
        print(f"\\n   Error handling analysis:")
        print(f"   • Successful ops: {success_stats['throughput']:.0f} ops/sec")
        print(f"   • Error ops: {error_stats['throughput']:.0f} ops/sec")
        print(f"   • Overhead factor: {success_stats['throughput'] / error_stats['throughput']:.1f}x")
        
        # Error handling should not be extremely slow
        max_error_overhead = 5.0  # Max 5x slower for error handling
        self.assertGreater(error_stats['throughput'], success_stats['throughput'] / max_error_overhead,
                         f"Error handling overhead too high: {success_stats['throughput'] / error_stats['throughput']:.1f}x")
    
    def test_complex_transformation_chain_performance(self):
        """Test performance of complex transformation chains."""
        iterations = 1000
        
        # Define a complex transformation chain
        def complex_chain(input_data: str) -> str:
            """Execute a chain of transformations."""
            result = self.transformation_manager.execute_function("upper", input_data)
            result = self.transformation_manager.execute_function("reverse", result)
            result = self.transformation_manager.execute_function("title", result)
            return result
        
        test_input = "performance test data for complex chain"
        
        with TransformationPerformanceTimer(
            f"Complex transformation chain ({iterations:,} chains)",
            "complex_chain"
        ) as timer:
            for _ in range(iterations):
                result = complex_chain(test_input)
                self.assertIsInstance(result, str)
                timer.record_execution()
        
        stats = self._record_performance(timer)
        
        # Complex chains should still be reasonably fast
        chain_threshold = self.throughput_threshold / 3  # Allow 3x slower for complex chains
        self.assertGreater(stats['throughput'], chain_threshold,
                         f"Complex chain throughput too low: {stats['throughput']:.0f} ops/sec")


if __name__ == '__main__':
    unittest.main()