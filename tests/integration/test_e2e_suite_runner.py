"""Comprehensive end-to-end test suite orchestrator.

This module provides a comprehensive test runner that executes all integration
tests in the correct order and provides detailed reporting.
"""

import json
import time
import unittest
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
import traceback

import pytest
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient

# Import all test modules
from test_proto_to_kafka_e2e import TestProtoToKafkaE2E
from test_schema_registry_integration import TestSchemaRegistryIntegration
from test_cross_platform_compatibility import TestCrossPlatformCompatibility


class E2ETestSuiteRunner:
    """Comprehensive end-to-end test suite runner."""
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        schema_registry_url: str = "http://localhost:8081",
        output_file: Optional[str] = None,
        verbose: bool = True
    ):
        """Initialize the test suite runner.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            schema_registry_url: Schema Registry URL
            output_file: Optional output file for detailed results
            verbose: Whether to print verbose output
        """
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.output_file = output_file
        self.verbose = verbose
        
        self.test_results = {
            "suite_start_time": None,
            "suite_end_time": None,
            "total_duration": 0,
            "environment_check": {},
            "test_modules": {},
            "overall_summary": {}
        }
    
    def check_environment(self) -> Dict[str, Any]:
        """Check if the test environment is ready."""
        if self.verbose:
            print("🔍 Checking test environment...")
        
        environment_status = {
            "kafka_available": False,
            "schema_registry_available": False,
            "dependencies_installed": False,
            "errors": [],
            "warnings": []
        }
        
        # Check Kafka connectivity
        try:
            admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})
            metadata = admin_client.list_topics(timeout=10)
            environment_status["kafka_available"] = True
            if self.verbose:
                print("✅ Kafka is available")
        except Exception as e:
            environment_status["errors"].append(f"Kafka not available: {e}")
            if self.verbose:
                print(f"❌ Kafka not available: {e}")
        
        # Check Schema Registry connectivity
        try:
            sr_client = SchemaRegistryClient({"url": self.schema_registry_url})
            subjects = sr_client.get_subjects()
            environment_status["schema_registry_available"] = True
            if self.verbose:
                print("✅ Schema Registry is available")
        except Exception as e:
            environment_status["errors"].append(f"Schema Registry not available: {e}")
            if self.verbose:
                print(f"❌ Schema Registry not available: {e}")
        
        # Check dependencies
        try:
            from testdatapy.schema.manager import SchemaManager
            from testdatapy.schema.registry_manager import SchemaRegistryManager
            from testdatapy.producers.protobuf_producer import ProtobufProducer
            environment_status["dependencies_installed"] = True
            if self.verbose:
                print("✅ All dependencies are installed")
        except ImportError as e:
            environment_status["errors"].append(f"Missing dependencies: {e}")
            if self.verbose:
                print(f"❌ Missing dependencies: {e}")
        
        self.test_results["environment_check"] = environment_status
        return environment_status
    
    def run_test_module(self, test_class, module_name: str) -> Dict[str, Any]:
        """Run a specific test module and collect results."""
        if self.verbose:
            print(f"\\n🧪 Running {module_name} tests...")
        
        module_results = {
            "name": module_name,
            "start_time": time.time(),
            "end_time": None,
            "duration": 0,
            "total_tests": 0,
            "passed_tests": 0,
            "failed_tests": 0,
            "skipped_tests": 0,
            "errors": [],
            "test_details": []
        }
        
        try:
            # Create test suite
            loader = unittest.TestLoader()
            suite = loader.loadTestsFromTestCase(test_class)
            
            # Count total tests
            module_results["total_tests"] = suite.countTestCases()
            
            # Run tests
            runner = unittest.TextTestRunner(
                verbosity=2 if self.verbose else 1,
                stream=sys.stdout if self.verbose else open('/dev/null', 'w')
            )
            
            result = runner.run(suite)
            
            # Collect results
            module_results["passed_tests"] = result.testsRun - len(result.failures) - len(result.errors) - len(result.skipped)
            module_results["failed_tests"] = len(result.failures) + len(result.errors)
            module_results["skipped_tests"] = len(result.skipped)
            
            # Collect detailed error information
            for test, error in result.failures + result.errors:
                module_results["errors"].append({
                    "test_name": str(test),
                    "error_type": "failure" if (test, error) in result.failures else "error",
                    "error_message": error,
                    "traceback": traceback.format_exc()
                })
            
            # Success status
            success = module_results["failed_tests"] == 0
            
            if self.verbose:
                if success:
                    print(f"✅ {module_name}: {module_results['passed_tests']}/{module_results['total_tests']} tests passed")
                else:
                    print(f"❌ {module_name}: {module_results['failed_tests']} tests failed")
            
        except Exception as e:
            module_results["errors"].append({
                "test_name": "module_setup",
                "error_type": "setup_error",
                "error_message": str(e),
                "traceback": traceback.format_exc()
            })
            if self.verbose:
                print(f"❌ {module_name}: Setup failed - {e}")
        
        module_results["end_time"] = time.time()
        module_results["duration"] = module_results["end_time"] - module_results["start_time"]
        
        return module_results
    
    def run_full_suite(self) -> Dict[str, Any]:
        """Run the complete end-to-end test suite."""
        self.test_results["suite_start_time"] = time.time()
        
        if self.verbose:
            print("🚀 Starting Comprehensive End-to-End Test Suite")
            print("=" * 60)
        
        # Step 1: Environment check
        env_check = self.check_environment()
        if env_check["errors"]:
            if self.verbose:
                print("\\n❌ Environment check failed. Aborting test suite.")
                for error in env_check["errors"]:
                    print(f"   - {error}")
            return self.test_results
        
        # Step 2: Run test modules in order
        test_modules = [
            (TestProtoToKafkaE2E, "Proto-to-Kafka E2E"),
            (TestSchemaRegistryIntegration, "Schema Registry Integration"),
            (TestCrossPlatformCompatibility, "Cross-Platform Compatibility")
        ]
        
        for test_class, module_name in test_modules:
            module_result = self.run_test_module(test_class, module_name)
            self.test_results["test_modules"][module_name] = module_result
        
        # Step 3: Generate overall summary
        self._generate_summary()
        
        self.test_results["suite_end_time"] = time.time()
        self.test_results["total_duration"] = (
            self.test_results["suite_end_time"] - self.test_results["suite_start_time"]
        )
        
        # Step 4: Print final results
        if self.verbose:
            self._print_final_report()
        
        # Step 5: Save detailed results if requested
        if self.output_file:
            self._save_results_to_file()
        
        return self.test_results
    
    def _generate_summary(self):
        """Generate overall test suite summary."""
        summary = {
            "total_test_modules": len(self.test_results["test_modules"]),
            "successful_modules": 0,
            "failed_modules": 0,
            "total_tests": 0,
            "total_passed": 0,
            "total_failed": 0,
            "total_skipped": 0,
            "overall_success_rate": 0.0
        }
        
        for module_name, module_result in self.test_results["test_modules"].items():
            summary["total_tests"] += module_result["total_tests"]
            summary["total_passed"] += module_result["passed_tests"]
            summary["total_failed"] += module_result["failed_tests"]
            summary["total_skipped"] += module_result["skipped_tests"]
            
            if module_result["failed_tests"] == 0:
                summary["successful_modules"] += 1
            else:
                summary["failed_modules"] += 1
        
        if summary["total_tests"] > 0:
            summary["overall_success_rate"] = (
                summary["total_passed"] / summary["total_tests"] * 100
            )
        
        self.test_results["overall_summary"] = summary
    
    def _print_final_report(self):
        """Print comprehensive final report."""
        print("\\n" + "=" * 60)
        print("🏁 FINAL TEST SUITE REPORT")
        print("=" * 60)
        
        summary = self.test_results["overall_summary"]
        
        print(f"📊 Overall Statistics:")
        print(f"   • Total Test Modules: {summary['total_test_modules']}")
        print(f"   • Successful Modules: {summary['successful_modules']}")
        print(f"   • Failed Modules: {summary['failed_modules']}")
        print(f"   • Total Tests: {summary['total_tests']}")
        print(f"   • Passed: {summary['total_passed']}")
        print(f"   • Failed: {summary['total_failed']}")
        print(f"   • Skipped: {summary['total_skipped']}")
        print(f"   • Success Rate: {summary['overall_success_rate']:.1f}%")
        print(f"   • Total Duration: {self.test_results['total_duration']:.2f} seconds")
        
        print(f"\\n📋 Module Details:")
        for module_name, module_result in self.test_results["test_modules"].items():
            status = "✅" if module_result["failed_tests"] == 0 else "❌"
            print(f"   {status} {module_name}:")
            print(f"      - Tests: {module_result['passed_tests']}/{module_result['total_tests']} passed")
            print(f"      - Duration: {module_result['duration']:.2f}s")
            
            if module_result["errors"]:
                print(f"      - Errors: {len(module_result['errors'])}")
        
        # Overall result
        if summary["failed_modules"] == 0:
            print(f"\\n🎉 ALL TESTS PASSED! The protobuf integration is working correctly.")
        else:
            print(f"\\n⚠️  {summary['failed_modules']} module(s) had failures. Please review the errors above.")
        
        print("=" * 60)
    
    def _save_results_to_file(self):
        """Save detailed results to JSON file."""
        try:
            output_path = Path(self.output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(self.test_results, f, indent=2, default=str)
            
            if self.verbose:
                print(f"\\n💾 Detailed results saved to: {output_path}")
        
        except Exception as e:
            if self.verbose:
                print(f"\\n⚠️  Failed to save results to file: {e}")


@pytest.mark.integration
class TestE2EFullSuite(unittest.TestCase):
    """Test class that runs the complete end-to-end suite."""
    
    def test_complete_e2e_suite(self):
        """Run the complete end-to-end test suite."""
        runner = E2ETestSuiteRunner(verbose=True)
        results = runner.run_full_suite()
        
        # Assert overall success
        summary = results["overall_summary"]
        self.assertEqual(summary["failed_modules"], 0, 
                        f"Test suite failed: {summary['failed_modules']} modules had failures")
        self.assertGreater(summary["overall_success_rate"], 95.0,
                          f"Success rate too low: {summary['overall_success_rate']:.1f}%")


def main():
    """Main entry point for running the test suite."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run comprehensive end-to-end test suite")
    parser.add_argument("--kafka-servers", default="localhost:9092",
                       help="Kafka bootstrap servers")
    parser.add_argument("--schema-registry", default="http://localhost:8081",
                       help="Schema Registry URL")
    parser.add_argument("--output", type=str,
                       help="Output file for detailed results")
    parser.add_argument("--quiet", action="store_true",
                       help="Suppress verbose output")
    
    args = parser.parse_args()
    
    runner = E2ETestSuiteRunner(
        bootstrap_servers=args.kafka_servers,
        schema_registry_url=args.schema_registry,
        output_file=args.output,
        verbose=not args.quiet
    )
    
    results = runner.run_full_suite()
    
    # Exit with appropriate code
    summary = results["overall_summary"]
    exit_code = 0 if summary["failed_modules"] == 0 else 1
    sys.exit(exit_code)


if __name__ == '__main__':
    main()