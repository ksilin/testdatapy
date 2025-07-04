"""Performance tests for vehicle scenarios and benchmarking validation."""
import pytest
import tempfile
import yaml
import time
from pathlib import Path

from testdatapy.performance.benchmark import VehicleBenchmarkSuite, BenchmarkMetrics, PerformanceMonitor


class TestVehiclePerformance:
    """Performance tests for vehicle scenarios."""
    
    @pytest.fixture
    def vehicle_config_small(self, tmp_path):
        """Create a small vehicle configuration for performance testing."""
        
        config = {
            "master_data": {
                "appointments": {
                    "source": "faker",
                    "count": 1000,  # Small for fast testing
                    "kafka_topic": "test_appointments",
                    "id_field": "jobid",
                    "schema": {
                        "jobid": {"type": "string", "format": "JOB_{seq:06d}"},
                        "full": {
                            "type": "object",
                            "properties": {
                                "Vehicle": {
                                    "type": "object",
                                    "properties": {
                                        "cLicenseNrCleaned": {
                                            "type": "string",
                                            "format": "M{random_digits:3}AB"
                                        }
                                    }
                                },
                                "Job": {
                                    "type": "object",
                                    "properties": {
                                        "dtStart": {"type": "timestamp_millis"}
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "transactional_data": {
                "carinlane_events": {
                    "kafka_topic": "test_events",
                    "rate_per_second": 100,
                    "max_messages": 500,  # Small for fast testing
                    "relationships": {
                        "appointment_plate": {
                            "references": "appointments.full.Vehicle.cLicenseNrCleaned",
                            "percentage": 25
                        }
                    },
                    "derived_fields": {
                        "event_id": {"type": "string", "format": "EVT_{seq:08d}"},
                        "sensor_id": {"type": "string", "initial_value": "SENSOR_01"},
                        "is_correlated": {"type": "random_boolean", "probability": 0.25}
                    }
                }
            }
        }
        
        config_file = tmp_path / "vehicle_test_config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config, f)
        
        return str(config_file)
    
    def test_performance_monitor(self):
        """Test real-time performance monitoring functionality."""
        
        monitor = PerformanceMonitor()
        
        # Start monitoring
        monitor.start_monitoring()
        
        # Simulate some work
        time.sleep(2.0)
        
        # Stop monitoring
        monitor.stop_monitoring()
        
        # Verify metrics were collected
        assert len(monitor.metrics_history) >= 1
        
        # Check metric structure
        metrics = monitor.metrics_history[0]
        assert "timestamp" in metrics
        assert "memory_mb" in metrics
        assert "cpu_percent" in metrics
        assert "thread_count" in metrics
        
        # Test aggregation functions
        peak_memory = monitor.get_peak_memory()
        avg_cpu = monitor.get_average_cpu()
        peak_cpu = monitor.get_peak_cpu()
        
        assert peak_memory >= 0
        assert avg_cpu >= 0
        assert peak_cpu >= 0
    
    def test_vehicle_benchmark_metrics_structure(self):
        """Test BenchmarkMetrics dataclass structure and defaults."""
        
        metrics = BenchmarkMetrics()
        
        # Verify all required fields exist
        assert hasattr(metrics, 'start_time')
        assert hasattr(metrics, 'end_time')
        assert hasattr(metrics, 'records_generated')
        assert hasattr(metrics, 'records_per_second')
        assert hasattr(metrics, 'correlation_ratio')
        assert hasattr(metrics, 'memory_peak_mb')
        assert hasattr(metrics, 'cpu_peak_percent')
        
        # Verify defaults
        assert metrics.records_generated == 0
        assert metrics.correlation_ratio == 0.0
        assert metrics.test_name == ""
        assert isinstance(metrics.timestamp, str)
    
    def test_vehicle_scenario_benchmark(self, vehicle_config_small):
        """Test complete vehicle scenario benchmarking."""
        
        benchmark_suite = VehicleBenchmarkSuite()
        
        # Run benchmark
        metrics = benchmark_suite.benchmark_vehicle_scenario(
            config_file=vehicle_config_small,
            test_name="Test_Vehicle_Small",
            enable_monitoring=True
        )
        
        # Verify metrics structure
        assert metrics.test_name == "Test_Vehicle_Small"
        assert metrics.records_generated > 0
        assert metrics.duration_seconds > 0
        assert metrics.records_per_second > 0
        assert 0.0 <= metrics.correlation_ratio <= 1.0
        assert metrics.memory_start_mb > 0
        assert metrics.memory_end_mb > 0
        
        # Verify vehicle-specific metrics
        assert metrics.correlation_count >= 0
        assert metrics.non_correlation_count >= 0
        assert metrics.correlation_count + metrics.non_correlation_count > 0
        
        # Verify reference pool metrics
        assert metrics.reference_pool_size > 0
    
    def test_vehicle_requirements_validation(self, vehicle_config_small):
        """Test vehicle performance requirements validation."""
        
        benchmark_suite = VehicleBenchmarkSuite()
        
        # Run benchmark
        metrics = benchmark_suite.benchmark_vehicle_scenario(
            config_file=vehicle_config_small,
            test_name="Test_Validation"
        )
        
        # Test validation function
        validation = benchmark_suite.validate_vehicle_requirements(metrics)
        
        # Verify validation structure
        assert "throughput_requirement" in validation
        assert "correlation_accuracy" in validation
        assert "memory_efficiency" in validation
        assert "processing_speed" in validation
        assert "overall_pass" in validation
        
        # All validation results should be boolean
        for key, value in validation.items():
            assert isinstance(value, bool)
    
    def test_correlation_accuracy_measurement(self, vehicle_config_small):
        """Test that correlation ratio is measured accurately."""
        
        benchmark_suite = VehicleBenchmarkSuite()
        
        metrics = benchmark_suite.benchmark_vehicle_scenario(
            config_file=vehicle_config_small,
            test_name="Test_Correlation"
        )
        
        # Verify correlation calculations
        total_events = metrics.correlation_count + metrics.non_correlation_count
        expected_ratio = metrics.correlation_count / total_events if total_events > 0 else 0
        
        assert abs(metrics.correlation_ratio - expected_ratio) < 0.001
        
        # Vehicle requirement: approximately 25% correlation
        assert 0.15 <= metrics.correlation_ratio <= 0.35  # Allow some variance for small datasets
    
    def test_performance_report_generation(self, vehicle_config_small, tmp_path):
        """Test performance report generation and structure."""
        
        benchmark_suite = VehicleBenchmarkSuite()
        
        # Run multiple benchmarks
        benchmark_suite.benchmark_vehicle_scenario(
            config_file=vehicle_config_small,
            test_name="Test_Report_1"
        )
        benchmark_suite.benchmark_vehicle_scenario(
            config_file=vehicle_config_small,
            test_name="Test_Report_2"
        )
        
        # Generate report
        report_file = tmp_path / "test_report.json"
        report = benchmark_suite.generate_performance_report(str(report_file))
        
        # Verify report structure
        assert "summary" in report
        assert "vehicle_requirements_validation" in report
        assert "detailed_results" in report
        assert "recommendations" in report
        
        # Verify summary structure
        summary = report["summary"]
        assert "total_benchmarks_run" in summary
        assert "latest_benchmark" in summary
        assert "performance_statistics" in summary
        
        # Verify detailed results
        assert len(report["detailed_results"]) == 2
        
        # Verify recommendations
        assert isinstance(report["recommendations"], list)
        
        # Verify file was created
        assert report_file.exists()
    
    @pytest.mark.slow
    def test_scaling_performance_benchmark(self, vehicle_config_small):
        """Test scaling performance across different data sizes."""
        
        benchmark_suite = VehicleBenchmarkSuite()
        
        # Run scaling benchmark with small scale factors
        scaling_results = benchmark_suite.benchmark_scaling_performance(
            config_file=vehicle_config_small,
            scale_factors=[100, 500, 1000]  # Small scales for test speed
        )
        
        # Verify results
        assert len(scaling_results) == 3
        
        # Verify increasing data sizes
        for i in range(len(scaling_results) - 1):
            current = scaling_results[i]
            next_result = scaling_results[i + 1]
            assert next_result.records_generated >= current.records_generated
        
        # Verify each result is valid
        for result in scaling_results:
            assert result.records_generated > 0
            assert result.duration_seconds > 0
            assert result.records_per_second > 0
    
    def test_throughput_measurement_accuracy(self, vehicle_config_small):
        """Test that throughput measurements are accurate."""
        
        benchmark_suite = VehicleBenchmarkSuite()
        
        metrics = benchmark_suite.benchmark_vehicle_scenario(
            config_file=vehicle_config_small,
            test_name="Test_Throughput"
        )
        
        # Calculate expected throughput
        expected_rps = metrics.records_generated / metrics.duration_seconds
        
        # Verify accuracy (within 1% tolerance)
        assert abs(metrics.records_per_second - expected_rps) < (expected_rps * 0.01)
        
        # Verify reasonable throughput for small dataset
        assert metrics.records_per_second > 10  # At least 10 records/second
    
    def test_memory_tracking_accuracy(self, vehicle_config_small):
        """Test memory usage tracking accuracy."""
        
        benchmark_suite = VehicleBenchmarkSuite()
        
        metrics = benchmark_suite.benchmark_vehicle_scenario(
            config_file=vehicle_config_small,
            test_name="Test_Memory",
            enable_monitoring=True
        )
        
        # Verify memory measurements
        assert metrics.memory_start_mb > 0
        assert metrics.memory_end_mb > 0
        assert metrics.memory_peak_mb >= metrics.memory_start_mb
        assert metrics.memory_peak_mb >= metrics.memory_end_mb
        
        # Memory growth should be reasonable
        assert metrics.memory_growth_mb >= 0  # Should not shrink
        assert metrics.memory_growth_mb < 1000  # Should not grow excessively for small test
    
    def test_vehicle_requirements_thresholds(self):
        """Test vehicle requirements validation thresholds."""
        
        benchmark_suite = VehicleBenchmarkSuite()
        
        # Test metrics that meet requirements
        good_metrics = BenchmarkMetrics(
            records_per_second=1500,  # Above 1000 requirement
            correlation_ratio=0.24,   # Within ±5% of 0.25
            memory_growth_mb=100,     # Below 500MB limit
            duration_seconds=60       # Below 120s limit
        )
        
        validation = benchmark_suite.validate_vehicle_requirements(good_metrics)
        assert validation["overall_pass"] is True
        assert validation["throughput_requirement"] is True
        assert validation["correlation_accuracy"] is True
        assert validation["memory_efficiency"] is True
        assert validation["processing_speed"] is True
        
        # Test metrics that fail requirements
        bad_metrics = BenchmarkMetrics(
            records_per_second=800,   # Below 1000 requirement
            correlation_ratio=0.15,   # Outside ±5% of 0.25
            memory_growth_mb=600,     # Above 500MB limit
            duration_seconds=150      # Above 120s limit
        )
        
        validation = benchmark_suite.validate_vehicle_requirements(bad_metrics)
        assert validation["overall_pass"] is False
        assert validation["throughput_requirement"] is False
        assert validation["correlation_accuracy"] is False
        assert validation["memory_efficiency"] is False
        assert validation["processing_speed"] is False
    
    def test_recommendations_generation(self):
        """Test performance recommendations generation."""
        
        benchmark_suite = VehicleBenchmarkSuite()
        
        # Test metrics that need improvement
        poor_metrics = BenchmarkMetrics(
            records_per_second=500,    # Low throughput
            correlation_ratio=0.35,    # Poor correlation
            memory_growth_mb=700,      # High memory usage
            cpu_peak_percent=90        # High CPU usage
        )
        
        recommendations = benchmark_suite._generate_recommendations(poor_metrics)
        
        assert len(recommendations) > 0
        assert any("throughput" in rec.lower() for rec in recommendations)
        assert any("correlation" in rec.lower() for rec in recommendations)
        assert any("memory" in rec.lower() for rec in recommendations)
        assert any("cpu" in rec.lower() for rec in recommendations)
        
        # Test metrics that are optimal
        good_metrics = BenchmarkMetrics(
            records_per_second=2000,
            correlation_ratio=0.25,
            memory_growth_mb=100,
            cpu_peak_percent=50
        )
        
        recommendations = benchmark_suite._generate_recommendations(good_metrics)
        assert len(recommendations) == 1
        assert "optimized" in recommendations[0].lower()


@pytest.mark.integration
class TestVehiclePerformanceIntegration:
    """Integration tests for vehicle performance benchmarking."""
    
    def test_full_vehicle_benchmark_suite(self, tmp_path):
        """Test the complete vehicle benchmark suite functionality."""
        
        # Create vehicle configuration
        config = {
            "master_data": {
                "appointments": {
                    "source": "faker",
                    "count": 2000,
                    "kafka_topic": "integration_appointments",
                    "id_field": "jobid",
                    "schema": {
                        "jobid": {"type": "string", "format": "JOB_{seq:06d}"},
                        "full": {
                            "type": "object",
                            "properties": {
                                "Vehicle": {
                                    "type": "object",
                                    "properties": {
                                        "cLicenseNrCleaned": {
                                            "type": "template",
                                            "template": "{area}{digits}{suffix}",
                                            "fields": {
                                                "area": {"type": "choice", "choices": ["M", "B"]},
                                                "digits": {"type": "string", "format": "{random_digits:3}"},
                                                "suffix": {"type": "choice", "choices": ["AB", "CD"]}
                                            }
                                        }
                                    }
                                },
                                "Job": {
                                    "type": "object",
                                    "properties": {
                                        "dtStart": {"type": "timestamp_millis"}
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "transactional_data": {
                "carinlane_events": {
                    "kafka_topic": "integration_events",
                    "rate_per_second": 500,
                    "max_messages": 1000,
                    "relationships": {
                        "appointment_plate": {
                            "references": "appointments.full.Vehicle.cLicenseNrCleaned",
                            "percentage": 25
                        }
                    },
                    "derived_fields": {
                        "event_id": {"type": "string", "format": "EVT_{seq:08d}"},
                        "sensor_id": {
                            "type": "template",
                            "template": "{site}_{type}_{lane}",
                            "fields": {
                                "site": {"type": "choice", "choices": ["MUC", "BER"]},
                                "type": {"type": "choice", "choices": ["ENTRY", "EXIT"]},
                                "lane": {"type": "string", "format": "L{random_digits:2}"}
                            }
                        },
                        "event_timestamp": {
                            "type": "timestamp_millis",
                            "relative_to_reference": "appointments.full.Job.dtStart",
                            "offset_minutes_min": -30,
                            "offset_minutes_max": 30,
                            "fallback": "now"
                        },
                        "is_correlated": {"type": "random_boolean", "probability": 0.25}
                    }
                }
            }
        }
        
        config_file = tmp_path / "integration_config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config, f)
        
        # Run benchmark suite
        benchmark_suite = VehicleBenchmarkSuite()
        
        metrics = benchmark_suite.benchmark_vehicle_scenario(
            config_file=str(config_file),
            test_name="Integration_Test",
            enable_monitoring=True
        )
        
        # Verify comprehensive results
        assert metrics.records_generated == 3000  # 2000 appointments + 1000 events
        assert metrics.duration_seconds > 0
        assert metrics.records_per_second > 0
        assert 0.2 <= metrics.correlation_ratio <= 0.3  # Vehicle 25% ±5%
        
        # Verify vehicle requirements
        validation = benchmark_suite.validate_vehicle_requirements(metrics)
        
        # Generate report
        report_file = tmp_path / "integration_report.json"
        report = benchmark_suite.generate_performance_report(str(report_file))
        
        assert report_file.exists()
        assert report["summary"]["total_benchmarks_run"] == 1
        assert len(report["recommendations"]) > 0