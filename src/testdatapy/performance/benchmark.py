"""Performance benchmarking system for vehicle scenarios and large-scale data generation."""
import time
import psutil
import threading
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
from pathlib import Path

from testdatapy.generators.reference_pool import ReferencePool
from testdatapy.generators.correlated_generator import CorrelatedDataGenerator
from testdatapy.generators.master_data_generator import MasterDataGenerator
from testdatapy.config.correlation_config import CorrelationConfig


@dataclass
class BenchmarkMetrics:
    """Metrics collected during performance benchmarking."""
    
    # Timing metrics
    start_time: float = 0.0
    end_time: float = 0.0
    duration_seconds: float = 0.0
    
    # Throughput metrics
    records_generated: int = 0
    records_per_second: float = 0.0
    
    # Memory metrics
    memory_start_mb: float = 0.0
    memory_peak_mb: float = 0.0
    memory_end_mb: float = 0.0
    memory_growth_mb: float = 0.0
    
    # CPU metrics
    cpu_start_percent: float = 0.0
    cpu_peak_percent: float = 0.0
    cpu_average_percent: float = 0.0
    
    # Vehicle-specific metrics
    correlation_ratio: float = 0.0
    correlation_count: int = 0
    non_correlation_count: int = 0
    
    # Reference pool metrics
    reference_pool_size: int = 0
    reference_pool_memory_mb: float = 0.0
    cache_hit_ratio: float = 0.0
    
    # Additional metadata
    entity_type: str = ""
    configuration_file: str = ""
    test_name: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


class PerformanceMonitor:
    """Real-time performance monitoring during data generation."""
    
    def __init__(self):
        self.monitoring = False
        self.monitor_thread = None
        self.metrics_history: List[Dict[str, Any]] = []
        self.sample_interval = 1.0  # seconds
        
    def start_monitoring(self) -> None:
        """Start real-time performance monitoring."""
        if self.monitoring:
            return
            
        self.monitoring = True
        self.metrics_history.clear()
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
    
    def stop_monitoring(self) -> None:
        """Stop performance monitoring."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2.0)
    
    def _monitor_loop(self) -> None:
        """Main monitoring loop collecting system metrics."""
        process = psutil.Process()
        
        while self.monitoring:
            try:
                # Collect current metrics
                memory_info = process.memory_info()
                cpu_percent = process.cpu_percent()
                
                metrics = {
                    "timestamp": time.time(),
                    "memory_mb": memory_info.rss / 1024 / 1024,
                    "memory_vms_mb": memory_info.vms / 1024 / 1024,
                    "cpu_percent": cpu_percent,
                    "thread_count": process.num_threads(),
                }
                
                self.metrics_history.append(metrics)
                time.sleep(self.sample_interval)
                
            except Exception:
                # Continue monitoring even if individual sample fails
                time.sleep(self.sample_interval)
    
    def get_peak_memory(self) -> float:
        """Get peak memory usage during monitoring period."""
        if not self.metrics_history:
            return 0.0
        return max(m["memory_mb"] for m in self.metrics_history)
    
    def get_average_cpu(self) -> float:
        """Get average CPU usage during monitoring period."""
        if not self.metrics_history:
            return 0.0
        cpu_values = [m["cpu_percent"] for m in self.metrics_history if m["cpu_percent"] > 0]
        return sum(cpu_values) / len(cpu_values) if cpu_values else 0.0
    
    def get_peak_cpu(self) -> float:
        """Get peak CPU usage during monitoring period."""
        if not self.metrics_history:
            return 0.0
        return max(m["cpu_percent"] for m in self.metrics_history)


class VehicleBenchmarkSuite:
    """Comprehensive benchmarking suite for vehicle scenarios."""
    
    def __init__(self):
        self.monitor = PerformanceMonitor()
        self.results: List[BenchmarkMetrics] = []
        
    def benchmark_vehicle_scenario(
        self, 
        config_file: str,
        test_name: str = "Vehicle_Full_Scenario",
        enable_monitoring: bool = True
    ) -> BenchmarkMetrics:
        """Benchmark the complete vehicle scenario."""
        
        metrics = BenchmarkMetrics(
            test_name=test_name,
            configuration_file=config_file
        )
        
        try:
            # Load configuration
            correlation_config = CorrelationConfig.from_yaml_file(config_file)
            
            # Initialize components
            ref_pool = ReferencePool()
            ref_pool.enable_stats()
            
            if enable_monitoring:
                self.monitor.start_monitoring()
            
            # Capture initial state
            process = psutil.Process()
            metrics.start_time = time.time()
            metrics.memory_start_mb = process.memory_info().rss / 1024 / 1024
            metrics.cpu_start_percent = process.cpu_percent()
            
            # Phase 1: Benchmark master data loading
            master_metrics = self._benchmark_master_data_loading(
                correlation_config, ref_pool
            )
            
            # Phase 2: Benchmark transactional data generation
            transaction_metrics = self._benchmark_transactional_data_generation(
                correlation_config, ref_pool
            )
            
            # Capture final state
            metrics.end_time = time.time()
            metrics.duration_seconds = metrics.end_time - metrics.start_time
            metrics.memory_end_mb = process.memory_info().rss / 1024 / 1024
            metrics.memory_growth_mb = metrics.memory_end_mb - metrics.memory_start_mb
            
            if enable_monitoring:
                self.monitor.stop_monitoring()
                metrics.memory_peak_mb = self.monitor.get_peak_memory()
                metrics.cpu_average_percent = self.monitor.get_average_cpu()
                metrics.cpu_peak_percent = self.monitor.get_peak_cpu()
            
            # Aggregate metrics
            metrics.records_generated = (
                master_metrics.get("total_records", 0) +
                transaction_metrics.get("total_records", 0)
            )
            metrics.records_per_second = (
                metrics.records_generated / metrics.duration_seconds
                if metrics.duration_seconds > 0 else 0
            )
            
            # Vehicle-specific metrics
            metrics.correlation_ratio = transaction_metrics.get("correlation_ratio", 0.0)
            metrics.correlation_count = transaction_metrics.get("correlation_count", 0)
            metrics.non_correlation_count = transaction_metrics.get("non_correlation_count", 0)
            
            # Reference pool metrics
            ref_pool_usage = ref_pool.get_memory_usage()
            metrics.reference_pool_size = ref_pool_usage.get("total_references", 0)
            metrics.reference_pool_memory_mb = ref_pool_usage.get("memory_estimate_mb", 0)
            
            self.results.append(metrics)
            return metrics
            
        except Exception as e:
            if enable_monitoring:
                self.monitor.stop_monitoring()
            raise RuntimeError(f"Benchmark failed: {e}") from e
    
    def _benchmark_master_data_loading(
        self, 
        correlation_config: CorrelationConfig, 
        ref_pool: ReferencePool
    ) -> Dict[str, Any]:
        """Benchmark master data loading performance."""
        
        master_gen = MasterDataGenerator(
            config=correlation_config,
            reference_pool=ref_pool
        )
        
        start_time = time.time()
        master_gen.load_all()
        duration = time.time() - start_time
        
        loading_stats = master_gen.get_loading_stats()
        total_records = loading_stats.get("_total", {}).get("total_records", 0)
        
        return {
            "duration_seconds": duration,
            "total_records": total_records,
            "records_per_second": total_records / duration if duration > 0 else 0,
            "loading_stats": loading_stats
        }
    
    def _benchmark_transactional_data_generation(
        self, 
        correlation_config: CorrelationConfig, 
        ref_pool: ReferencePool
    ) -> Dict[str, Any]:
        """Benchmark transactional data generation performance."""
        
        results = {
            "total_records": 0,
            "correlation_count": 0,
            "non_correlation_count": 0,
            "correlation_ratio": 0.0
        }
        
        # Benchmark all transactional entities
        for entity_type in correlation_config.config.get("transactional_data", {}):
            entity_config = correlation_config.get_transaction_config(entity_type)
            max_messages = entity_config.get("max_messages", 1000)
            
            generator = CorrelatedDataGenerator(
                entity_type=entity_type,
                config=correlation_config,
                reference_pool=ref_pool,
                max_messages=max_messages
            )
            
            # Generate and count correlations
            for record in generator.generate():
                results["total_records"] += 1
                
                if record.get("appointment_plate") is not None:
                    results["correlation_count"] += 1
                else:
                    results["non_correlation_count"] += 1
        
        # Calculate correlation ratio
        if results["total_records"] > 0:
            results["correlation_ratio"] = (
                results["correlation_count"] / results["total_records"]
            )
        
        return results
    
    def benchmark_scaling_performance(
        self, 
        config_file: str,
        scale_factors: List[int] = [1000, 10000, 50000, 100000]
    ) -> List[BenchmarkMetrics]:
        """Benchmark performance across different data scales."""
        
        scaling_results = []
        
        for scale_factor in scale_factors:
            # Create scaled configuration
            scaled_config = self._create_scaled_config(config_file, scale_factor)
            
            # Run benchmark
            metrics = self.benchmark_scenario(
                config_file=scaled_config,
                test_name=f"Scale_{scale_factor}",
                enable_monitoring=True
            )
            
            scaling_results.append(metrics)
            
            # Clean up temporary config
            Path(scaled_config).unlink(missing_ok=True)
        
        return scaling_results
    
    def _create_scaled_config(self, base_config_file: str, scale_factor: int) -> str:
        """Create a scaled version of the configuration for testing."""
        
        with open(base_config_file, 'r') as f:
            import yaml
            config = yaml.safe_load(f)
        
        # Scale master data counts
        for entity_config in config.get("master_data", {}).values():
            if "count" in entity_config:
                entity_config["count"] = scale_factor
        
        # Scale transactional data counts
        for entity_config in config.get("transactional_data", {}).values():
            if "max_messages" in entity_config:
                entity_config["max_messages"] = scale_factor * 2 
        
        # Write scaled config to temporary file
        temp_config_file = f"/tmp/scaled_{scale_factor}.yaml"
        with open(temp_config_file, 'w') as f:
            yaml.dump(config, f)
        
        return temp_config_file
    
    def validate_vehicle_requirements(self, metrics: BenchmarkMetrics) -> Dict[str, bool]:
        """Validate vehicle performance requirements against benchmark results."""
        
        validation_results = {
            "throughput_requirement": metrics.records_per_second >= 1000,
            "correlation_accuracy": abs(metrics.correlation_ratio - 0.25) <= 0.05,  # ±5% tolerance
            "memory_efficiency": metrics.memory_growth_mb <= 500,  # Max 500MB growth
            "processing_speed": metrics.duration_seconds <= 120,  # Max 2 minutes for standard test
        }
        
        # Overall validation
        validation_results["overall_pass"] = all(validation_results.values())
        
        return validation_results
    
    def generate_performance_report(
        self, 
        output_file: Optional[str] = None
    ) -> Dict[str, Any]:
        """Generate comprehensive performance report."""
        
        if not self.results:
            return {"error": "No benchmark results available"}
        
        # Calculate summary statistics
        latest_result = self.results[-1]
        all_throughputs = [r.records_per_second for r in self.results]
        all_correlations = [r.correlation_ratio for r in self.results]
        
        report = {
            "summary": {
                "total_benchmarks_run": len(self.results),
                "latest_benchmark": {
                    "test_name": latest_result.test_name,
                    "records_generated": latest_result.records_generated,
                    "throughput_rps": latest_result.records_per_second,
                    "duration_seconds": latest_result.duration_seconds,
                    "correlation_ratio": latest_result.correlation_ratio,
                    "memory_usage_mb": latest_result.memory_peak_mb,
                    "cpu_usage_percent": latest_result.cpu_peak_percent,
                },
                "performance_statistics": {
                    "avg_throughput_rps": sum(all_throughputs) / len(all_throughputs),
                    "max_throughput_rps": max(all_throughputs),
                    "min_throughput_rps": min(all_throughputs),
                    "avg_correlation_ratio": sum(all_correlations) / len(all_correlations),
                    "correlation_std_dev": self._calculate_std_dev(all_correlations),
                }
            },
            "requirements_validation": self.validate_requirements(latest_result),
            "detailed_results": [
                {
                    "test_name": r.test_name,
                    "timestamp": r.timestamp,
                    "records_generated": r.records_generated,
                    "throughput_rps": r.records_per_second,
                    "duration_seconds": r.duration_seconds,
                    "correlation_ratio": r.correlation_ratio,
                    "memory_peak_mb": r.memory_peak_mb,
                    "cpu_peak_percent": r.cpu_peak_percent,
                    "reference_pool_size": r.reference_pool_size,
                }
                for r in self.results
            ],
            "recommendations": self._generate_recommendations(latest_result)
        }
        
        if output_file:
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2)
        
        return report
    
    def _calculate_std_dev(self, values: List[float]) -> float:
        """Calculate standard deviation of values."""
        if len(values) < 2:
            return 0.0
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
        return variance ** 0.5
    
    def _generate_recommendations(self, metrics: BenchmarkMetrics) -> List[str]:
        """Generate performance optimization recommendations."""
        
        recommendations = []
        
        if metrics.records_per_second < 1000:
            recommendations.append(
                f"Throughput ({metrics.records_per_second:.0f} rps) below requirement (1000 rps). "
                "Consider optimizing reference pool operations or reducing field complexity."
            )
        
        if abs(metrics.correlation_ratio - 0.25) > 0.05:
            recommendations.append(
                f"Correlation ratio ({metrics.correlation_ratio:.3f}) deviates from requirement (0.25). "
                "Check percentage-based correlation implementation."
            )
        
        if metrics.memory_growth_mb > 500:
            recommendations.append(
                f"Memory growth ({metrics.memory_growth_mb:.1f} MB) exceeds recommended limit. "
                "Consider implementing memory cleanup or reducing cache sizes."
            )
        
        if metrics.cpu_peak_percent > 80:
            recommendations.append(
                f"Peak CPU usage ({metrics.cpu_peak_percent:.1f}%) is high. "
                "Consider optimizing computational operations or reducing rate limits."
            )
        
        if not recommendations:
            recommendations.append("Performance meets all requirements. System is optimized.")
        
        return recommendations


def run_vehicle_benchmark_suite(config_file: str, output_dir: str = "./benchmark_results") -> None:
    """Run complete vehicle benchmark suite and generate reports."""
    
    Path(output_dir).mkdir(exist_ok=True)
    
    benchmark_suite = VehicleBenchmarkSuite()
    
    print("🚀 Starting Performance Benchmark Suite...")
    
    # Run main scenario benchmark
    print("📊 Running scenario benchmark...")
    main_metrics = benchmark_suite.benchmark_scenario(
        config_file=config_file,
        test_name="Main_Scenario"
    )
    
    # Run scaling benchmarks
    print("📈 Running scaling performance benchmarks...")
    scaling_metrics = benchmark_suite.benchmark_scaling_performance(
        config_file=config_file,
        scale_factors=[1000, 5000, 10000]  # Reduced for practical testing
    )
    
    # Generate comprehensive report
    print("📋 Generating performance report...")
    report = benchmark_suite.generate_performance_report(
        output_file=f"{output_dir}/performance_report.json"
    )
    
    # Print summary
    print("\n✅ Benchmark Suite Complete!")
    print(f"📊 Results saved to: {output_dir}/")
    print(f"🚀 Latest Throughput: {main_metrics.records_per_second:.0f} records/sec")
    print(f"🎯 Correlation Ratio: {main_metrics.correlation_ratio:.3f} (target: 0.25)")
    print(f"💾 Memory Usage: {main_metrics.memory_peak_mb:.1f} MB")
    print(f"⏱️  Total Duration: {main_metrics.duration_seconds:.1f} seconds")
    
    # Validation summary
    validation = benchmark_suite.validate_requirements(main_metrics)
    if validation["overall_pass"]:
        print("✅ All requirements PASSED!")
    else:
        print("❌ Some requirements FAILED. Check detailed report.")


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
        run_benchmark_suite(config_file)
    else:
        print("Usage: python benchmark.py <config.yaml>")