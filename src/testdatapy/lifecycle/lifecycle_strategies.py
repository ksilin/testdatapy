"""Lifecycle strategies for different use cases and environments.

This module provides pre-defined strategies for common lifecycle management
scenarios like testing, development, and production environments.
"""

import time
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from .lifecycle_config import LifecycleConfig, TopicConfig, LifecycleAction, CleanupPolicy
from ..logging_config import get_schema_logger


@dataclass
class LifecycleResult:
    """Result of a lifecycle operation."""
    success: bool
    action: str
    topic_name: str
    message: str
    details: Dict[str, Any]
    duration: float
    error: Optional[Exception] = None


class LifecycleStrategy(ABC):
    """Abstract base class for lifecycle strategies."""
    
    def __init__(self, name: str, description: str):
        """
        Initialize the strategy.
        
        Args:
            name: Strategy name
            description: Strategy description
        """
        self.name = name
        self.description = description
        self._logger = get_schema_logger(__name__)
    
    @abstractmethod
    def create_config(self, topic_names: List[str], **kwargs) -> LifecycleConfig:
        """
        Create lifecycle configuration for this strategy.
        
        Args:
            topic_names: List of topic names to configure
            **kwargs: Strategy-specific parameters
            
        Returns:
            Lifecycle configuration
        """
        pass
    
    @abstractmethod
    def validate_environment(self, config: LifecycleConfig) -> List[str]:
        """
        Validate environment for this strategy.
        
        Args:
            config: Lifecycle configuration
            
        Returns:
            List of validation issues (empty if valid)
        """
        pass
    
    def get_strategy_info(self) -> Dict[str, Any]:
        """Get strategy information."""
        return {
            "name": self.name,
            "description": self.description,
            "type": self.__class__.__name__
        }


class TestDataStrategy(LifecycleStrategy):
    """Strategy for test data generation and testing scenarios."""
    
    def __init__(self):
        super().__init__(
            name="test_data",
            description="Optimized for test data generation with automatic cleanup"
        )
    
    def create_config(self, topic_names: List[str], **kwargs) -> LifecycleConfig:
        """
        Create test data configuration.
        
        Args:
            topic_names: List of topic names
            **kwargs: Additional parameters:
                - cleanup_after: bool (default True) - Delete topics after use
                - clear_before: bool (default True) - Clear data before use
                - partitions: int (default 1) - Number of partitions
                - temp_prefix: str (default "testdata-") - Prefix for temporary topics
                
        Returns:
            Test data lifecycle configuration
        """
        cleanup_after = kwargs.get('cleanup_after', True)
        clear_before = kwargs.get('clear_before', True)
        partitions = kwargs.get('partitions', 1)
        temp_prefix = kwargs.get('temp_prefix', 'testdata-')
        
        topics = []
        for topic_name in topic_names:
            # Add prefix if not already present
            if not topic_name.startswith(temp_prefix):
                full_topic_name = f"{temp_prefix}{topic_name}"
            else:
                full_topic_name = topic_name
            
            topics.append(TopicConfig(
                name=full_topic_name,
                partitions=partitions,
                replication_factor=1,
                config={
                    "cleanup.policy": "delete",
                    "retention.ms": "3600000",  # 1 hour retention
                    "segment.ms": "60000"       # 1 minute segments
                },
                tags=["test_data", "temporary"],
                pre_action=LifecycleAction.RECREATE if clear_before else LifecycleAction.ENSURE_EXISTS,
                post_action=LifecycleAction.DELETE if cleanup_after else LifecycleAction.SKIP,
                create_if_missing=True,
                fail_if_exists=False,
                clear_before_use=clear_before
            ))
        
        return LifecycleConfig(
            topics=topics,
            default_partitions=partitions,
            default_replication_factor=1,
            pre_generation_policy=LifecycleAction.RECREATE if clear_before else LifecycleAction.ENSURE_EXISTS,
            post_generation_policy=CleanupPolicy.DELETE_TOPICS if cleanup_after else CleanupPolicy.NONE,
            confirm_destructive_actions=False,
            dry_run_mode=False,
            max_topics_to_delete=50,  # Allow many test topics
            environment="test",
            allowed_topic_patterns=[f"{temp_prefix}*", "test-*", "*-test", "temp-*"],
            protected_topics=[],
            operation_timeout=15,
            max_retries=2
        )
    
    def validate_environment(self, config: LifecycleConfig) -> List[str]:
        """Validate test environment."""
        issues = []
        
        # Check environment
        if config.environment not in ["test", "development", "local"]:
            issues.append(f"Test strategy not recommended for environment: {config.environment}")
        
        # Check topic naming
        for topic in config.topics:
            if not any(tag in ["test_data", "temporary", "test"] for tag in topic.tags):
                issues.append(f"Topic '{topic.name}' should be tagged as test data")
        
        # Check retention settings
        for topic in config.topics:
            retention_ms = topic.config.get("retention.ms")
            if retention_ms and int(retention_ms) > 86400000:  # > 24 hours
                issues.append(f"Topic '{topic.name}' has long retention for test data: {retention_ms}ms")
        
        return issues


class DevelopmentStrategy(LifecycleStrategy):
    """Strategy for development environments with moderate persistence."""
    
    def __init__(self):
        super().__init__(
            name="development",
            description="Balanced approach for development with data persistence"
        )
    
    def create_config(self, topic_names: List[str], **kwargs) -> LifecycleConfig:
        """
        Create development configuration.
        
        Args:
            topic_names: List of topic names
            **kwargs: Additional parameters:
                - partitions: int (default 2) - Number of partitions
                - retention_hours: int (default 24) - Retention in hours
                - dev_prefix: str (default "dev-") - Prefix for dev topics
                - clear_on_restart: bool (default False) - Clear data on restart
                
        Returns:
            Development lifecycle configuration
        """
        partitions = kwargs.get('partitions', 2)
        retention_hours = kwargs.get('retention_hours', 24)
        dev_prefix = kwargs.get('dev_prefix', 'dev-')
        clear_on_restart = kwargs.get('clear_on_restart', False)
        
        topics = []
        for topic_name in topic_names:
            # Add prefix if not already present
            if not topic_name.startswith(dev_prefix):
                full_topic_name = f"{dev_prefix}{topic_name}"
            else:
                full_topic_name = topic_name
            
            topics.append(TopicConfig(
                name=full_topic_name,
                partitions=partitions,
                replication_factor=1,
                config={
                    "cleanup.policy": "delete",
                    "retention.ms": str(retention_hours * 3600 * 1000),
                    "segment.ms": "1800000",  # 30 minute segments
                    "compression.type": "snappy"
                },
                tags=["development", "persistent"],
                pre_action=LifecycleAction.CLEAR_DATA if clear_on_restart else LifecycleAction.ENSURE_EXISTS,
                post_action=LifecycleAction.SKIP,
                create_if_missing=True,
                fail_if_exists=False,
                clear_before_use=clear_on_restart
            ))
        
        return LifecycleConfig(
            topics=topics,
            default_partitions=partitions,
            default_replication_factor=1,
            pre_generation_policy=LifecycleAction.ENSURE_EXISTS,
            post_generation_policy=CleanupPolicy.NONE,
            confirm_destructive_actions=True,
            dry_run_mode=False,
            max_topics_to_delete=10,
            environment="development",
            allowed_topic_patterns=[f"{dev_prefix}*", "dev-*", "staging-*"],
            protected_topics=[],
            operation_timeout=30
        )
    
    def validate_environment(self, config: LifecycleConfig) -> List[str]:
        """Validate development environment."""
        issues = []
        
        # Check environment
        if config.environment not in ["development", "dev", "staging"]:
            issues.append(f"Development strategy not suitable for environment: {config.environment}")
        
        # Check for production-like settings
        for topic in config.topics:
            if topic.replication_factor > 2:
                issues.append(f"Topic '{topic.name}' has high replication factor for development")
        
        return issues


class ProductionStrategy(LifecycleStrategy):
    """Strategy for production environments with safety and durability."""
    
    def __init__(self):
        super().__init__(
            name="production",
            description="Production-safe strategy with high durability and safety checks"
        )
    
    def create_config(self, topic_names: List[str], **kwargs) -> LifecycleConfig:
        """
        Create production configuration.
        
        Args:
            topic_names: List of topic names
            **kwargs: Additional parameters:
                - partitions: int (default 6) - Number of partitions
                - replication_factor: int (default 3) - Replication factor
                - retention_days: int (default 7) - Retention in days
                - min_insync_replicas: int (default 2) - Min in-sync replicas
                
        Returns:
            Production lifecycle configuration
        """
        partitions = kwargs.get('partitions', 6)
        replication_factor = kwargs.get('replication_factor', 3)
        retention_days = kwargs.get('retention_days', 7)
        min_insync_replicas = kwargs.get('min_insync_replicas', 2)
        
        topics = []
        for topic_name in topic_names:
            topics.append(TopicConfig(
                name=topic_name,
                partitions=partitions,
                replication_factor=replication_factor,
                config={
                    "cleanup.policy": "delete",
                    "retention.ms": str(retention_days * 24 * 3600 * 1000),
                    "segment.ms": "604800000",  # 7 day segments
                    "min.insync.replicas": str(min_insync_replicas),
                    "compression.type": "lz4",
                    "unclean.leader.election.enable": "false"
                },
                tags=["production", "durable"],
                pre_action=LifecycleAction.ENSURE_EXISTS,
                post_action=LifecycleAction.SKIP,
                create_if_missing=True,
                fail_if_exists=False,
                clear_before_use=False
            ))
        
        return LifecycleConfig(
            topics=topics,
            default_partitions=partitions,
            default_replication_factor=replication_factor,
            pre_generation_policy=LifecycleAction.ENSURE_EXISTS,
            post_generation_policy=CleanupPolicy.NONE,
            confirm_destructive_actions=True,
            dry_run_mode=False,
            max_topics_to_delete=1,  # Very conservative
            environment="production",
            allowed_topic_patterns=[],  # No pattern restrictions
            protected_topics=[],  # Will be populated by admin
            operation_timeout=60,
            max_retries=5,
            retry_delay=2.0
        )
    
    def validate_environment(self, config: LifecycleConfig) -> List[str]:
        """Validate production environment."""
        issues = []
        
        # Check environment
        if config.environment not in ["production", "prod"]:
            issues.append(f"Production strategy should only be used in production environment")
        
        # Check durability settings
        for topic in config.topics:
            if topic.replication_factor < 3:
                issues.append(f"Topic '{topic.name}' has low replication factor for production: {topic.replication_factor}")
            
            if topic.partitions < 3:
                issues.append(f"Topic '{topic.name}' has low partition count for production: {topic.partitions}")
            
            min_insync = int(topic.config.get("min.insync.replicas", "1"))
            if min_insync < 2:
                issues.append(f"Topic '{topic.name}' has low min.insync.replicas for production: {min_insync}")
        
        # Check for dangerous actions
        dangerous_actions = [LifecycleAction.DELETE, LifecycleAction.RECREATE, LifecycleAction.CLEAR_DATA]
        for topic in config.topics:
            if topic.pre_action in dangerous_actions:
                issues.append(f"Topic '{topic.name}' has dangerous pre-action for production: {topic.pre_action.value}")
            if topic.post_action in dangerous_actions:
                issues.append(f"Topic '{topic.name}' has dangerous post-action for production: {topic.post_action.value}")
        
        # Check cleanup policy
        if config.post_generation_policy != CleanupPolicy.NONE:
            issues.append(f"Production should not have post-generation cleanup: {config.post_generation_policy.value}")
        
        return issues


class PerformanceTestStrategy(LifecycleStrategy):
    """Strategy optimized for performance testing scenarios."""
    
    def __init__(self):
        super().__init__(
            name="performance_test",
            description="Optimized for high-throughput performance testing"
        )
    
    def create_config(self, topic_names: List[str], **kwargs) -> LifecycleConfig:
        """
        Create performance test configuration.
        
        Args:
            topic_names: List of topic names
            **kwargs: Additional parameters:
                - partitions: int (default 12) - Number of partitions for parallelism
                - batch_size: int (default 1048576) - Batch size in bytes
                - linger_ms: int (default 100) - Linger time for batching
                
        Returns:
            Performance test lifecycle configuration
        """
        partitions = kwargs.get('partitions', 12)
        batch_size = kwargs.get('batch_size', 1048576)  # 1MB
        linger_ms = kwargs.get('linger_ms', 100)
        
        topics = []
        for topic_name in topic_names:
            perf_topic_name = f"perf-{topic_name}"
            
            topics.append(TopicConfig(
                name=perf_topic_name,
                partitions=partitions,
                replication_factor=1,  # Single replica for performance
                config={
                    "cleanup.policy": "delete",
                    "retention.ms": "3600000",  # 1 hour
                    "segment.ms": "300000",     # 5 minute segments
                    "compression.type": "lz4",
                    "batch.size": str(batch_size),
                    "linger.ms": str(linger_ms),
                    "max.message.bytes": "10485760"  # 10MB max message
                },
                tags=["performance", "test", "high_throughput"],
                pre_action=LifecycleAction.RECREATE,
                post_action=LifecycleAction.DELETE,
                create_if_missing=True,
                fail_if_exists=False,
                clear_before_use=True
            ))
        
        return LifecycleConfig(
            topics=topics,
            default_partitions=partitions,
            default_replication_factor=1,
            pre_generation_policy=LifecycleAction.RECREATE,
            post_generation_policy=CleanupPolicy.DELETE_TOPICS,
            confirm_destructive_actions=False,
            dry_run_mode=False,
            max_topics_to_delete=100,
            environment="performance_test",
            allowed_topic_patterns=["perf-*", "benchmark-*", "load-*"],
            protected_topics=[],
            operation_timeout=45
        )
    
    def validate_environment(self, config: LifecycleConfig) -> List[str]:
        """Validate performance test environment."""
        issues = []
        
        # Check for performance-oriented settings
        for topic in config.topics:
            if topic.partitions < 6:
                issues.append(f"Topic '{topic.name}' may have too few partitions for performance testing: {topic.partitions}")
            
            if topic.replication_factor > 1:
                issues.append(f"Topic '{topic.name}' has replication which may impact performance: {topic.replication_factor}")
        
        return issues


# Registry of available strategies
STRATEGY_REGISTRY = {
    "test_data": TestDataStrategy,
    "development": DevelopmentStrategy, 
    "production": ProductionStrategy,
    "performance_test": PerformanceTestStrategy
}


def get_strategy(strategy_name: str) -> LifecycleStrategy:
    """
    Get lifecycle strategy by name.
    
    Args:
        strategy_name: Name of the strategy
        
    Returns:
        Lifecycle strategy instance
        
    Raises:
        ValueError: If strategy not found
    """
    if strategy_name not in STRATEGY_REGISTRY:
        available = ", ".join(STRATEGY_REGISTRY.keys())
        raise ValueError(f"Unknown strategy '{strategy_name}'. Available: {available}")
    
    return STRATEGY_REGISTRY[strategy_name]()


def list_strategies() -> Dict[str, Dict[str, str]]:
    """
    List all available strategies.
    
    Returns:
        Dictionary of strategy information
    """
    strategies = {}
    for name, strategy_class in STRATEGY_REGISTRY.items():
        strategy = strategy_class()
        strategies[name] = strategy.get_strategy_info()
    
    return strategies