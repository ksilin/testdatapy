"""Comprehensive topic lifecycle management implementation.

This module provides the main TopicLifecycleManager class that orchestrates
all topic lifecycle operations including creation, cleanup, and management.
"""

import time
from typing import Dict, List, Any, Optional, Callable
from pathlib import Path
from dataclasses import dataclass

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
from confluent_kafka.error import KafkaError

from .lifecycle_config import LifecycleConfig, TopicConfig, LifecycleAction, CleanupPolicy
from .lifecycle_strategies import LifecycleResult, LifecycleStrategy, get_strategy
from ..topics import TopicManager
from ..logging_config import get_schema_logger
from ..performance.performance_monitor import get_performance_monitor, monitored


@dataclass
class LifecycleExecutionResult:
    """Result of lifecycle execution."""
    success: bool
    total_operations: int
    successful_operations: int
    failed_operations: int
    skipped_operations: int
    results: List[LifecycleResult]
    total_duration: float
    dry_run: bool
    
    def get_failed_results(self) -> List[LifecycleResult]:
        """Get failed operation results."""
        return [r for r in self.results if not r.success]
    
    def get_successful_results(self) -> List[LifecycleResult]:
        """Get successful operation results."""
        return [r for r in self.results if r.success]
    
    def get_summary(self) -> Dict[str, Any]:
        """Get execution summary."""
        return {
            "success": self.success,
            "total_operations": self.total_operations,
            "successful": self.successful_operations,
            "failed": self.failed_operations,
            "skipped": self.skipped_operations,
            "duration": self.total_duration,
            "dry_run": self.dry_run
        }


class TopicLifecycleManager:
    """Comprehensive topic lifecycle management."""
    
    def __init__(self, bootstrap_servers: str, admin_config: Optional[Dict[str, Any]] = None):
        """
        Initialize the lifecycle manager.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            admin_config: Additional admin client configuration
        """
        self._logger = get_schema_logger(__name__)
        self._bootstrap_servers = bootstrap_servers
        self._admin_config = admin_config or {}
        self._monitor = get_performance_monitor()
        
        # Initialize Kafka admin client
        full_config = {"bootstrap.servers": bootstrap_servers}
        full_config.update(self._admin_config)
        self._admin_client = AdminClient(full_config)
        
        # Initialize topic manager
        self._topic_manager = TopicManager(bootstrap_servers, admin_config)
        
        # Operation statistics
        self._stats = {
            "executions": 0,
            "total_operations": 0,
            "successful_operations": 0,
            "failed_operations": 0,
            "total_execution_time": 0.0
        }
        
        self._logger.info("TopicLifecycleManager initialized",
                         bootstrap_servers=bootstrap_servers)
    
    @monitored("lifecycle_execution")
    def execute_lifecycle(self, config: LifecycleConfig, phase: str = "both") -> LifecycleExecutionResult:
        """
        Execute lifecycle operations based on configuration.
        
        Args:
            config: Lifecycle configuration
            phase: Phase to execute ("pre", "post", or "both")
            
        Returns:
            Execution result
        """
        start_time = time.perf_counter()
        self._stats["executions"] += 1
        
        # Validate configuration
        validation_issues = config.validate()
        if validation_issues:
            self._logger.error("Configuration validation failed", issues=validation_issues)
            raise ValueError(f"Configuration validation failed: {'; '.join(validation_issues)}")
        
        results = []
        
        if phase in ["pre", "both"]:
            self._logger.info("Executing pre-generation lifecycle operations")
            pre_results = self._execute_pre_generation(config)
            results.extend(pre_results)
        
        if phase in ["post", "both"]:
            self._logger.info("Executing post-generation lifecycle operations")
            post_results = self._execute_post_generation(config)
            results.extend(post_results)
        
        # Calculate statistics
        total_operations = len(results)
        successful_operations = sum(1 for r in results if r.success)
        failed_operations = sum(1 for r in results if not r.success and r.error)
        skipped_operations = total_operations - successful_operations - failed_operations
        
        duration = time.perf_counter() - start_time
        
        # Update statistics
        self._stats["total_operations"] += total_operations
        self._stats["successful_operations"] += successful_operations
        self._stats["failed_operations"] += failed_operations
        self._stats["total_execution_time"] += duration
        
        execution_result = LifecycleExecutionResult(
            success=failed_operations == 0,
            total_operations=total_operations,
            successful_operations=successful_operations,
            failed_operations=failed_operations,
            skipped_operations=skipped_operations,
            results=results,
            total_duration=duration,
            dry_run=config.dry_run_mode
        )
        
        self._logger.info("Lifecycle execution completed",
                         **execution_result.get_summary())
        
        return execution_result
    
    def _execute_pre_generation(self, config: LifecycleConfig) -> List[LifecycleResult]:
        """Execute pre-generation operations."""
        results = []
        
        for topic in config.topics:
            result = self._execute_topic_action(topic, topic.pre_action, config, "pre")
            results.append(result)
        
        return results
    
    def _execute_post_generation(self, config: LifecycleConfig) -> List[LifecycleResult]:
        """Execute post-generation operations."""
        results = []
        
        # Execute topic-specific post actions
        for topic in config.topics:
            if topic.post_action != LifecycleAction.SKIP:
                result = self._execute_topic_action(topic, topic.post_action, config, "post")
                results.append(result)
        
        # Execute global cleanup policy
        if config.post_generation_policy != CleanupPolicy.NONE:
            cleanup_results = self._execute_cleanup_policy(config)
            results.extend(cleanup_results)
        
        return results
    
    def _execute_topic_action(self, topic: TopicConfig, action: LifecycleAction,
                            config: LifecycleConfig, phase: str) -> LifecycleResult:
        """Execute a specific action on a topic."""
        start_time = time.perf_counter()
        
        try:
            if action == LifecycleAction.SKIP:
                return LifecycleResult(
                    success=True,
                    action=action.value,
                    topic_name=topic.name,
                    message="Skipped",
                    details={},
                    duration=0.0
                )
            
            # Check topic name permissions
            if not config.is_topic_name_allowed(topic.name):
                return LifecycleResult(
                    success=False,
                    action=action.value,
                    topic_name=topic.name,
                    message="Topic name not allowed by pattern rules",
                    details={"allowed_patterns": config.allowed_topic_patterns},
                    duration=time.perf_counter() - start_time,
                    error=ValueError("Topic name not allowed")
                )
            
            # Check protection
            if config.is_topic_protected(topic.name) and action in [LifecycleAction.DELETE, LifecycleAction.RECREATE]:
                return LifecycleResult(
                    success=False,
                    action=action.value,
                    topic_name=topic.name,
                    message="Topic is protected from destructive operations",
                    details={"protected_topics": config.protected_topics},
                    duration=time.perf_counter() - start_time,
                    error=PermissionError("Topic is protected")
                )
            
            # Dry run mode
            if config.dry_run_mode:
                return LifecycleResult(
                    success=True,
                    action=action.value,
                    topic_name=topic.name,
                    message=f"DRY RUN: Would execute {action.value}",
                    details={"dry_run": True},
                    duration=time.perf_counter() - start_time
                )
            
            # Execute the action
            if action == LifecycleAction.CREATE:
                result = self._create_topic(topic, config)
            elif action == LifecycleAction.ENSURE_EXISTS:
                result = self._ensure_topic_exists(topic, config)
            elif action == LifecycleAction.CLEAR_DATA:
                result = self._clear_topic_data(topic, config)
            elif action == LifecycleAction.RECREATE:
                result = self._recreate_topic(topic, config)
            elif action == LifecycleAction.DELETE:
                result = self._delete_topic(topic, config)
            else:
                raise ValueError(f"Unknown action: {action}")
            
            duration = time.perf_counter() - start_time
            result.duration = duration
            
            self._logger.debug("Topic action completed",
                             topic=topic.name,
                             action=action.value,
                             success=result.success,
                             duration=duration)
            
            return result
        
        except Exception as e:
            duration = time.perf_counter() - start_time
            self._logger.error("Topic action failed",
                             topic=topic.name,
                             action=action.value,
                             error=str(e),
                             duration=duration)
            
            return LifecycleResult(
                success=False,
                action=action.value,
                topic_name=topic.name,
                message=f"Action failed: {str(e)}",
                details={"error_type": type(e).__name__},
                duration=duration,
                error=e
            )
    
    def _create_topic(self, topic: TopicConfig, config: LifecycleConfig) -> LifecycleResult:
        """Create a topic."""
        if self._topic_manager.topic_exists(topic.name):
            if topic.fail_if_exists:
                return LifecycleResult(
                    success=False,
                    action="create",
                    topic_name=topic.name,
                    message="Topic already exists and fail_if_exists is True",
                    details={"fail_if_exists": True},
                    duration=0.0,
                    error=ValueError("Topic already exists")
                )
            else:
                return LifecycleResult(
                    success=True,
                    action="create",
                    topic_name=topic.name,
                    message="Topic already exists, skipping creation",
                    details={"already_exists": True},
                    duration=0.0
                )
        
        self._topic_manager.create_topic(
            topic.name,
            topic.partitions,
            topic.replication_factor,
            topic.config
        )
        
        return LifecycleResult(
            success=True,
            action="create",
            topic_name=topic.name,
            message="Topic created successfully",
            details={
                "partitions": topic.partitions,
                "replication_factor": topic.replication_factor,
                "config": topic.config
            },
            duration=0.0
        )
    
    def _ensure_topic_exists(self, topic: TopicConfig, config: LifecycleConfig) -> LifecycleResult:
        """Ensure topic exists, creating if necessary."""
        exists = self._topic_manager.topic_exists(topic.name)
        
        if exists:
            return LifecycleResult(
                success=True,
                action="ensure_exists",
                topic_name=topic.name,
                message="Topic already exists",
                details={"already_exists": True},
                duration=0.0
            )
        
        if topic.create_if_missing:
            self._topic_manager.create_topic(
                topic.name,
                topic.partitions,
                topic.replication_factor,
                topic.config
            )
            
            return LifecycleResult(
                success=True,
                action="ensure_exists",
                topic_name=topic.name,
                message="Topic created (was missing)",
                details={
                    "created": True,
                    "partitions": topic.partitions,
                    "replication_factor": topic.replication_factor
                },
                duration=0.0
            )
        else:
            return LifecycleResult(
                success=False,
                action="ensure_exists",
                topic_name=topic.name,
                message="Topic doesn't exist and create_if_missing is False",
                details={"create_if_missing": False},
                duration=0.0,
                error=ValueError("Topic doesn't exist")
            )
    
    def _clear_topic_data(self, topic: TopicConfig, config: LifecycleConfig) -> LifecycleResult:
        """Clear topic data by recreating with short retention."""
        if not self._topic_manager.topic_exists(topic.name):
            return LifecycleResult(
                success=False,
                action="clear_data",
                topic_name=topic.name,
                message="Cannot clear data - topic doesn't exist",
                details={},
                duration=0.0,
                error=ValueError("Topic doesn't exist")
            )
        
        # For now, implement as recreate
        # In a more sophisticated implementation, you could:
        # 1. Temporarily change retention policy to very short
        # 2. Wait for data to expire
        # 3. Restore original retention policy
        return self._recreate_topic(topic, config)
    
    def _recreate_topic(self, topic: TopicConfig, config: LifecycleConfig) -> LifecycleResult:
        """Recreate a topic (delete and create)."""
        exists = self._topic_manager.topic_exists(topic.name)
        operations = []
        
        if exists:
            try:
                self._topic_manager.delete_topic(topic.name)
                operations.append("deleted")
                
                # Wait a moment for deletion to propagate
                time.sleep(1.0)
            except Exception as e:
                return LifecycleResult(
                    success=False,
                    action="recreate",
                    topic_name=topic.name,
                    message=f"Failed to delete existing topic: {str(e)}",
                    details={"operations": operations},
                    duration=0.0,
                    error=e
                )
        
        try:
            self._topic_manager.create_topic(
                topic.name,
                topic.partitions,
                topic.replication_factor,
                topic.config
            )
            operations.append("created")
        except Exception as e:
            return LifecycleResult(
                success=False,
                action="recreate",
                topic_name=topic.name,
                message=f"Failed to create topic: {str(e)}",
                details={"operations": operations},
                duration=0.0,
                error=e
            )
        
        return LifecycleResult(
            success=True,
            action="recreate",
            topic_name=topic.name,
            message="Topic recreated successfully",
            details={
                "operations": operations,
                "partitions": topic.partitions,
                "replication_factor": topic.replication_factor
            },
            duration=0.0
        )
    
    def _delete_topic(self, topic: TopicConfig, config: LifecycleConfig) -> LifecycleResult:
        """Delete a topic."""
        if not self._topic_manager.topic_exists(topic.name):
            return LifecycleResult(
                success=True,
                action="delete",
                topic_name=topic.name,
                message="Topic doesn't exist (already deleted)",
                details={"already_deleted": True},
                duration=0.0
            )
        
        self._topic_manager.delete_topic(topic.name)
        
        return LifecycleResult(
            success=True,
            action="delete",
            topic_name=topic.name,
            message="Topic deleted successfully",
            details={},
            duration=0.0
        )
    
    def _execute_cleanup_policy(self, config: LifecycleConfig) -> List[LifecycleResult]:
        """Execute global cleanup policy."""
        results = []
        
        if config.post_generation_policy == CleanupPolicy.DELETE_TOPICS:
            # Delete all topics in configuration
            topics_to_delete = [t for t in config.topics if not config.is_topic_protected(t.name)]
            
            if len(topics_to_delete) > config.max_topics_to_delete:
                result = LifecycleResult(
                    success=False,
                    action="cleanup_policy",
                    topic_name="ALL",
                    message=f"Too many topics to delete: {len(topics_to_delete)} > {config.max_topics_to_delete}",
                    details={"topics_count": len(topics_to_delete), "max_allowed": config.max_topics_to_delete},
                    duration=0.0,
                    error=ValueError("Too many topics to delete")
                )
                results.append(result)
                return results
            
            for topic in topics_to_delete:
                result = self._delete_topic(topic, config)
                results.append(result)
        
        elif config.post_generation_policy == CleanupPolicy.CLEAR_DATA:
            # Clear data from all topics
            for topic in config.topics:
                if not config.is_topic_protected(topic.name):
                    result = self._clear_topic_data(topic, config)
                    results.append(result)
        
        return results
    
    def validate_kafka_connection(self) -> Dict[str, Any]:
        """Validate connection to Kafka cluster."""
        try:
            metadata = self._admin_client.list_topics(timeout=10)
            return {
                "connected": True,
                "broker_count": len(metadata.brokers),
                "topic_count": len(metadata.topics),
                "cluster_id": getattr(metadata, 'cluster_id', 'unknown')
            }
        except Exception as e:
            return {
                "connected": False,
                "error": str(e),
                "error_type": type(e).__name__
            }
    
    def get_existing_topics(self, name_filter: Optional[str] = None) -> List[str]:
        """Get list of existing topics with optional name filtering."""
        try:
            all_topics = self._topic_manager.list_topics()
            
            if name_filter:
                import fnmatch
                return [topic for topic in all_topics if fnmatch.fnmatch(topic, name_filter)]
            
            return all_topics
        except Exception as e:
            self._logger.error("Failed to list topics", error=str(e))
            return []
    
    def preview_lifecycle_operations(self, config: LifecycleConfig, phase: str = "both") -> Dict[str, Any]:
        """Preview what operations would be performed without executing them."""
        preview_config = LifecycleConfig.from_dict(config.to_dict())
        preview_config.dry_run_mode = True
        
        # Execute in dry run mode
        result = self.execute_lifecycle(preview_config, phase)
        
        # Group operations by type
        operations_by_action = {}
        for op_result in result.results:
            action = op_result.action
            if action not in operations_by_action:
                operations_by_action[action] = []
            operations_by_action[action].append({
                "topic": op_result.topic_name,
                "message": op_result.message,
                "details": op_result.details
            })
        
        return {
            "total_operations": result.total_operations,
            "operations_by_action": operations_by_action,
            "existing_topics": self.get_existing_topics(),
            "validation_issues": config.validate()
        }
    
    def apply_strategy(self, strategy_name: str, topic_names: List[str], **kwargs) -> LifecycleConfig:
        """Apply a predefined lifecycle strategy."""
        strategy = get_strategy(strategy_name)
        config = strategy.create_config(topic_names, **kwargs)
        
        # Validate strategy for current environment
        validation_issues = strategy.validate_environment(config)
        if validation_issues:
            self._logger.warning("Strategy validation issues", 
                               strategy=strategy_name,
                               issues=validation_issues)
        
        return config
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get lifecycle manager statistics."""
        stats = self._stats.copy()
        
        if stats["executions"] > 0:
            stats["avg_execution_time"] = stats["total_execution_time"] / stats["executions"]
            stats["avg_operations_per_execution"] = stats["total_operations"] / stats["executions"]
        
        if stats["total_operations"] > 0:
            stats["success_rate"] = stats["successful_operations"] / stats["total_operations"]
        
        return stats