"""Configuration models for topic lifecycle management.

This module defines the configuration structure for managing topic lifecycles
including creation, cleanup, and retention policies.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any
import json
from pathlib import Path


class LifecycleAction(Enum):
    """Available lifecycle actions."""
    CREATE = "create"
    ENSURE_EXISTS = "ensure_exists"
    CLEAR_DATA = "clear_data"
    RECREATE = "recreate"
    DELETE = "delete"
    SKIP = "skip"


class CleanupPolicy(Enum):
    """Cleanup policies for topics."""
    NONE = "none"
    DELETE_TOPICS = "delete_topics"
    CLEAR_DATA = "clear_data"
    ARCHIVE_DATA = "archive_data"
    CONDITIONAL = "conditional"


@dataclass
class TopicConfig:
    """Configuration for a single topic."""
    name: str
    partitions: int = 1
    replication_factor: int = 1
    config: Dict[str, str] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    
    # Lifecycle behavior
    pre_action: LifecycleAction = LifecycleAction.ENSURE_EXISTS
    post_action: LifecycleAction = LifecycleAction.SKIP
    
    # Conditional logic
    create_if_missing: bool = True
    fail_if_exists: bool = False
    clear_before_use: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "name": self.name,
            "partitions": self.partitions,
            "replication_factor": self.replication_factor,
            "config": self.config,
            "tags": self.tags,
            "pre_action": self.pre_action.value,
            "post_action": self.post_action.value,
            "create_if_missing": self.create_if_missing,
            "fail_if_exists": self.fail_if_exists,
            "clear_before_use": self.clear_before_use
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TopicConfig':
        """Create from dictionary representation."""
        return cls(
            name=data["name"],
            partitions=data.get("partitions", 1),
            replication_factor=data.get("replication_factor", 1),
            config=data.get("config", {}),
            tags=data.get("tags", []),
            pre_action=LifecycleAction(data.get("pre_action", "ensure_exists")),
            post_action=LifecycleAction(data.get("post_action", "skip")),
            create_if_missing=data.get("create_if_missing", True),
            fail_if_exists=data.get("fail_if_exists", False),
            clear_before_use=data.get("clear_before_use", False)
        )


@dataclass
class LifecycleConfig:
    """Complete lifecycle configuration."""
    topics: List[TopicConfig] = field(default_factory=list)
    
    # Global settings
    default_partitions: int = 1
    default_replication_factor: int = 1
    default_config: Dict[str, str] = field(default_factory=dict)
    
    # Lifecycle policies
    pre_generation_policy: LifecycleAction = LifecycleAction.ENSURE_EXISTS
    post_generation_policy: CleanupPolicy = CleanupPolicy.NONE
    
    # Safety settings
    confirm_destructive_actions: bool = True
    dry_run_mode: bool = False
    max_topics_to_delete: int = 10
    
    # Timing and retries
    operation_timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    
    # Environment-specific settings
    environment: str = "development"
    allowed_topic_patterns: List[str] = field(default_factory=lambda: ["test-*", "dev-*", "*-test"])
    protected_topics: List[str] = field(default_factory=list)
    
    def add_topic(self, topic: TopicConfig) -> None:
        """Add a topic configuration."""
        # Check if topic already exists and update if so
        for i, existing_topic in enumerate(self.topics):
            if existing_topic.name == topic.name:
                self.topics[i] = topic
                return
        
        self.topics.append(topic)
    
    def remove_topic(self, topic_name: str) -> bool:
        """Remove a topic configuration."""
        for i, topic in enumerate(self.topics):
            if topic.name == topic_name:
                del self.topics[i]
                return True
        return False
    
    def get_topic(self, topic_name: str) -> Optional[TopicConfig]:
        """Get topic configuration by name."""
        for topic in self.topics:
            if topic.name == topic_name:
                return topic
        return None
    
    def get_topics_by_tag(self, tag: str) -> List[TopicConfig]:
        """Get topics with specific tag."""
        return [topic for topic in self.topics if tag in topic.tags]
    
    def get_topics_by_action(self, action: LifecycleAction, phase: str = "pre") -> List[TopicConfig]:
        """Get topics with specific action."""
        if phase == "pre":
            return [topic for topic in self.topics if topic.pre_action == action]
        elif phase == "post":
            return [topic for topic in self.topics if topic.post_action == action]
        else:
            raise ValueError("Phase must be 'pre' or 'post'")
    
    def is_topic_protected(self, topic_name: str) -> bool:
        """Check if topic is protected from deletion."""
        return topic_name in self.protected_topics
    
    def is_topic_name_allowed(self, topic_name: str) -> bool:
        """Check if topic name matches allowed patterns."""
        if not self.allowed_topic_patterns:
            return True
        
        import fnmatch
        return any(fnmatch.fnmatch(topic_name, pattern) for pattern in self.allowed_topic_patterns)
    
    def validate(self) -> List[str]:
        """Validate configuration and return any issues."""
        issues = []
        
        # Check for duplicate topic names
        topic_names = [topic.name for topic in self.topics]
        duplicates = set([name for name in topic_names if topic_names.count(name) > 1])
        if duplicates:
            issues.append(f"Duplicate topic names: {', '.join(duplicates)}")
        
        # Check topic name patterns
        for topic in self.topics:
            if not self.is_topic_name_allowed(topic.name):
                issues.append(f"Topic name '{topic.name}' doesn't match allowed patterns")
        
        # Check for protected topics in deletion lists
        deletion_topics = self.get_topics_by_action(LifecycleAction.DELETE, "post")
        for topic in deletion_topics:
            if self.is_topic_protected(topic.name):
                issues.append(f"Protected topic '{topic.name}' is marked for deletion")
        
        # Validate replication factors
        for topic in self.topics:
            if topic.replication_factor < 1:
                issues.append(f"Topic '{topic.name}' has invalid replication factor: {topic.replication_factor}")
        
        # Validate partition counts
        for topic in self.topics:
            if topic.partitions < 1:
                issues.append(f"Topic '{topic.name}' has invalid partition count: {topic.partitions}")
        
        return issues
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "topics": [topic.to_dict() for topic in self.topics],
            "default_partitions": self.default_partitions,
            "default_replication_factor": self.default_replication_factor,
            "default_config": self.default_config,
            "pre_generation_policy": self.pre_generation_policy.value,
            "post_generation_policy": self.post_generation_policy.value,
            "confirm_destructive_actions": self.confirm_destructive_actions,
            "dry_run_mode": self.dry_run_mode,
            "max_topics_to_delete": self.max_topics_to_delete,
            "operation_timeout": self.operation_timeout,
            "max_retries": self.max_retries,
            "retry_delay": self.retry_delay,
            "environment": self.environment,
            "allowed_topic_patterns": self.allowed_topic_patterns,
            "protected_topics": self.protected_topics
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'LifecycleConfig':
        """Create from dictionary representation."""
        topics = [TopicConfig.from_dict(topic_data) for topic_data in data.get("topics", [])]
        
        return cls(
            topics=topics,
            default_partitions=data.get("default_partitions", 1),
            default_replication_factor=data.get("default_replication_factor", 1),
            default_config=data.get("default_config", {}),
            pre_generation_policy=LifecycleAction(data.get("pre_generation_policy", "ensure_exists")),
            post_generation_policy=CleanupPolicy(data.get("post_generation_policy", "none")),
            confirm_destructive_actions=data.get("confirm_destructive_actions", True),
            dry_run_mode=data.get("dry_run_mode", False),
            max_topics_to_delete=data.get("max_topics_to_delete", 10),
            operation_timeout=data.get("operation_timeout", 30),
            max_retries=data.get("max_retries", 3),
            retry_delay=data.get("retry_delay", 1.0),
            environment=data.get("environment", "development"),
            allowed_topic_patterns=data.get("allowed_topic_patterns", ["test-*", "dev-*", "*-test"]),
            protected_topics=data.get("protected_topics", [])
        )
    
    def save_to_file(self, file_path: str) -> None:
        """Save configuration to JSON file."""
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(path, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
    
    @classmethod
    def load_from_file(cls, file_path: str) -> 'LifecycleConfig':
        """Load configuration from JSON file."""
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        return cls.from_dict(data)
    
    @classmethod
    def create_test_config(cls, topic_names: List[str]) -> 'LifecycleConfig':
        """Create a test configuration for given topic names."""
        topics = []
        for topic_name in topic_names:
            topics.append(TopicConfig(
                name=topic_name,
                partitions=1,
                replication_factor=1,
                pre_action=LifecycleAction.ENSURE_EXISTS,
                post_action=LifecycleAction.DELETE,
                clear_before_use=True,
                tags=["test"]
            ))
        
        return cls(
            topics=topics,
            pre_generation_policy=LifecycleAction.ENSURE_EXISTS,
            post_generation_policy=CleanupPolicy.DELETE_TOPICS,
            environment="test",
            allowed_topic_patterns=["test-*", "*-test", "temp-*"],
            confirm_destructive_actions=False
        )
    
    @classmethod
    def create_production_config(cls, topic_names: List[str]) -> 'LifecycleConfig':
        """Create a production-safe configuration for given topic names."""
        topics = []
        for topic_name in topic_names:
            topics.append(TopicConfig(
                name=topic_name,
                partitions=3,
                replication_factor=3,
                pre_action=LifecycleAction.ENSURE_EXISTS,
                post_action=LifecycleAction.SKIP,
                clear_before_use=False,
                create_if_missing=True,
                fail_if_exists=False
            ))
        
        return cls(
            topics=topics,
            default_partitions=3,
            default_replication_factor=3,
            pre_generation_policy=LifecycleAction.ENSURE_EXISTS,
            post_generation_policy=CleanupPolicy.NONE,
            environment="production",
            allowed_topic_patterns=[],  # No restrictions in production
            confirm_destructive_actions=True,
            dry_run_mode=False
        )