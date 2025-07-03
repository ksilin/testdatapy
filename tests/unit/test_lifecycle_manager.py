"""Unit tests for TopicLifecycleManager.

This module tests the topic lifecycle management functionality including
configuration, strategies, and execution.
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pytest

from testdatapy.lifecycle import (
    TopicLifecycleManager, LifecycleConfig, TopicConfig,
    LifecycleAction, CleanupPolicy, TestDataStrategy, ProductionStrategy
)


class TestLifecycleConfig(unittest.TestCase):
    """Test LifecycleConfig functionality."""
    
    def test_topic_config_creation(self):
        """Test TopicConfig creation and validation."""
        topic = TopicConfig(
            name="test-topic",
            partitions=3,
            replication_factor=2,
            pre_action=LifecycleAction.ENSURE_EXISTS,
            post_action=LifecycleAction.DELETE
        )
        
        self.assertEqual(topic.name, "test-topic")
        self.assertEqual(topic.partitions, 3)
        self.assertEqual(topic.replication_factor, 2)
        self.assertEqual(topic.pre_action, LifecycleAction.ENSURE_EXISTS)
        self.assertEqual(topic.post_action, LifecycleAction.DELETE)
    
    def test_topic_config_serialization(self):
        """Test TopicConfig serialization to/from dict."""
        topic = TopicConfig(
            name="serialization-test",
            partitions=2,
            config={"retention.ms": "86400000"},
            tags=["test", "temporary"]
        )
        
        # Serialize to dict
        topic_dict = topic.to_dict()
        self.assertEqual(topic_dict["name"], "serialization-test")
        self.assertEqual(topic_dict["partitions"], 2)
        self.assertEqual(topic_dict["config"]["retention.ms"], "86400000")
        self.assertIn("test", topic_dict["tags"])
        
        # Deserialize from dict
        restored_topic = TopicConfig.from_dict(topic_dict)
        self.assertEqual(restored_topic.name, topic.name)
        self.assertEqual(restored_topic.partitions, topic.partitions)
        self.assertEqual(restored_topic.config, topic.config)
        self.assertEqual(restored_topic.tags, topic.tags)
    
    def test_lifecycle_config_creation(self):
        """Test LifecycleConfig creation and manipulation."""
        config = LifecycleConfig(
            default_partitions=2,
            environment="test",
            allowed_topic_patterns=["test-*"]
        )
        
        # Add topics
        topic1 = TopicConfig(name="test-topic-1")
        topic2 = TopicConfig(name="test-topic-2")
        
        config.add_topic(topic1)
        config.add_topic(topic2)
        
        self.assertEqual(len(config.topics), 2)
        self.assertEqual(config.get_topic("test-topic-1"), topic1)
        self.assertEqual(config.get_topic("test-topic-2"), topic2)
    
    def test_lifecycle_config_validation(self):
        """Test LifecycleConfig validation."""
        config = LifecycleConfig(
            environment="test",
            allowed_topic_patterns=["test-*"],
            protected_topics=["important-topic"]
        )
        
        # Add valid topic
        config.add_topic(TopicConfig(name="test-valid"))
        
        # Add invalid topic (doesn't match pattern)
        config.add_topic(TopicConfig(name="invalid-name"))
        
        # Add duplicate topic
        config.add_topic(TopicConfig(name="test-valid"))
        
        # Add topic marked for deletion that's protected
        config.add_topic(TopicConfig(
            name="important-topic",
            post_action=LifecycleAction.DELETE
        ))
        
        # Add topic with invalid settings
        config.add_topic(TopicConfig(
            name="test-invalid-partitions",
            partitions=0  # Invalid
        ))
        
        issues = config.validate()
        
        # Should find multiple issues
        self.assertGreater(len(issues), 0)
        
        # Check specific issues
        issue_text = " ".join(issues)
        self.assertIn("doesn't match allowed patterns", issue_text)
        self.assertIn("Duplicate topic names", issue_text)
        self.assertIn("Protected topic", issue_text)
        self.assertIn("invalid partition count", issue_text)
    
    def test_lifecycle_config_file_operations(self):
        """Test saving and loading configuration files."""
        config = LifecycleConfig(
            environment="test",
            pre_generation_policy=LifecycleAction.ENSURE_EXISTS,
            post_generation_policy=CleanupPolicy.DELETE_TOPICS
        )
        
        config.add_topic(TopicConfig(
            name="file-test-topic",
            partitions=3,
            config={"cleanup.policy": "delete"}
        ))
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_file = f.name
        
        try:
            # Save to file
            config.save_to_file(temp_file)
            
            # Load from file
            loaded_config = LifecycleConfig.load_from_file(temp_file)
            
            # Verify loaded configuration
            self.assertEqual(loaded_config.environment, "test")
            self.assertEqual(loaded_config.pre_generation_policy, LifecycleAction.ENSURE_EXISTS)
            self.assertEqual(loaded_config.post_generation_policy, CleanupPolicy.DELETE_TOPICS)
            self.assertEqual(len(loaded_config.topics), 1)
            
            topic = loaded_config.topics[0]
            self.assertEqual(topic.name, "file-test-topic")
            self.assertEqual(topic.partitions, 3)
            self.assertEqual(topic.config["cleanup.policy"], "delete")
        
        finally:
            Path(temp_file).unlink()
    
    def test_create_test_config(self):
        """Test creating test configuration."""
        topic_names = ["test-topic-1", "test-topic-2", "test-topic-3"]
        config = LifecycleConfig.create_test_config(topic_names)
        
        self.assertEqual(len(config.topics), 3)
        self.assertEqual(config.environment, "test")
        self.assertEqual(config.post_generation_policy, CleanupPolicy.DELETE_TOPICS)
        self.assertFalse(config.confirm_destructive_actions)
        
        for topic in config.topics:
            self.assertEqual(topic.replication_factor, 1)
            self.assertEqual(topic.pre_action, LifecycleAction.ENSURE_EXISTS)
            self.assertEqual(topic.post_action, LifecycleAction.DELETE)
            self.assertTrue(topic.clear_before_use)
            self.assertIn("test", topic.tags)
    
    def test_create_production_config(self):
        """Test creating production configuration."""
        topic_names = ["prod-topic-1", "prod-topic-2"]
        config = LifecycleConfig.create_production_config(topic_names)
        
        self.assertEqual(len(config.topics), 2)
        self.assertEqual(config.environment, "production")
        self.assertEqual(config.post_generation_policy, CleanupPolicy.NONE)
        self.assertTrue(config.confirm_destructive_actions)
        
        for topic in config.topics:
            self.assertEqual(topic.partitions, 3)
            self.assertEqual(topic.replication_factor, 3)
            self.assertEqual(topic.pre_action, LifecycleAction.ENSURE_EXISTS)
            self.assertEqual(topic.post_action, LifecycleAction.SKIP)
            self.assertFalse(topic.clear_before_use)


class TestLifecycleStrategies(unittest.TestCase):
    """Test lifecycle strategies."""
    
    def test_test_data_strategy(self):
        """Test TestDataStrategy."""
        strategy = TestDataStrategy()
        topic_names = ["topic1", "topic2"]
        
        config = strategy.create_config(topic_names, cleanup_after=True, clear_before=True)
        
        self.assertEqual(len(config.topics), 2)
        self.assertEqual(config.environment, "test")
        self.assertEqual(config.post_generation_policy, CleanupPolicy.DELETE_TOPICS)
        
        for topic in config.topics:
            self.assertTrue(topic.name.startswith("testdata-"))
            self.assertEqual(topic.pre_action, LifecycleAction.RECREATE)
            self.assertEqual(topic.post_action, LifecycleAction.DELETE)
            self.assertTrue(topic.clear_before_use)
            self.assertIn("test_data", topic.tags)
    
    def test_production_strategy(self):
        """Test ProductionStrategy."""
        strategy = ProductionStrategy()
        topic_names = ["prod-topic"]
        
        config = strategy.create_config(topic_names, partitions=6, replication_factor=3)
        
        self.assertEqual(len(config.topics), 1)
        self.assertEqual(config.environment, "production")
        self.assertEqual(config.post_generation_policy, CleanupPolicy.NONE)
        
        topic = config.topics[0]
        self.assertEqual(topic.name, "prod-topic")
        self.assertEqual(topic.partitions, 6)
        self.assertEqual(topic.replication_factor, 3)
        self.assertEqual(topic.pre_action, LifecycleAction.ENSURE_EXISTS)
        self.assertEqual(topic.post_action, LifecycleAction.SKIP)
        self.assertFalse(topic.clear_before_use)
    
    def test_strategy_validation(self):
        """Test strategy environment validation."""
        strategy = ProductionStrategy()
        topic_names = ["test-topic"]
        
        # Create config with wrong environment
        config = strategy.create_config(topic_names)
        config.environment = "development"  # Wrong environment for production strategy
        
        issues = strategy.validate_environment(config)
        self.assertGreater(len(issues), 0)
        self.assertIn("should only be used in production", " ".join(issues))


class TestTopicLifecycleManager(unittest.TestCase):
    """Test TopicLifecycleManager functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_admin_client = Mock()
        self.mock_topic_manager = Mock()
        
        # Mock AdminClient creation
        with patch('testdatapy.lifecycle.topic_lifecycle_manager.AdminClient') as mock_admin, \
             patch('testdatapy.lifecycle.topic_lifecycle_manager.TopicManager') as mock_tm:
            mock_admin.return_value = self.mock_admin_client
            mock_tm.return_value = self.mock_topic_manager
            
            self.manager = TopicLifecycleManager("localhost:9092")
    
    def test_manager_initialization(self):
        """Test TopicLifecycleManager initialization."""
        self.assertEqual(self.manager._bootstrap_servers, "localhost:9092")
        self.assertIsNotNone(self.manager._admin_client)
        self.assertIsNotNone(self.manager._topic_manager)
    
    def test_validate_kafka_connection_success(self):
        """Test successful Kafka connection validation."""
        # Mock successful connection
        mock_metadata = Mock()
        mock_metadata.brokers = {'broker1': None, 'broker2': None}
        mock_metadata.topics = {'topic1': None, 'topic2': None, 'topic3': None}
        mock_metadata.cluster_id = 'test-cluster-id'
        
        self.mock_admin_client.list_topics.return_value = mock_metadata
        
        result = self.manager.validate_kafka_connection()
        
        self.assertTrue(result['connected'])
        self.assertEqual(result['broker_count'], 2)
        self.assertEqual(result['topic_count'], 3)
        self.assertEqual(result['cluster_id'], 'test-cluster-id')
    
    def test_validate_kafka_connection_failure(self):
        """Test failed Kafka connection validation."""
        # Mock connection failure
        self.mock_admin_client.list_topics.side_effect = Exception("Connection failed")
        
        result = self.manager.validate_kafka_connection()
        
        self.assertFalse(result['connected'])
        self.assertIn('error', result)
        self.assertEqual(result['error_type'], 'Exception')
    
    def test_get_existing_topics(self):
        """Test getting existing topics."""
        self.mock_topic_manager.list_topics.return_value = [
            "topic1", "topic2", "test-topic1", "test-topic2", "other-topic"
        ]
        
        # Test without filter
        all_topics = self.manager.get_existing_topics()
        self.assertEqual(len(all_topics), 5)
        
        # Test with filter
        test_topics = self.manager.get_existing_topics("test-*")
        self.assertEqual(len(test_topics), 2)
        self.assertIn("test-topic1", test_topics)
        self.assertIn("test-topic2", test_topics)
    
    def test_execute_lifecycle_dry_run(self):
        """Test lifecycle execution in dry run mode."""
        config = LifecycleConfig(dry_run_mode=True)
        config.add_topic(TopicConfig(
            name="dry-run-topic",
            pre_action=LifecycleAction.CREATE,
            post_action=LifecycleAction.DELETE
        ))
        
        result = self.manager.execute_lifecycle(config, "both")
        
        self.assertTrue(result.success)
        self.assertTrue(result.dry_run)
        self.assertEqual(result.total_operations, 2)  # pre + post
        self.assertEqual(result.successful_operations, 2)
        self.assertEqual(result.failed_operations, 0)
        
        # Verify no actual Kafka operations were performed
        self.mock_topic_manager.create_topic.assert_not_called()
        self.mock_topic_manager.delete_topic.assert_not_called()
    
    def test_execute_lifecycle_pre_phase_only(self):
        """Test executing only pre-generation phase."""
        config = LifecycleConfig(dry_run_mode=True)
        config.add_topic(TopicConfig(
            name="pre-only-topic",
            pre_action=LifecycleAction.ENSURE_EXISTS,
            post_action=LifecycleAction.DELETE
        ))
        
        result = self.manager.execute_lifecycle(config, "pre")
        
        self.assertTrue(result.success)
        self.assertEqual(result.total_operations, 1)  # Only pre action
        
        # Check that the operation was the pre-action
        self.assertEqual(result.results[0].action, "ensure_exists")
    
    def test_execute_lifecycle_validation_failure(self):
        """Test lifecycle execution with configuration validation failure."""
        config = LifecycleConfig()
        # Add invalid topic (duplicate names)
        config.add_topic(TopicConfig(name="duplicate"))
        config.add_topic(TopicConfig(name="duplicate"))
        
        with self.assertRaises(ValueError) as context:
            self.manager.execute_lifecycle(config)
        
        self.assertIn("Configuration validation failed", str(context.exception))
    
    def test_topic_action_ensure_exists_success(self):
        """Test ensure_exists action when topic already exists."""
        config = LifecycleConfig(dry_run_mode=False)
        topic = TopicConfig(name="existing-topic", pre_action=LifecycleAction.ENSURE_EXISTS)
        
        # Mock topic exists
        self.mock_topic_manager.topic_exists.return_value = True
        
        result = self.manager._execute_topic_action(topic, LifecycleAction.ENSURE_EXISTS, config, "pre")
        
        self.assertTrue(result.success)
        self.assertEqual(result.action, "ensure_exists")
        self.assertIn("already exists", result.message)
        
        # Should not try to create
        self.mock_topic_manager.create_topic.assert_not_called()
    
    def test_topic_action_ensure_exists_create(self):
        """Test ensure_exists action when topic needs to be created."""
        config = LifecycleConfig(dry_run_mode=False)
        topic = TopicConfig(name="new-topic", pre_action=LifecycleAction.ENSURE_EXISTS)
        
        # Mock topic doesn't exist
        self.mock_topic_manager.topic_exists.return_value = False
        
        result = self.manager._execute_topic_action(topic, LifecycleAction.ENSURE_EXISTS, config, "pre")
        
        self.assertTrue(result.success)
        self.assertEqual(result.action, "ensure_exists")
        self.assertIn("created", result.message)
        
        # Should try to create
        self.mock_topic_manager.create_topic.assert_called_once_with(
            "new-topic", 1, 1, {}
        )
    
    def test_topic_action_protected_topic(self):
        """Test action on protected topic fails appropriately."""
        config = LifecycleConfig(
            dry_run_mode=False,
            protected_topics=["protected-topic"]
        )
        topic = TopicConfig(name="protected-topic")
        
        result = self.manager._execute_topic_action(topic, LifecycleAction.DELETE, config, "post")
        
        self.assertFalse(result.success)
        self.assertEqual(result.action, "delete")
        self.assertIn("protected", result.message)
        self.assertIsInstance(result.error, PermissionError)
    
    def test_topic_action_name_pattern_violation(self):
        """Test action on topic with invalid name pattern."""
        config = LifecycleConfig(
            dry_run_mode=False,
            allowed_topic_patterns=["test-*"]
        )
        topic = TopicConfig(name="invalid-name")
        
        result = self.manager._execute_topic_action(topic, LifecycleAction.CREATE, config, "pre")
        
        self.assertFalse(result.success)
        self.assertEqual(result.action, "create")
        self.assertIn("not allowed", result.message)
        self.assertIsInstance(result.error, ValueError)
    
    def test_apply_strategy(self):
        """Test applying a predefined strategy."""
        topic_names = ["strategy-topic-1", "strategy-topic-2"]
        
        config = self.manager.apply_strategy("test_data", topic_names, cleanup_after=True)
        
        self.assertEqual(len(config.topics), 2)
        self.assertEqual(config.environment, "test")
        
        for topic in config.topics:
            self.assertTrue(topic.name.startswith("testdata-"))
            self.assertIn("test_data", topic.tags)
    
    def test_preview_lifecycle_operations(self):
        """Test previewing lifecycle operations."""
        config = LifecycleConfig()
        config.add_topic(TopicConfig(
            name="preview-topic",
            pre_action=LifecycleAction.CREATE,
            post_action=LifecycleAction.DELETE
        ))
        
        # Mock existing topics
        self.mock_topic_manager.list_topics.return_value = ["existing-topic"]
        
        preview = self.manager.preview_lifecycle_operations(config, "both")
        
        self.assertEqual(preview['total_operations'], 2)
        self.assertIn('operations_by_action', preview)
        self.assertIn('existing_topics', preview)
        self.assertEqual(len(preview['existing_topics']), 1)
    
    def test_get_statistics(self):
        """Test getting manager statistics."""
        # Execute some operations first
        config = LifecycleConfig(dry_run_mode=True)
        config.add_topic(TopicConfig(name="stats-topic"))
        
        self.manager.execute_lifecycle(config)
        
        stats = self.manager.get_statistics()
        
        self.assertIn('executions', stats)
        self.assertIn('total_operations', stats)
        self.assertIn('successful_operations', stats)
        self.assertGreater(stats['executions'], 0)


if __name__ == '__main__':
    unittest.main()