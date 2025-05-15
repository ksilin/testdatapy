"""Configuration loader and validator for correlation-based data generation."""
from typing import Dict, Any, List, Optional
import yaml
from pathlib import Path


class ValidationError(Exception):
    """Raised when configuration validation fails."""
    pass


class CorrelationConfig:
    """Manages correlation configuration for data generation.
    
    This class loads and validates configuration files that define:
    - Master data sources (customers, products, etc.)
    - Transactional data with relationships
    - Generation rules and constraints
    """
    
    def __init__(self, config_dict: Dict[str, Any]):
        """Initialize with a configuration dictionary.
        
        Args:
            config_dict: Dictionary containing the configuration
            
        Raises:
            ValidationError: If configuration is invalid
        """
        self.config = config_dict
        self._validate()
    
    @classmethod
    def from_yaml_file(cls, file_path: str) -> 'CorrelationConfig':
        """Load configuration from a YAML file.
        
        Args:
            file_path: Path to YAML configuration file
            
        Returns:
            CorrelationConfig instance
        """
        with open(file_path, 'r') as f:
            config_dict = yaml.safe_load(f)
        return cls(config_dict)
    
    def _validate(self) -> None:
        """Validate the configuration.
        
        Raises:
            ValidationError: If configuration is invalid
        """
        # Check required top-level keys
        if "master_data" not in self.config:
            self.config["master_data"] = {}
        
        if "transactional_data" not in self.config:
            self.config["transactional_data"] = {}
        
        # Validate master data
        for name, config in self.config.get("master_data", {}).items():
            self._validate_master_data(name, config)
        
        # Validate transactional data
        for name, config in self.config.get("transactional_data", {}).items():
            self._validate_transactional_data(name, config)
        
        # Validate references
        self._validate_all_references()
    
    def _validate_master_data(self, name: str, config: Dict[str, Any]) -> None:
        """Validate master data configuration."""
        required_fields = ["kafka_topic"]
        
        for field in required_fields:
            if field not in config:
                raise ValidationError(f"Master data '{name}' missing required field: {field}")
        
        # Validate source-specific requirements
        if config.get("source") == "csv" and "file" not in config:
            raise ValidationError(f"Master data '{name}' with CSV source must specify 'file'")
    
    def _validate_transactional_data(self, name: str, config: Dict[str, Any]) -> None:
        """Validate transactional data configuration."""
        required_fields = ["kafka_topic"]
        
        for field in required_fields:
            if field not in config:
                raise ValidationError(f"Transactional data '{name}' missing required field: {field}")
    
    def _validate_all_references(self) -> None:
        """Validate that all references point to existing master data."""
        # Collect all available references
        available_refs = set()
        
        # From master data
        for name, config in self.config.get("master_data", {}).items():
            id_field = config.get("id_field", "id")
            available_refs.add(f"{name}.{id_field}")
        
        # From transactional data (for temporal relationships)
        for name, config in self.config.get("transactional_data", {}).items():
            # Use the explicitly defined id_field if present
            if "id_field" in config:
                available_refs.add(f"{name}.{config['id_field']}")
            else:
                # Add default ID field pattern with entity_name_id
                available_refs.add(f"{name}.{name}_id")
                
                # Also add singular form (e.g., orders.order_id)
                singular_name = name.rstrip('s')  # Simple singularization
                available_refs.add(f"{name}.{singular_name}_id")
        
        # Validate all relationships
        for name, config in self.config.get("transactional_data", {}).items():
            for field_name, rel_config in config.get("relationships", {}).items():
                if isinstance(rel_config, dict) and "references" in rel_config:
                    ref = rel_config["references"]
                    if ref not in available_refs:
                        raise ValidationError(
                            f"Invalid reference in '{name}.{field_name}': '{ref}' not found. Available: {available_refs}"
                        )
    def has_master_type(self, type_name: str) -> bool:
        """Check if a master data type exists."""
        return type_name in self.config.get("master_data", {})
    
    def has_transaction_type(self, type_name: str) -> bool:
        """Check if a transactional data type exists."""
        return type_name in self.config.get("transactional_data", {})
    
    def get_master_config(self, type_name: str) -> Dict[str, Any]:
        """Get configuration for a master data type."""
        return self.config["master_data"].get(type_name, {})
    
    def get_transaction_config(self, type_name: str) -> Dict[str, Any]:
        """Get configuration for a transactional data type."""
        return self.config["transactional_data"].get(type_name, {})
    
    def get_relationship(self, type_name: str, field_name: str) -> Dict[str, Any]:
        """Get relationship configuration for a specific field."""
        trans_config = self.get_transaction_config(type_name)
        return trans_config.get("relationships", {}).get(field_name, {})
    
    def get_all_topics(self) -> List[str]:
        """Get all Kafka topics defined in the configuration."""
        topics = []
        
        # Master data topics
        for config in self.config.get("master_data", {}).values():
            if "kafka_topic" in config:
                topics.append(config["kafka_topic"])
        
        # Transactional data topics
        for config in self.config.get("transactional_data", {}).values():
            if "kafka_topic" in config:
                topics.append(config["kafka_topic"])
        
        return topics
