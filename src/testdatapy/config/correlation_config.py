"""Configuration loader and validator for correlation-based data generation."""
from typing import Dict, Any, List, Optional
import yaml
import os
import warnings
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
        
        # Validate field conflicts
        self._validate_field_conflicts()
        
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
        
        # Validate protobuf configuration if present
        self._validate_protobuf_config(name, config)
    
    def _validate_transactional_data(self, name: str, config: Dict[str, Any]) -> None:
        """Validate transactional data configuration."""
        required_fields = ["kafka_topic"]
        
        for field in required_fields:
            if field not in config:
                raise ValidationError(f"Transactional data '{name}' missing required field: {field}")
        
        # Validate protobuf configuration if present
        self._validate_protobuf_config(name, config)
    
    def _validate_field_conflicts(self) -> None:
        """Validate field conflicts between auto-generation and derived_fields."""
        for name, config in self.config.get("transactional_data", {}).items():
            # Get id_field (auto-generated unless in relationships)
            id_field = config.get("id_field", f"{name.rstrip('s')}_id")
            
            # Check if id_field would be auto-generated
            relationships = config.get("relationships", {})
            will_auto_generate = id_field not in relationships
            
            # Check if same field is in derived_fields
            derived_fields = config.get("derived_fields", {})
            if will_auto_generate and id_field in derived_fields:
                warnings.warn(
                    f"Field '{id_field}' in entity '{name}' is defined in both auto-generation "
                    f"(id_field) and derived_fields. Derived_fields will override auto-generation.",
                    UserWarning
                )
    
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
    
    def _validate_protobuf_config(self, name: str, config: Dict[str, Any]) -> None:
        """Validate protobuf configuration if present.
        
        Args:
            name: Entity name for error messages
            config: Entity configuration dictionary
            
        Raises:
            ValidationError: If protobuf configuration is invalid
        """
        protobuf_module = config.get('protobuf_module')
        protobuf_class = config.get('protobuf_class')
        schema_path = config.get('schema_path')
        proto_file_path = config.get('proto_file_path')
        
        # If no protobuf config, validation passes
        if not protobuf_module and not protobuf_class:
            return
        
        # Validate required protobuf fields
        if protobuf_module and not protobuf_class:
            raise ValidationError(
                f"Entity '{name}' specifies 'protobuf_module' but missing 'protobuf_class'. "
                f"Both fields are required when using custom protobuf schemas."
            )
        
        if protobuf_class and not protobuf_module:
            raise ValidationError(
                f"Entity '{name}' specifies 'protobuf_class' but missing 'protobuf_module'. "
                f"Both fields are required when using custom protobuf schemas."
            )
        
        # Validate schema_path if provided
        if schema_path:
            self._validate_schema_path(name, schema_path)
        
        # Validate proto_file_path if provided (optional field for schema registration)
        if proto_file_path:
            if not isinstance(proto_file_path, str):
                raise ValidationError(
                    f"Entity '{name}' has invalid 'proto_file_path': must be a string"
                )
    
    def _validate_schema_path(self, name: str, schema_path: str) -> None:
        """Validate schema_path configuration.
        
        Args:
            name: Entity name for error messages
            schema_path: Schema path to validate
            
        Raises:
            ValidationError: If schema path is invalid
        """
        if not isinstance(schema_path, str):
            raise ValidationError(
                f"Entity '{name}' has invalid 'schema_path': must be a string"
            )
        
        if not schema_path.strip():
            raise ValidationError(
                f"Entity '{name}' has empty 'schema_path'"
            )
        
        # Validate path exists and is accessible
        path = Path(schema_path).expanduser()
        
        # Convert to absolute path if relative
        if not path.is_absolute():
            path = Path.cwd() / path
        
        try:
            resolved_path = path.resolve()
        except (OSError, RuntimeError) as e:
            raise ValidationError(
                f"Entity '{name}' has invalid 'schema_path' '{schema_path}': cannot resolve path ({e})"
            )
        
        if not resolved_path.exists():
            raise ValidationError(
                f"Entity '{name}' 'schema_path' does not exist: {resolved_path}"
            )
        
        if not resolved_path.is_dir():
            raise ValidationError(
                f"Entity '{name}' 'schema_path' is not a directory: {resolved_path}"
            )
        
        # Check if directory is readable
        if not os.access(resolved_path, os.R_OK):
            raise ValidationError(
                f"Entity '{name}' 'schema_path' is not readable: {resolved_path}"
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
    
    def get_protobuf_config(self, type_name: str, is_master: bool = False) -> Optional[Dict[str, str]]:
        """Get protobuf configuration for an entity type.
        
        Args:
            type_name: Entity type name
            is_master: Whether this is master data (True) or transactional data (False)
            
        Returns:
            Dictionary with protobuf configuration or None if not configured
        """
        if is_master:
            config = self.get_master_config(type_name)
        else:
            config = self.get_transaction_config(type_name)
        
        protobuf_module = config.get('protobuf_module')
        protobuf_class = config.get('protobuf_class')
        
        if not protobuf_module or not protobuf_class:
            return None
        
        result = {
            'protobuf_module': protobuf_module,
            'protobuf_class': protobuf_class
        }
        
        # Add optional fields if present
        if 'schema_path' in config:
            result['schema_path'] = config['schema_path']
        
        if 'proto_file_path' in config:
            result['proto_file_path'] = config['proto_file_path']
        
        return result
    
    def has_protobuf_config(self, type_name: str, is_master: bool = False) -> bool:
        """Check if an entity type has protobuf configuration.
        
        Args:
            type_name: Entity type name
            is_master: Whether this is master data (True) or transactional data (False)
            
        Returns:
            True if protobuf configuration is present and valid
        """
        return self.get_protobuf_config(type_name, is_master) is not None
    
    def get_key_field(self, type_name: str, is_master: bool = False) -> str:
        """Get the key field for an entity type following priority logic.
        
        Priority:
        1. key_field (explicit key field specification)
        2. id_field (entity identifier field)
        3. Default: {entity_singular}_id
        
        Args:
            type_name: Entity type name
            is_master: Whether this is master data (True) or transactional data (False)
            
        Returns:
            Key field name to use for Kafka message key
        """
        if is_master:
            config = self.get_master_config(type_name)
        else:
            config = self.get_transaction_config(type_name)
        
        # Priority 1: Explicit key_field
        key_field = config.get("key_field")
        if key_field:
            return key_field
        
        # Priority 2: id_field
        id_field = config.get("id_field")
        if id_field:
            return id_field
        
        # Priority 3: Default pattern
        singular_name = type_name.rstrip('s')  # Simple singularization
        return f"{singular_name}_id"
