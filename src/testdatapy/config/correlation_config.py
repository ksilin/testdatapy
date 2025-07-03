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
        
        # Validate protobuf settings if present
        if "protobuf_settings" in self.config:
            self._validate_protobuf_settings()
        
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
        """Validate that all references point to existing master data with nested field support."""
        # Collect all available references
        available_refs = set()
        
        # From master data with nested object support
        for name, config in self.config.get("master_data", {}).items():
            id_field = config.get("id_field", "id")
            available_refs.add(f"{name}.{id_field}")
            
            # Add nested field paths from schema
            schema = config.get("schema", {})
            self._collect_nested_references(available_refs, name, schema, "")
        
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
            
            # Add nested field paths from derived_fields schema
            derived_fields = config.get("derived_fields", {})
            self._collect_nested_references(available_refs, name, derived_fields, "")
        
        # Validate all relationships
        for name, config in self.config.get("transactional_data", {}).items():
            for field_name, rel_config in config.get("relationships", {}).items():
                if isinstance(rel_config, dict) and "references" in rel_config:
                    ref = rel_config["references"]
                    if ref not in available_refs:
                        raise ValidationError(
                            f"Invalid reference in '{name}.{field_name}': '{ref}' not found. Available: {sorted(available_refs)}"
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
    
    def _validate_protobuf_settings(self) -> None:
        """Validate protobuf_settings section.
        
        Raises:
            ValidationError: If protobuf_settings configuration is invalid
        """
        settings = self.config["protobuf_settings"]
        
        if not isinstance(settings, dict):
            raise ValidationError("protobuf_settings must be a dictionary")
        
        # Validate schema_paths
        if "schema_paths" in settings:
            schema_paths = settings["schema_paths"]
            if not isinstance(schema_paths, list):
                raise ValidationError("protobuf_settings.schema_paths must be a list")
            
            for i, path in enumerate(schema_paths):
                if not isinstance(path, str):
                    raise ValidationError(f"protobuf_settings.schema_paths[{i}] must be a string")
                
                # Validate path exists
                try:
                    self._validate_schema_path(f"protobuf_settings.schema_paths[{i}]", path)
                except ValidationError as e:
                    raise ValidationError(f"protobuf_settings.schema_paths[{i}]: {e}")
        
        # Validate boolean fields
        for field in ["auto_compile", "temp_compilation", "validate_dependencies"]:
            if field in settings and not isinstance(settings[field], bool):
                raise ValidationError(f"protobuf_settings.{field} must be a boolean")
        
        # Validate string fields
        for field in ["protoc_path"]:
            if field in settings and not isinstance(settings[field], str):
                raise ValidationError(f"protobuf_settings.{field} must be a string")
        
        # Validate numeric fields
        if "timeout" in settings:
            timeout = settings["timeout"]
            if not isinstance(timeout, (int, float)) or timeout <= 0:
                raise ValidationError("protobuf_settings.timeout must be a positive number")
    
    def get_protobuf_settings(self) -> Optional[Dict[str, Any]]:
        """Get protobuf settings with defaults applied.
        
        Returns:
            Dictionary with protobuf settings or None if not configured
        """
        if "protobuf_settings" not in self.config:
            return None
        
        settings = self.config["protobuf_settings"].copy()
        
        # Apply defaults
        settings.setdefault("auto_compile", False)
        settings.setdefault("temp_compilation", True)
        settings.setdefault("validate_dependencies", False)
        settings.setdefault("schema_paths", [])
        settings.setdefault("timeout", 60)
        
        return settings
    
    def get_merged_protobuf_config(self, type_name: str, is_master: bool = False) -> Dict[str, Any]:
        """Get merged protobuf configuration for an entity.
        
        Merges global protobuf_settings with entity-specific protobuf configuration.
        
        Args:
            type_name: Entity type name
            is_master: Whether this is master data (True) or transactional data (False)
            
        Returns:
            Merged protobuf configuration dictionary
        """
        # Start with global settings
        global_settings = self.get_protobuf_settings() or {}
        merged_config = global_settings.copy()
        
        # Get entity configuration
        if is_master:
            entity_config = self.get_master_config(type_name)
        else:
            entity_config = self.get_transaction_config(type_name)
        
        # Merge entity-specific protobuf settings
        for field in ["protobuf_module", "protobuf_class", "proto_file_path"]:
            if field in entity_config:
                merged_config[field] = entity_config[field]
        
        # Entity schema_path becomes entity_schema_path
        if "schema_path" in entity_config:
            merged_config["entity_schema_path"] = entity_config["schema_path"]
        
        # Entity-level overrides for global settings
        for field in ["auto_compile", "temp_compilation", "validate_dependencies"]:
            if field in entity_config:
                merged_config[field] = entity_config[field]
        
        return merged_config
    
    def get_effective_schema_paths(self, type_name: str, is_master: bool = False) -> List[str]:
        """Get effective schema paths with priority ordering.
        
        Priority order:
        1. Entity-specific schema_path (highest priority)
        2. Global protobuf_settings.schema_paths
        
        Args:
            type_name: Entity type name
            is_master: Whether this is master data (True) or transactional data (False)
            
        Returns:
            List of schema paths in priority order
        """
        paths = []
        
        # Entity-specific path has highest priority
        if is_master:
            entity_config = self.get_master_config(type_name)
        else:
            entity_config = self.get_transaction_config(type_name)
        
        if "schema_path" in entity_config:
            paths.append(entity_config["schema_path"])
        
        # Add global schema paths
        global_settings = self.get_protobuf_settings()
        if global_settings and "schema_paths" in global_settings:
            for path in global_settings["schema_paths"]:
                if path not in paths:  # Avoid duplicates
                    paths.append(path)
        
        return paths
    
    def get_protobuf_dependencies(self, type_name: str, is_master: bool = False) -> List[str]:
        """Get protobuf dependencies for an entity.
        
        Args:
            type_name: Entity type name
            is_master: Whether this is master data (True) or transactional data (False)
            
        Returns:
            List of proto file dependencies
        """
        merged_config = self.get_merged_protobuf_config(type_name, is_master)
        
        # Check if dependency validation is enabled
        if not merged_config.get("validate_dependencies", False):
            return []
        
        # Get proto file path
        proto_file_path = merged_config.get("proto_file_path")
        if not proto_file_path:
            return []
        
        # Get schema paths to search for dependencies
        schema_paths = self.get_effective_schema_paths(type_name, is_master)
        
        # Use SchemaManager to resolve dependencies (if available)
        try:
            from ..schema.manager import SchemaManager
            from ..schema.compiler import ProtobufCompiler
            
            compiler = ProtobufCompiler()
            
            # Find the proto file in schema paths
            proto_file = None
            for schema_path in schema_paths:
                candidate = Path(schema_path) / proto_file_path
                if candidate.exists():
                    proto_file = str(candidate)
                    break
            
            if not proto_file:
                return []
            
            # Resolve dependencies
            dependencies = compiler.resolve_dependencies(proto_file, schema_paths)
            return dependencies
            
        except ImportError:
            # SchemaManager not available, return empty list
            return []
    
    def merge_with_app_config(self, app_config) -> Dict[str, Any]:
        """Merge correlation protobuf settings with app config.
        
        Args:
            app_config: AppConfig instance with protobuf settings
            
        Returns:
            Merged protobuf configuration
        """
        merged_config = {}
        
        # Start with app config protobuf settings
        if hasattr(app_config, 'protobuf'):
            app_protobuf_dict = app_config.to_protobuf_config()
            merged_config.update(app_protobuf_dict)
        
        # Override with correlation config settings
        correlation_settings = self.get_protobuf_settings()
        if correlation_settings:
            # Merge schema_paths (combine, don't override)
            app_paths = merged_config.get("schema_paths", [])
            corr_paths = correlation_settings.get("schema_paths", [])
            
            # Correlation paths have higher priority
            combined_paths = corr_paths.copy()
            for path in app_paths:
                if path not in combined_paths:
                    combined_paths.append(path)
            
            merged_config.update(correlation_settings)
            merged_config["schema_paths"] = combined_paths
        
        return merged_config
    
    def get_final_protobuf_config(self, type_name: str, is_master: bool = False, app_config=None) -> Dict[str, Any]:
        """Get final protobuf configuration with all precedence rules applied.
        
        Precedence order (highest to lowest):
        1. Entity-specific settings
        2. Correlation config protobuf_settings
        3. App config protobuf settings
        4. Defaults
        
        Args:
            type_name: Entity type name
            is_master: Whether this is master data (True) or transactional data (False)
            app_config: Optional AppConfig instance
            
        Returns:
            Final protobuf configuration
        """
        # Start with app config (if provided)
        if app_config and hasattr(app_config, 'protobuf'):
            final_config = app_config.to_protobuf_config()
        else:
            final_config = {
                "auto_compile": False,
                "temp_compilation": True,
                "schema_paths": [],
                "timeout": 60
            }
        
        # Apply correlation config settings
        correlation_settings = self.get_protobuf_settings()
        if correlation_settings:
            # Merge schema_paths
            app_paths = final_config.get("schema_paths", [])
            corr_paths = correlation_settings.get("schema_paths", [])
            
            combined_paths = corr_paths.copy()
            for path in app_paths:
                if path not in combined_paths:
                    combined_paths.append(path)
            
            final_config.update(correlation_settings)
            final_config["schema_paths"] = combined_paths
        
        # Apply entity-specific settings (highest precedence)
        entity_config = self.get_merged_protobuf_config(type_name, is_master)
        
        # Entity schema path gets highest priority in schema_paths list
        if "entity_schema_path" in entity_config:
            schema_paths = final_config.get("schema_paths", [])
            entity_path = entity_config["entity_schema_path"]
            
            # Remove entity path if already in list, then add at beginning
            if entity_path in schema_paths:
                schema_paths.remove(entity_path)
            schema_paths.insert(0, entity_path)
            
            final_config["schema_paths"] = schema_paths
        
        # Override with entity settings
        for field in ["auto_compile", "temp_compilation", "protobuf_module", "protobuf_class", "proto_file_path"]:
            if field in entity_config:
                final_config[field] = entity_config[field]
        
        return final_config
    
    def validate_protobuf_config_complete(self) -> Dict[str, Any]:
        """Validate complete protobuf configuration.
        
        Returns:
            Dictionary with validation results including warnings and errors
        """
        result = {
            "valid": True,
            "warnings": [],
            "errors": []
        }
        
        try:
            # Check if protobuf_settings is configured
            if not self.get_protobuf_settings():
                result["warnings"].append("No protobuf_settings configured")
                return result
            
            # Validate schema paths exist
            settings = self.get_protobuf_settings()
            for path in settings.get("schema_paths", []):
                if not Path(path).exists():
                    result["errors"].append(f"Schema path does not exist: {path}")
            
            # Validate entities with protobuf configuration
            for section, is_master in [("master_data", True), ("transactional_data", False)]:
                for entity_name, entity_config in self.config.get(section, {}).items():
                    if self.has_protobuf_config(entity_name, is_master):
                        # Validate entity protobuf config
                        try:
                            merged_config = self.get_merged_protobuf_config(entity_name, is_master)
                            
                            # Check required fields
                            if not merged_config.get("protobuf_module"):
                                result["errors"].append(f"Entity {entity_name} missing protobuf_module")
                            if not merged_config.get("protobuf_class"):
                                result["errors"].append(f"Entity {entity_name} missing protobuf_class")
                            
                        except Exception as e:
                            result["errors"].append(f"Entity {entity_name} protobuf config error: {e}")
            
            if result["errors"]:
                result["valid"] = False
                
        except Exception as e:
            result["valid"] = False
            result["errors"].append(f"Configuration validation error: {e}")
        
        return result
    
    def get_protobuf_compilation_order(self) -> List[str]:
        """Get protobuf files in compilation order based on dependencies.
        
        Returns:
            List of proto file paths in compilation order
        """
        all_files = []
        
        # Collect all proto files from entities
        for section, is_master in [("master_data", True), ("transactional_data", False)]:
            for entity_name in self.config.get(section, {}):
                if self.has_protobuf_config(entity_name, is_master):
                    dependencies = self.get_protobuf_dependencies(entity_name, is_master)
                    all_files.extend(dependencies)
                    
                    # Add the main proto file
                    merged_config = self.get_merged_protobuf_config(entity_name, is_master)
                    proto_file_path = merged_config.get("proto_file_path")
                    if proto_file_path:
                        schema_paths = self.get_effective_schema_paths(entity_name, is_master)
                        for schema_path in schema_paths:
                            candidate = Path(schema_path) / proto_file_path
                            if candidate.exists():
                                all_files.append(str(candidate))
                                break
        
        # Remove duplicates while preserving order
        seen = set()
        compilation_order = []
        for file_path in all_files:
            if file_path not in seen:
                seen.add(file_path)
                compilation_order.append(file_path)
        
        return compilation_order
    
    def _collect_nested_references(self, available_refs: set, entity_name: str, schema: Dict[str, Any], path_prefix: str) -> None:
        """Collect nested field references from object schemas."""
        for field_name, field_config in schema.items():
            current_path = f"{path_prefix}.{field_name}" if path_prefix else field_name
            full_reference = f"{entity_name}.{current_path}"
            
            # Add this field as a reference
            available_refs.add(full_reference)
            
            # If this is an object type, recursively collect nested fields
            if isinstance(field_config, dict):
                field_type = field_config.get("type")
                if field_type == "object":
                    properties = field_config.get("properties", {})
                    self._collect_nested_references(available_refs, entity_name, properties, current_path)
    
    def validate_vehicle_field_types(self) -> Dict[str, Any]:
        """Validate vehicle-specific field types and configurations."""
        result = {
            "valid": True,
            "warnings": [],
            "errors": []
        }
        
        # Supported vehicle field types
        supported_types = {
            "string", "object", "faker", "uuid", "integer", "float", "choice", 
            "weighted_choice", "timestamp", "timestamp_millis", "conditional", 
            "template", "reference", "random_boolean"
        }
        
        # Check all entities for unsupported field types
        for section_name, section_data in [("master_data", self.config.get("master_data", {})), 
                                          ("transactional_data", self.config.get("transactional_data", {}))]:
            for entity_name, entity_config in section_data.items():
                # Check schema fields
                schema = entity_config.get("schema", {})
                self._validate_field_types_recursive(schema, f"{section_name}.{entity_name}.schema", 
                                                   supported_types, result)
                
                # Check derived_fields
                derived_fields = entity_config.get("derived_fields", {})
                self._validate_field_types_recursive(derived_fields, f"{section_name}.{entity_name}.derived_fields", 
                                                   supported_types, result)
        
        return result
    
    def _validate_field_types_recursive(self, schema: Dict[str, Any], path: str, 
                                      supported_types: set, result: Dict[str, Any]) -> None:
        """Recursively validate field types in nested schemas."""
        for field_name, field_config in schema.items():
            field_path = f"{path}.{field_name}"
            
            if isinstance(field_config, dict):
                field_type = field_config.get("type")
                
                if field_type and field_type not in supported_types:
                    result["errors"].append(f"Unsupported field type '{field_type}' at {field_path}")
                    result["valid"] = False
                
                # Vehicle-specific validations
                if field_type == "conditional":
                    self._validate_conditional_field(field_config, field_path, result)
                elif field_type == "template":
                    self._validate_template_field(field_config, field_path, result)
                elif field_type == "reference":
                    self._validate_reference_field(field_config, field_path, result)
                elif field_type == "weighted_choice":
                    self._validate_weighted_choice_field(field_config, field_path, result)
                elif field_type == "object":
                    # Recursively validate nested object properties
                    properties = field_config.get("properties", {})
                    self._validate_field_types_recursive(properties, field_path, supported_types, result)
    
    def _validate_conditional_field(self, field_config: Dict[str, Any], field_path: str, result: Dict[str, Any]) -> None:
        """Validate conditional field configuration."""
        condition_field = field_config.get("condition_field")
        condition_value = field_config.get("condition_value")
        when_true = field_config.get("when_true")
        when_false = field_config.get("when_false")
        
        if not condition_field:
            result["errors"].append(f"Conditional field at {field_path} missing 'condition_field'")
            result["valid"] = False
        
        if "condition_value" not in field_config:
            result["errors"].append(f"Conditional field at {field_path} missing 'condition_value'")
            result["valid"] = False
        
        if not when_true and not when_false:
            result["warnings"].append(f"Conditional field at {field_path} has no 'when_true' or 'when_false' actions")
    
    def _validate_template_field(self, field_config: Dict[str, Any], field_path: str, result: Dict[str, Any]) -> None:
        """Validate template field configuration."""
        template = field_config.get("template")
        fields = field_config.get("fields", {})
        
        if not template:
            result["errors"].append(f"Template field at {field_path} missing 'template' string")
            result["valid"] = False
            return
        
        # Check if template variables have corresponding field definitions
        import re
        variables = re.findall(r'\{([^}]+)\}', template)
        for var_name in variables:
            if var_name not in fields:
                result["warnings"].append(f"Template field at {field_path} references undefined variable '{var_name}'")
    
    def _validate_reference_field(self, field_config: Dict[str, Any], field_path: str, result: Dict[str, Any]) -> None:
        """Validate reference field configuration."""
        source = field_config.get("source")
        via = field_config.get("via")
        
        if not source:
            result["errors"].append(f"Reference field at {field_path} missing 'source'")
            result["valid"] = False
        
        # Pattern validation
        if source and "." not in source and not source.startswith("self."):
            result["warnings"].append(f"Reference field at {field_path} source '{source}' should use dot notation (e.g., 'entity.field')")
    
    def _validate_weighted_choice_field(self, field_config: Dict[str, Any], field_path: str, result: Dict[str, Any]) -> None:
        """Validate weighted choice field configuration."""
        choices = field_config.get("choices", [])
        weights = field_config.get("weights", [])
        
        if not choices:
            result["errors"].append(f"Weighted choice field at {field_path} missing 'choices'")
            result["valid"] = False
            return
        
        if weights and len(weights) != len(choices):
            result["errors"].append(f"Weighted choice field at {field_path} weights count ({len(weights)}) doesn't match choices count ({len(choices)})")
            result["valid"] = False
        
        if weights:
            # Check for negative weights
            for i, weight in enumerate(weights):
                if not isinstance(weight, (int, float)) or weight < 0:
                    result["errors"].append(f"Weighted choice field at {field_path} weight[{i}] must be a non-negative number")
                    result["valid"] = False
    
    def get_vehicle_specific_validation(self) -> Dict[str, Any]:
        """Get comprehensive vehicle-specific validation results."""
        result = {
            "valid": True,
            "warnings": [],
            "errors": [],
            "field_type_validation": {},
            "reference_validation": {}
        }
        
        # Validate vehicle field types
        field_validation = self.validate_vehicle_field_types()
        result["field_type_validation"] = field_validation
        result["warnings"].extend(field_validation["warnings"])
        result["errors"].extend(field_validation["errors"])
        
        if not field_validation["valid"]:
            result["valid"] = False
        
        return result
