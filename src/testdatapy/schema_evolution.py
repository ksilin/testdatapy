"""Schema evolution support for TestDataPy."""
import json
from typing import Any

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient


class SchemaEvolutionManager:
    """Manage schema evolution for Avro messages."""
    
    def __init__(self, schema_registry_url: str, config: dict[str, Any] | None = None):
        """Initialize schema evolution manager.
        
        Args:
            schema_registry_url: Schema Registry URL
            config: Additional Schema Registry configuration
        """
        sr_config = {"url": schema_registry_url}
        if config:
            sr_config.update(config)
        
        self.client = SchemaRegistryClient(sr_config)
        self._cache = {}
    
    def register_schema(self, subject: str, schema_str: str) -> int:
        """Register a new schema version.
        
        Args:
            subject: Schema subject
            schema_str: Avro schema as string
            
        Returns:
            Schema ID
        """
        schema = Schema(schema_str, schema_type="AVRO")
        return self.client.register_schema(subject_name=subject, schema=schema)
    
    def get_latest_schema(self, subject: str) -> dict[str, Any] | None:
        """Get the latest schema for a subject.
        
        Args:
            subject: Schema subject
            
        Returns:
            Schema as dictionary or None
        """
        try:
            version = self.client.get_latest_version(subject)
            schema_str = version.schema.schema_str
            return json.loads(schema_str)
        except Exception:
            return None
    
    def get_schema_by_id(self, schema_id: int) -> dict[str, Any] | None:
        """Get schema by ID.
        
        Args:
            schema_id: Schema ID
            
        Returns:
            Schema as dictionary or None
        """
        if schema_id in self._cache:
            return self._cache[schema_id]
        
        try:
            schema = self.client.get_schema(schema_id)
            schema_dict = json.loads(schema.schema_str)
            self._cache[schema_id] = schema_dict
            return schema_dict
        except Exception:
            return None
    
    def get_all_versions(self, subject: str) -> list[int]:
        """Get all version IDs for a subject.
        
        Args:
            subject: Schema subject
            
        Returns:
            List of version IDs
        """
        try:
            return self.client.get_versions(subject)
        except Exception:
            return []
    
    def check_compatibility(self, subject: str, schema_str: str) -> bool:
        """Check if schema is compatible with existing versions.
        
        Args:
            subject: Schema subject
            schema_str: New schema to check
            
        Returns:
            True if compatible
        """
        try:
            schema = Schema(schema_str, schema_type="AVRO")
            result = self.client.test_compatibility(
                subject_name=subject,
                schema=schema
            )
            return result
        except Exception:
            return False
    
    def evolve_schema(
        self, 
        current_schema: dict[str, Any],
        changes: dict[str, Any]
    ) -> dict[str, Any]:
        """Evolve a schema with backward-compatible changes.
        
        Args:
            current_schema: Current schema
            changes: Changes to apply
            
        Returns:
            Evolved schema
        """
        evolved = current_schema.copy()
        
        # Add new optional fields
        if "add_fields" in changes:
            for field in changes["add_fields"]:
                # Ensure new fields have defaults for backward compatibility
                if "default" not in field:
                    if field["type"] == "string":
                        field["default"] = ""
                    elif field["type"] == "int":
                        field["default"] = 0
                    elif field["type"] == "boolean":
                        field["default"] = False
                    else:
                        field["default"] = None
                
                evolved["fields"].append(field)
        
        # Make fields optional
        if "make_optional" in changes:
            for field_name in changes["make_optional"]:
                for field in evolved["fields"]:
                    if field["name"] == field_name:
                        # Convert to union with null
                        if not isinstance(field["type"], list):
                            field["type"] = ["null", field["type"]]
                            field["default"] = None
        
        # Add aliases for renamed fields
        if "rename_fields" in changes:
            for old_name, new_name in changes["rename_fields"].items():
                for field in evolved["fields"]:
                    if field["name"] == old_name:
                        field["name"] = new_name
                        field["aliases"] = field.get("aliases", [])
                        field["aliases"].append(old_name)
        
        return evolved
    
    def migrate_data(
        self,
        data: dict[str, Any],
        from_schema: dict[str, Any],
        to_schema: dict[str, Any]
    ) -> dict[str, Any]:
        """Migrate data from one schema version to another.
        
        Args:
            data: Data to migrate
            from_schema: Source schema
            to_schema: Target schema
            
        Returns:
            Migrated data
        """
        migrated = {}
        
        # Get field mappings
        to_fields = {f["name"]: f for f in to_schema["fields"]}
        
        # Handle field aliases
        aliases = {}
        for field in to_schema["fields"]:
            if "aliases" in field:
                for alias in field["aliases"]:
                    aliases[alias] = field["name"]
        
        # Migrate fields
        for field_name, field_def in to_fields.items():
            # Direct mapping
            if field_name in data:
                migrated[field_name] = data[field_name]
            # Check aliases
            elif field_name in aliases and aliases[field_name] in data:
                migrated[field_name] = data[aliases[field_name]]
            # Use default
            elif "default" in field_def:
                migrated[field_name] = field_def["default"]
            # Required field missing
            elif not self._is_optional(field_def):
                raise ValueError(f"Required field {field_name} missing in data")
        
        return migrated
    
    def _is_optional(self, field_def: dict[str, Any]) -> bool:
        """Check if a field is optional.
        
        Args:
            field_def: Field definition
            
        Returns:
            True if field is optional
        """
        field_type = field_def["type"]
        if isinstance(field_type, list) and "null" in field_type:
            return True
        return "default" in field_def
    
    def generate_migration_plan(
        self,
        from_version: int,
        to_version: int,
        subject: str
    ) -> list[dict[str, Any]]:
        """Generate a migration plan between schema versions.
        
        Args:
            from_version: Source version
            to_version: Target version
            subject: Schema subject
            
        Returns:
            List of migration steps
        """
        steps = []
        
        # Get all versions between from and to
        all_versions = self.get_all_versions(subject)
        versions_to_migrate = [
            v for v in all_versions 
            if from_version <= v <= to_version
        ]
        
        # Create migration steps
        for i in range(len(versions_to_migrate) - 1):
            current_version = versions_to_migrate[i]
            next_version = versions_to_migrate[i + 1]
            
            current_schema = self.get_schema_by_id(current_version)
            next_schema = self.get_schema_by_id(next_version)
            
            # Analyze changes
            changes = self._analyze_schema_changes(current_schema, next_schema)
            
            steps.append({
                "from_version": current_version,
                "to_version": next_version,
                "changes": changes
            })
        
        return steps
    
    def _analyze_schema_changes(
        self,
        old_schema: dict[str, Any],
        new_schema: dict[str, Any]
    ) -> dict[str, Any]:
        """Analyze changes between two schema versions.
        
        Args:
            old_schema: Old schema version
            new_schema: New schema version
            
        Returns:
            Dictionary describing changes
        """
        changes = {
            "added_fields": [],
            "removed_fields": [],
            "modified_fields": [],
            "renamed_fields": {}
        }
        
        old_fields = {f["name"]: f for f in old_schema["fields"]}
        new_fields = {f["name"]: f for f in new_schema["fields"]}
        
        # Find added fields
        for name, field in new_fields.items():
            if name not in old_fields:
                # Check if it's a rename by looking at aliases
                if "aliases" in field:
                    for alias in field["aliases"]:
                        if alias in old_fields:
                            changes["renamed_fields"][alias] = name
                            break
                else:
                    changes["added_fields"].append(field)
        
        # Find removed fields
        for name, field in old_fields.items():
            if name not in new_fields:
                # Check if it was renamed
                renamed = False
                for new_name, new_field in new_fields.items():
                    if "aliases" in new_field and name in new_field["aliases"]:
                        renamed = True
                        break
                
                if not renamed:
                    changes["removed_fields"].append(field)
        
        # Find modified fields
        for name, new_field in new_fields.items():
            if name in old_fields:
                old_field = old_fields[name]
                if old_field != new_field:
                    changes["modified_fields"].append({
                        "name": name,
                        "old": old_field,
                        "new": new_field
                    })
        
        return changes
