"""Enhanced Schema Registry integration for TestDataPy.

This module provides comprehensive Schema Registry support including:
- Automatic protobuf schema registration from .proto files
- Subject naming conventions and versioning
- Schema compatibility checking
- Local schema caching for offline development
"""

import json
import logging
import os
import pickle
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer

from .compiler import ProtobufCompiler
from .exceptions import (
    SchemaRegistrationError, 
    SchemaNotFoundError, 
    ValidationError,
    CompilationError,
    SchemaCompatibilityError
)
from ..logging_config import get_schema_logger, PerformanceTimer


class SchemaRegistryManager:
    """Enhanced Schema Registry manager with automatic protobuf support."""
    
    def __init__(
        self, 
        schema_registry_url: str, 
        config: Optional[Dict[str, Any]] = None,
        auto_register: bool = True,
        subject_naming_strategy: str = "topic_name",
        cache_dir: Optional[Union[str, Path]] = None,
        cache_ttl_hours: int = 24,
        enable_persistent_cache: bool = True
    ):
        """Initialize Schema Registry manager.
        
        Args:
            schema_registry_url: Schema Registry URL
            config: Additional Schema Registry configuration
            auto_register: Whether to auto-register schemas
            subject_naming_strategy: Strategy for subject names ("topic_name", "record_name", "topic_record_name")
            cache_dir: Directory for persistent cache (default: ~/.testdatapy/schema_cache)
            cache_ttl_hours: Cache time-to-live in hours
            enable_persistent_cache: Whether to enable persistent caching
        """
        self._logger = get_schema_logger(__name__)
        self.schema_registry_url = schema_registry_url
        self.auto_register = auto_register
        self.subject_naming_strategy = subject_naming_strategy
        
        # Set up Schema Registry client
        sr_config = {"url": schema_registry_url}
        if config:
            sr_config.update(config)
        
        try:
            with PerformanceTimer(self._logger, "schema_registry_connection", url=schema_registry_url):
                self.client = SchemaRegistryClient(sr_config)
                # Test connection
                self.client.get_subjects()
                self._logger.info("Connected to Schema Registry", url=schema_registry_url)
        except Exception as e:
            self._logger.error("Failed to connect to Schema Registry", 
                             url=schema_registry_url, error=str(e))
            raise ValidationError(f"Cannot connect to Schema Registry at {schema_registry_url}: {e}")
        
        # Initialize protobuf compiler for schema compilation
        self.compiler = ProtobufCompiler()
        
        # Configure caching
        self.enable_persistent_cache = enable_persistent_cache
        self.cache_ttl_hours = cache_ttl_hours
        self.cache_ttl_seconds = cache_ttl_hours * 3600
        
        # Set up cache directory
        if cache_dir:
            self.cache_dir = Path(cache_dir)
        else:
            self.cache_dir = Path.home() / ".testdatapy" / "schema_cache"
        
        if self.enable_persistent_cache:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            self._logger.info("Persistent schema cache enabled", 
                             cache_dir=str(self.cache_dir),
                             ttl_hours=cache_ttl_hours)
        
        # In-memory cache for current session
        self._schema_cache: Dict[str, Dict[str, Any]] = {}
        
        # Load persistent cache if enabled
        if self.enable_persistent_cache:
            self._load_persistent_cache()
        
        self._logger.info("SchemaRegistryManager initialized", 
                         url=schema_registry_url,
                         auto_register=auto_register,
                         subject_naming_strategy=subject_naming_strategy)
    
    def register_protobuf_schema_from_file(
        self, 
        proto_file_path: Union[str, Path], 
        topic: str,
        message_name: Optional[str] = None,
        schema_paths: Optional[List[str]] = None,
        force_register: bool = False
    ) -> Dict[str, Any]:
        """Automatically register protobuf schema from .proto file.
        
        Args:
            proto_file_path: Path to .proto file
            topic: Kafka topic name
            message_name: Specific message name (if proto has multiple messages)
            schema_paths: Additional paths for proto compilation
            force_register: Force registration even if schema exists
            
        Returns:
            Dictionary with registration results
        """
        start_time = time.time()
        proto_file = Path(proto_file_path)
        
        self._logger.info("Starting automatic schema registration", 
                         proto_file=str(proto_file), topic=topic, message_name=message_name)
        
        if not proto_file.exists():
            raise SchemaNotFoundError(
                schema_path=str(proto_file),
                schema_type="protobuf schema",
                search_paths=schema_paths or []
            )
        
        try:
            # Step 1: Compile proto file to validate it
            with PerformanceTimer(self._logger, "proto_validation", proto_file=str(proto_file)):
                self.compiler.validate_proto_syntax(str(proto_file))
            
            # Step 2: Read proto file content
            with open(proto_file, 'r', encoding='utf-8') as f:
                proto_content = f.read()
            
            # Step 3: Extract schema information
            schema_info = self._extract_schema_info(proto_content, message_name)
            
            # Step 4: Generate subject name
            subject = self._generate_subject_name(topic, schema_info.get("package"), schema_info.get("message_name"))
            
            # Step 5: Check if schema already exists (unless force_register)
            if not force_register:
                existing_schema = self._get_existing_schema(subject)
                if existing_schema and self._are_schemas_equivalent(proto_content, existing_schema):
                    self._logger.info("Schema already exists and is equivalent", 
                                    subject=subject, 
                                    existing_version=existing_schema.get("version"))
                    return {
                        "action": "already_exists",
                        "subject": subject,
                        "schema_id": existing_schema.get("id"),
                        "version": existing_schema.get("version"),
                        "duration": time.time() - start_time
                    }
            
            # Step 6: Register the schema
            registration_result = self._register_schema(subject, proto_content, schema_info)
            
            # Step 7: Cache the schema locally
            self._cache_schema(subject, {
                "proto_content": proto_content,
                "schema_info": schema_info,
                "registration_result": registration_result,
                "topic": topic,
                "proto_file": str(proto_file)
            })
            
            duration = time.time() - start_time
            self._logger.log_schema_registry_operation(
                operation="auto_register",
                subject=subject,
                success=True,
                duration=duration,
                schema_id=registration_result.get("schema_id")
            )
            
            result = {
                "action": "registered",
                "subject": subject,
                "schema_id": registration_result["schema_id"],
                "version": registration_result.get("version"),
                "schema_info": schema_info,
                "proto_file": str(proto_file),
                "duration": duration
            }
            
            self._logger.info("Automatic schema registration completed", **result)
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            self._logger.log_schema_registry_operation(
                operation="auto_register",
                subject=subject if 'subject' in locals() else "unknown",
                success=False,
                duration=duration,
                error=str(e)
            )
            
            if isinstance(e, (SchemaRegistrationError, SchemaNotFoundError, ValidationError, CompilationError)):
                raise
            else:
                raise SchemaRegistrationError(
                    message=f"Failed to register protobuf schema: {e}",
                    subject=subject if 'subject' in locals() else "unknown",
                    schema_content=proto_content[:200] + "..." if 'proto_content' in locals() and len(proto_content) > 200 else "",
                    original_error=e
                )
    
    def register_protobuf_schemas_from_directory(
        self, 
        proto_dir: Union[str, Path],
        topic_mapping: Optional[Dict[str, str]] = None,
        recursive: bool = True,
        schema_paths: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Register all protobuf schemas from a directory.
        
        Args:
            proto_dir: Directory containing .proto files
            topic_mapping: Mapping of proto file names to topic names
            recursive: Whether to search recursively
            schema_paths: Additional paths for proto compilation
            
        Returns:
            Dictionary mapping proto files to registration results
        """
        proto_dir = Path(proto_dir)
        if not proto_dir.exists() or not proto_dir.is_dir():
            raise ValidationError(f"Proto directory does not exist or is not a directory: {proto_dir}")
        
        self._logger.info("Starting batch schema registration", 
                         proto_dir=str(proto_dir), recursive=recursive)
        
        # Discover proto files
        if recursive:
            proto_files = list(proto_dir.rglob("*.proto"))
        else:
            proto_files = list(proto_dir.glob("*.proto"))
        
        if not proto_files:
            self._logger.warning("No proto files found", proto_dir=str(proto_dir))
            return {}
        
        results = {}
        successful_registrations = 0
        failed_registrations = 0
        
        for proto_file in proto_files:
            try:
                # Determine topic name
                proto_name = proto_file.stem
                topic = topic_mapping.get(proto_name, proto_name) if topic_mapping else proto_name
                
                # Register schema
                result = self.register_protobuf_schema_from_file(
                    proto_file, topic, schema_paths=schema_paths
                )
                results[str(proto_file)] = result
                
                if result["action"] in ["registered", "already_exists"]:
                    successful_registrations += 1
                
            except Exception as e:
                self._logger.error("Failed to register schema", 
                                 proto_file=str(proto_file), error=str(e))
                results[str(proto_file)] = {
                    "action": "failed",
                    "error": str(e),
                    "proto_file": str(proto_file)
                }
                failed_registrations += 1
        
        self._logger.info("Batch schema registration completed", 
                         total_files=len(proto_files),
                         successful=successful_registrations,
                         failed=failed_registrations)
        
        return results
    
    def _extract_schema_info(self, proto_content: str, message_name: Optional[str] = None) -> Dict[str, Any]:
        """Extract schema information from proto content.
        
        Args:
            proto_content: Proto file content
            message_name: Specific message name to extract
            
        Returns:
            Dictionary with schema information
        """
        import re
        
        schema_info = {}
        
        # Extract package name
        package_match = re.search(r'package\s+([a-zA-Z0-9_.]+)\s*;', proto_content)
        if package_match:
            schema_info["package"] = package_match.group(1)
        
        # Extract syntax version
        syntax_match = re.search(r'syntax\s*=\s*["\']([^"\']+)["\']', proto_content)
        if syntax_match:
            schema_info["syntax"] = syntax_match.group(1)
        
        # Extract message definitions
        message_pattern = r'message\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\{'
        messages = re.findall(message_pattern, proto_content)
        schema_info["messages"] = messages
        
        # Determine main message
        if message_name:
            if message_name in messages:
                schema_info["message_name"] = message_name
            else:
                raise ValidationError(f"Message '{message_name}' not found in proto file")
        elif messages:
            # Use first message as default
            schema_info["message_name"] = messages[0]
        else:
            raise ValidationError("No message definitions found in proto file")
        
        # Extract imports
        import_pattern = r'import\s+["\']([^"\']+)["\']'
        imports = re.findall(import_pattern, proto_content)
        schema_info["imports"] = imports
        
        self._logger.debug("Extracted schema info", **schema_info)
        return schema_info
    
    def _generate_subject_name(self, topic: str, package: Optional[str] = None, message_name: Optional[str] = None) -> str:
        """Generate subject name based on naming strategy.
        
        Args:
            topic: Kafka topic name
            package: Protobuf package name
            message_name: Message name
            
        Returns:
            Subject name
        """
        if self.subject_naming_strategy == "topic_name":
            subject = f"{topic}-value"
        elif self.subject_naming_strategy == "record_name" and message_name:
            subject = message_name
        elif self.subject_naming_strategy == "topic_record_name" and message_name:
            subject = f"{topic}-{message_name}"
        else:
            # Default fallback
            subject = f"{topic}-value"
        
        self._logger.debug("Generated subject name", 
                         subject=subject, topic=topic, 
                         strategy=self.subject_naming_strategy,
                         message_name=message_name)
        return subject
    
    def _get_existing_schema(self, subject: str) -> Optional[Dict[str, Any]]:
        """Get existing schema for subject.
        
        Args:
            subject: Schema subject
            
        Returns:
            Schema information or None if not found
        """
        try:
            version = self.client.get_latest_version(subject)
            return {
                "id": version.schema_id,
                "version": version.version,
                "schema": version.schema.schema_str,
                "schema_type": version.schema.schema_type
            }
        except Exception:
            return None
    
    def _are_schemas_equivalent(self, new_schema: str, existing_schema: Dict[str, Any]) -> bool:
        """Check if two schemas are equivalent.
        
        Args:
            new_schema: New schema content
            existing_schema: Existing schema information
            
        Returns:
            True if schemas are equivalent
        """
        if existing_schema.get("schema_type") != "PROTOBUF":
            return False
        
        # Simple content comparison (could be enhanced with semantic comparison)
        existing_content = existing_schema.get("schema", "")
        
        # Normalize whitespace for comparison
        new_normalized = ' '.join(new_schema.split())
        existing_normalized = ' '.join(existing_content.split())
        
        return new_normalized == existing_normalized
    
    def _register_schema(self, subject: str, proto_content: str, schema_info: Dict[str, Any]) -> Dict[str, Any]:
        """Register schema with Schema Registry.
        
        Args:
            subject: Subject name
            proto_content: Proto file content
            schema_info: Schema information
            
        Returns:
            Registration result
        """
        try:
            schema = Schema(proto_content, schema_type="PROTOBUF")
            
            with PerformanceTimer(self._logger, "schema_registration", subject=subject):
                schema_id = self.client.register_schema(subject, schema)
            
            # Get version information
            try:
                version_info = self.client.get_latest_version(subject)
                version = version_info.version
            except Exception:
                version = None
            
            return {
                "schema_id": schema_id,
                "version": version,
                "subject": subject,
                "schema_type": "PROTOBUF"
            }
            
        except Exception as e:
            raise SchemaRegistrationError(
                message=f"Failed to register schema: {e}",
                subject=subject,
                schema_content=proto_content[:200] + "..." if len(proto_content) > 200 else proto_content,
                original_error=e
            )
    
    def _cache_schema(self, subject: str, schema_data: Dict[str, Any]) -> None:
        """Cache schema locally for offline development.
        
        Args:
            subject: Schema subject
            schema_data: Schema data to cache
        """
        cached_entry = {
            **schema_data,
            "cached_at": time.time()
        }
        
        self._schema_cache[subject] = cached_entry
        
        # Save to persistent cache if enabled
        if self.enable_persistent_cache:
            self._save_persistent_cache()
        
        self._logger.debug("Cached schema locally", 
                         subject=subject, 
                         cache_size=len(self._schema_cache),
                         persistent=self.enable_persistent_cache)
    
    def get_cached_schema(self, subject: str) -> Optional[Dict[str, Any]]:
        """Get schema from local cache.
        
        Args:
            subject: Schema subject
            
        Returns:
            Cached schema data or None
        """
        cached_entry = self._schema_cache.get(subject)
        
        if cached_entry and self._is_cache_entry_valid(cached_entry):
            return cached_entry
        elif cached_entry:
            # Remove expired entry
            del self._schema_cache[subject]
            if self.enable_persistent_cache:
                self._save_persistent_cache()
            self._logger.debug("Removed expired cache entry", subject=subject)
        
        return None
    
    def list_subjects(self, pattern: Optional[str] = None) -> List[str]:
        """List all subjects in Schema Registry.
        
        Args:
            pattern: Optional pattern to filter subjects
            
        Returns:
            List of subject names
        """
        try:
            subjects = self.client.get_subjects()
            
            if pattern:
                import fnmatch
                subjects = [s for s in subjects if fnmatch.fnmatch(s, pattern)]
            
            return subjects
            
        except Exception as e:
            self._logger.error("Failed to list subjects", error=str(e))
            return []
    
    def get_schema_metadata(self, subject: str) -> Dict[str, Any]:
        """Get comprehensive metadata for a schema.
        
        Args:
            subject: Schema subject
            
        Returns:
            Schema metadata
        """
        try:
            # Get latest version
            latest_version = self.client.get_latest_version(subject)
            
            # Get all versions
            versions = self.client.get_versions(subject)
            
            # Get cached info if available
            cached_info = self.get_cached_schema(subject)
            
            metadata = {
                "subject": subject,
                "latest_version": latest_version.version,
                "latest_schema_id": latest_version.schema_id,
                "schema_type": latest_version.schema.schema_type,
                "all_versions": versions,
                "total_versions": len(versions),
                "schema_content": latest_version.schema.schema_str,
                "cached_locally": cached_info is not None
            }
            
            if cached_info:
                metadata["cache_info"] = {
                    "topic": cached_info.get("topic"),
                    "proto_file": cached_info.get("proto_file"),
                    "cached_at": cached_info.get("cached_at")
                }
            
            return metadata
            
        except Exception as e:
            self._logger.error("Failed to get schema metadata", 
                             subject=subject, error=str(e))
            raise ValidationError(f"Cannot get metadata for subject {subject}: {e}")
    
    def clear_cache(self, subject: Optional[str] = None) -> None:
        """Clear local schema cache.
        
        Args:
            subject: Optional specific subject to clear, or None for all
        """
        if subject:
            if subject in self._schema_cache:
                del self._schema_cache[subject]
                if self.enable_persistent_cache:
                    self._save_persistent_cache()
                self._logger.info("Cleared cache for subject", subject=subject)
        else:
            self._schema_cache.clear()
            if self.enable_persistent_cache:
                self._clear_persistent_cache()
            self._logger.info("Cleared all schema cache")
    
    def check_schema_compatibility(
        self, 
        subject: str, 
        new_schema_content: str,
        compatibility_level: Optional[str] = None
    ) -> Dict[str, Any]:
        """Check compatibility of new schema with existing versions.
        
        Args:
            subject: Schema subject name
            new_schema_content: New schema content to check
            compatibility_level: Override compatibility level for check
            
        Returns:
            Dictionary with compatibility results
        """
        start_time = time.time()
        
        self._logger.info("Starting schema compatibility check", 
                         subject=subject,
                         compatibility_level=compatibility_level)
        
        try:
            # Create Schema object
            new_schema = Schema(new_schema_content, schema_type="PROTOBUF")
            
            # Get current compatibility level if not provided
            if not compatibility_level:
                try:
                    compatibility_level = self.client.get_compatibility(subject)
                except Exception:
                    # Default to BACKWARD if can't get current level
                    compatibility_level = "BACKWARD"
            
            # Test compatibility
            with PerformanceTimer(self._logger, "compatibility_check", subject=subject):
                is_compatible = self.client.test_compatibility(subject, new_schema)
            
            # Get detailed compatibility info
            compatibility_result = {
                "subject": subject,
                "compatible": is_compatible,
                "compatibility_level": compatibility_level,
                "duration": time.time() - start_time
            }
            
            if not is_compatible:
                # Get existing schema for comparison
                try:
                    existing_version = self.client.get_latest_version(subject)
                    compatibility_result["latest_version"] = existing_version.version
                    compatibility_result["latest_schema_id"] = existing_version.schema_id
                    
                    # Analyze compatibility issues
                    compatibility_errors = self._analyze_compatibility_issues(
                        existing_version.schema.schema_str,
                        new_schema_content,
                        compatibility_level
                    )
                    compatibility_result["compatibility_errors"] = compatibility_errors
                    
                except Exception as e:
                    self._logger.warning("Could not get existing schema for comparison", 
                                       subject=subject, error=str(e))
            
            # Log compatibility check result
            self._logger.log_schema_registry_operation(
                operation="compatibility_check",
                subject=subject,
                success=True,
                duration=compatibility_result["duration"],
                compatible=is_compatible
            )
            
            self._logger.info("Schema compatibility check completed", **compatibility_result)
            return compatibility_result
            
        except Exception as e:
            duration = time.time() - start_time
            self._logger.log_schema_registry_operation(
                operation="compatibility_check",
                subject=subject,
                success=False,
                duration=duration,
                error=str(e)
            )
            
            if isinstance(e, SchemaCompatibilityError):
                raise
            else:
                raise ValidationError(f"Failed to check schema compatibility for subject {subject}: {e}")
    
    def check_schema_evolution_compatibility(
        self, 
        proto_file_path: Union[str, Path],
        topic: str,
        message_name: Optional[str] = None,
        force_compatibility_level: Optional[str] = None
    ) -> Dict[str, Any]:
        """Check if proto file is compatible for evolution.
        
        Args:
            proto_file_path: Path to .proto file
            topic: Kafka topic name
            message_name: Specific message name
            force_compatibility_level: Override compatibility level
            
        Returns:
            Dictionary with evolution compatibility results
        """
        proto_file = Path(proto_file_path)
        
        if not proto_file.exists():
            raise SchemaNotFoundError(
                schema_path=str(proto_file),
                schema_type="protobuf schema"
            )
        
        self._logger.info("Checking schema evolution compatibility", 
                         proto_file=str(proto_file), topic=topic)
        
        try:
            # Read and validate proto content
            with open(proto_file, 'r', encoding='utf-8') as f:
                proto_content = f.read()
            
            # Validate proto syntax
            self.compiler.validate_proto_syntax(str(proto_file))
            
            # Extract schema info
            schema_info = self._extract_schema_info(proto_content, message_name)
            
            # Generate subject name
            subject = self._generate_subject_name(topic, schema_info.get("package"), schema_info.get("message_name"))
            
            # Check compatibility
            compatibility_result = self.check_schema_compatibility(
                subject=subject,
                new_schema_content=proto_content,
                compatibility_level=force_compatibility_level
            )
            
            # Add evolution-specific information
            evolution_result = {
                **compatibility_result,
                "proto_file": str(proto_file),
                "topic": topic,
                "schema_info": schema_info,
                "evolution_safe": compatibility_result["compatible"]
            }
            
            if not evolution_result["evolution_safe"]:
                evolution_result["evolution_recommendations"] = self._generate_evolution_recommendations(
                    compatibility_result.get("compatibility_errors", []),
                    compatibility_result.get("compatibility_level", "BACKWARD")
                )
            
            return evolution_result
            
        except Exception as e:
            if isinstance(e, (ValidationError, CompilationError, SchemaNotFoundError)):
                raise
            else:
                raise ValidationError(f"Failed to check evolution compatibility: {e}")
    
    def _analyze_compatibility_issues(
        self, 
        existing_schema: str, 
        new_schema: str, 
        compatibility_level: str
    ) -> List[str]:
        """Analyze specific compatibility issues between schemas.
        
        Args:
            existing_schema: Current schema content
            new_schema: New schema content
            compatibility_level: Compatibility level to check against
            
        Returns:
            List of specific compatibility errors
        """
        compatibility_errors = []
        
        try:
            # Extract message information from both schemas
            existing_info = self._extract_schema_info(existing_schema)
            new_info = self._extract_schema_info(new_schema)
            
            # Check for removed messages
            existing_messages = set(existing_info.get("messages", []))
            new_messages = set(new_info.get("messages", []))
            
            removed_messages = existing_messages - new_messages
            if removed_messages and compatibility_level in ["BACKWARD", "FULL"]:
                compatibility_errors.extend([
                    f"Removed message '{msg}' breaks {compatibility_level} compatibility"
                    for msg in removed_messages
                ])
            
            added_messages = new_messages - existing_messages
            if added_messages and compatibility_level in ["FORWARD", "FULL"]:
                # Adding messages is generally OK for protobuf
                pass
            
            # Check package changes
            existing_package = existing_info.get("package")
            new_package = new_info.get("package")
            
            if existing_package != new_package:
                compatibility_errors.append(
                    f"Package change from '{existing_package}' to '{new_package}' may break compatibility"
                )
            
            # Check syntax version changes
            existing_syntax = existing_info.get("syntax")
            new_syntax = new_info.get("syntax")
            
            if existing_syntax and new_syntax and existing_syntax != new_syntax:
                compatibility_errors.append(
                    f"Syntax version change from '{existing_syntax}' to '{new_syntax}' may break compatibility"
                )
            
            # More detailed field-level analysis could be added here
            # This would require parsing the proto message structure more deeply
            
        except Exception as e:
            compatibility_errors.append(f"Could not analyze schema differences: {e}")
        
        return compatibility_errors
    
    def _generate_evolution_recommendations(
        self, 
        compatibility_errors: List[str], 
        compatibility_level: str
    ) -> List[str]:
        """Generate recommendations for schema evolution.
        
        Args:
            compatibility_errors: List of compatibility errors
            compatibility_level: Current compatibility level
            
        Returns:
            List of recommendations
        """
        recommendations = []
        
        if compatibility_level in ["BACKWARD", "FULL"]:
            recommendations.extend([
                "Only add optional fields to maintain backward compatibility",
                "Do not remove or rename existing fields",
                "Do not change field types",
                "Consider using field deprecation instead of removal"
            ])
        
        if compatibility_level in ["FORWARD", "FULL"]:
            recommendations.extend([
                "Ensure old consumers can handle new optional fields",
                "Do not add required fields without defaults",
                "Consider the impact on existing consumers"
            ])
        
        # Specific recommendations based on errors
        for error in compatibility_errors:
            if "removed message" in error.lower():
                recommendations.append("Use message deprecation instead of removal")
            elif "package change" in error.lower():
                recommendations.append("Avoid changing package names; use message aliases if needed")
            elif "syntax version" in error.lower():
                recommendations.append("Maintain consistent protobuf syntax version")
        
        # Remove duplicates while preserving order
        seen = set()
        unique_recommendations = []
        for rec in recommendations:
            if rec not in seen:
                seen.add(rec)
                unique_recommendations.append(rec)
        
        return unique_recommendations
    
    def get_schema_evolution_history(self, subject: str) -> Dict[str, Any]:
        """Get schema evolution history for a subject.
        
        Args:
            subject: Schema subject name
            
        Returns:
            Dictionary with evolution history
        """
        try:
            # Get all versions
            all_versions = self.client.get_versions(subject)
            
            if not all_versions:
                return {
                    "subject": subject,
                    "versions": [],
                    "total_versions": 0,
                    "evolution_timeline": []
                }
            
            evolution_timeline = []
            
            for version in all_versions:
                try:
                    version_info = self.client.get_version(subject, version)
                    
                    # Extract basic info about this version
                    schema_content = version_info.schema.schema_str
                    schema_info = self._extract_schema_info(schema_content)
                    
                    evolution_entry = {
                        "version": version,
                        "schema_id": version_info.schema_id,
                        "schema_type": version_info.schema.schema_type,
                        "messages": schema_info.get("messages", []),
                        "package": schema_info.get("package"),
                        "syntax": schema_info.get("syntax")
                    }
                    
                    evolution_timeline.append(evolution_entry)
                    
                except Exception as e:
                    self._logger.warning(f"Could not get details for version {version}", 
                                       subject=subject, error=str(e))
            
            # Sort by version
            evolution_timeline.sort(key=lambda x: x["version"])
            
            return {
                "subject": subject,
                "versions": all_versions,
                "total_versions": len(all_versions),
                "evolution_timeline": evolution_timeline,
                "latest_version": max(all_versions) if all_versions else None
            }
            
        except Exception as e:
            self._logger.error("Failed to get schema evolution history", 
                             subject=subject, error=str(e))
            raise ValidationError(f"Cannot get evolution history for subject {subject}: {e}")
    
    def validate_schema_evolution_plan(
        self, 
        evolution_plan: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Validate a planned schema evolution sequence.
        
        Args:
            evolution_plan: List of evolution steps with schema content
            
        Returns:
            Validation results for the evolution plan
        """
        validation_results = {
            "valid": True,
            "steps": [],
            "overall_compatible": True,
            "total_steps": len(evolution_plan),
            "failed_steps": 0
        }
        
        for i, step in enumerate(evolution_plan):
            step_result = {
                "step": i + 1,
                "subject": step.get("subject"),
                "valid": True,
                "compatible": True,
                "errors": [],
                "warnings": []
            }
            
            try:
                if "schema_content" not in step:
                    step_result["valid"] = False
                    step_result["errors"].append("Missing schema_content in evolution step")
                    continue
                
                if "subject" not in step:
                    step_result["valid"] = False
                    step_result["errors"].append("Missing subject in evolution step")
                    continue
                
                # Check compatibility for this step
                compatibility_result = self.check_schema_compatibility(
                    subject=step["subject"],
                    new_schema_content=step["schema_content"],
                    compatibility_level=step.get("compatibility_level")
                )
                
                step_result["compatible"] = compatibility_result["compatible"]
                
                if not compatibility_result["compatible"]:
                    step_result["errors"].extend(
                        compatibility_result.get("compatibility_errors", [])
                    )
                    validation_results["overall_compatible"] = False
                    validation_results["failed_steps"] += 1
                
            except Exception as e:
                step_result["valid"] = False
                step_result["errors"].append(f"Evolution step validation failed: {e}")
                validation_results["valid"] = False
                validation_results["failed_steps"] += 1
            
            validation_results["steps"].append(step_result)
        
        return validation_results
    
    def _load_persistent_cache(self) -> None:
        """Load schema cache from persistent storage."""
        if not self.enable_persistent_cache:
            return
        
        cache_file = self.cache_dir / "schema_cache.pkl"
        metadata_file = self.cache_dir / "cache_metadata.json"
        
        try:
            if cache_file.exists() and metadata_file.exists():
                # Load metadata first
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                
                # Check if cache is still valid
                cache_created = metadata.get("created_at", 0)
                current_time = time.time()
                
                if current_time - cache_created < self.cache_ttl_seconds:
                    # Load cache data
                    with open(cache_file, 'rb') as f:
                        cached_data = pickle.load(f)
                    
                    # Validate each cached entry
                    valid_entries = 0
                    for subject, data in cached_data.items():
                        if self._is_cache_entry_valid(data):
                            self._schema_cache[subject] = data
                            valid_entries += 1
                    
                    self._logger.info("Loaded persistent schema cache", 
                                     total_entries=len(cached_data),
                                     valid_entries=valid_entries,
                                     cache_age_hours=round((current_time - cache_created) / 3600, 1))
                else:
                    self._logger.info("Persistent cache expired, will create new cache",
                                     cache_age_hours=round((current_time - cache_created) / 3600, 1),
                                     ttl_hours=self.cache_ttl_hours)
                    # Clean up expired cache
                    self._clear_persistent_cache()
        
        except Exception as e:
            self._logger.warning("Failed to load persistent cache", error=str(e))
            # If loading fails, start with empty cache
            self._schema_cache = {}
    
    def _save_persistent_cache(self) -> None:
        """Save current schema cache to persistent storage."""
        if not self.enable_persistent_cache:
            return
        
        cache_file = self.cache_dir / "schema_cache.pkl"
        metadata_file = self.cache_dir / "cache_metadata.json"
        
        try:
            # Filter out expired entries before saving
            current_time = time.time()
            valid_cache = {}
            
            for subject, data in self._schema_cache.items():
                if self._is_cache_entry_valid(data):
                    valid_cache[subject] = data
            
            # Save cache data
            with open(cache_file, 'wb') as f:
                pickle.dump(valid_cache, f)
            
            # Save metadata
            metadata = {
                "created_at": current_time,
                "ttl_hours": self.cache_ttl_hours,
                "entry_count": len(valid_cache),
                "schema_registry_url": self.schema_registry_url
            }
            
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            self._logger.debug("Saved persistent schema cache", 
                             entry_count=len(valid_cache),
                             cache_file=str(cache_file))
        
        except Exception as e:
            self._logger.warning("Failed to save persistent cache", error=str(e))
    
    def _is_cache_entry_valid(self, cache_entry: Dict[str, Any]) -> bool:
        """Check if a cache entry is still valid.
        
        Args:
            cache_entry: Cache entry to validate
            
        Returns:
            True if entry is valid
        """
        if not isinstance(cache_entry, dict):
            return False
        
        cached_at = cache_entry.get("cached_at", 0)
        if not cached_at:
            return False
        
        current_time = time.time()
        return (current_time - cached_at) < self.cache_ttl_seconds
    
    def _clear_persistent_cache(self) -> None:
        """Clear persistent cache files."""
        if not self.enable_persistent_cache:
            return
        
        cache_file = self.cache_dir / "schema_cache.pkl"
        metadata_file = self.cache_dir / "cache_metadata.json"
        
        try:
            if cache_file.exists():
                cache_file.unlink()
            if metadata_file.exists():
                metadata_file.unlink()
            
            self._logger.info("Cleared persistent cache files")
        
        except Exception as e:
            self._logger.warning("Failed to clear persistent cache", error=str(e))
    
    def get_cache_statistics(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        current_time = time.time()
        stats = {
            "cache_enabled": self.enable_persistent_cache,
            "cache_dir": str(self.cache_dir) if self.enable_persistent_cache else None,
            "ttl_hours": self.cache_ttl_hours,
            "in_memory_entries": len(self._schema_cache),
            "valid_entries": 0,
            "expired_entries": 0,
            "subjects": [],
            "total_size_bytes": 0
        }
        
        # Analyze in-memory cache
        for subject, data in self._schema_cache.items():
            if self._is_cache_entry_valid(data):
                stats["valid_entries"] += 1
                stats["subjects"].append({
                    "subject": subject,
                    "cached_at": data.get("cached_at", 0),
                    "age_hours": round((current_time - data.get("cached_at", 0)) / 3600, 1),
                    "topic": data.get("topic"),
                    "proto_file": data.get("proto_file")
                })
            else:
                stats["expired_entries"] += 1
        
        # Get persistent cache info
        if self.enable_persistent_cache:
            cache_file = self.cache_dir / "schema_cache.pkl"
            metadata_file = self.cache_dir / "cache_metadata.json"
            
            if cache_file.exists():
                stats["persistent_cache_size_bytes"] = cache_file.stat().st_size
                stats["total_size_bytes"] += stats["persistent_cache_size_bytes"]
            
            if metadata_file.exists():
                try:
                    with open(metadata_file, 'r') as f:
                        metadata = json.load(f)
                    
                    stats["persistent_cache_created"] = metadata.get("created_at", 0)
                    stats["persistent_cache_age_hours"] = round(
                        (current_time - metadata.get("created_at", 0)) / 3600, 1
                    )
                    stats["persistent_cache_entries"] = metadata.get("entry_count", 0)
                
                except Exception as e:
                    stats["persistent_cache_error"] = str(e)
        
        return stats
    
    def cleanup_expired_cache(self) -> Dict[str, int]:
        """Clean up expired cache entries.
        
        Returns:
            Dictionary with cleanup statistics
        """
        initial_count = len(self._schema_cache)
        expired_subjects = []
        
        # Find expired entries
        for subject, data in list(self._schema_cache.items()):
            if not self._is_cache_entry_valid(data):
                expired_subjects.append(subject)
                del self._schema_cache[subject]
        
        # Save updated cache
        if expired_subjects and self.enable_persistent_cache:
            self._save_persistent_cache()
        
        cleanup_stats = {
            "initial_entries": initial_count,
            "expired_entries": len(expired_subjects),
            "remaining_entries": len(self._schema_cache),
            "expired_subjects": expired_subjects
        }
        
        if expired_subjects:
            self._logger.info("Cleaned up expired cache entries", **cleanup_stats)
        
        return cleanup_stats
    
    def warmup_cache_from_registry(
        self, 
        subjects: Optional[List[str]] = None,
        max_subjects: int = 100
    ) -> Dict[str, Any]:
        """Warmup cache by fetching schemas from registry.
        
        Args:
            subjects: Specific subjects to fetch, or None for all
            max_subjects: Maximum number of subjects to fetch
            
        Returns:
            Dictionary with warmup results
        """
        start_time = time.time()
        warmup_results = {
            "success": True,
            "fetched_subjects": [],
            "failed_subjects": [],
            "total_fetched": 0,
            "total_failed": 0,
            "duration": 0
        }
        
        try:
            # Get subjects to fetch
            if subjects is None:
                subjects = self.list_subjects()
            
            # Limit subjects to avoid overwhelming the registry
            subjects_to_fetch = subjects[:max_subjects]
            
            self._logger.info("Starting cache warmup", 
                             total_subjects=len(subjects_to_fetch))
            
            for subject in subjects_to_fetch:
                try:
                    # Get schema metadata and cache it
                    metadata = self.get_schema_metadata(subject)
                    
                    # Create cache entry
                    cache_data = {
                        "schema_metadata": metadata,
                        "warmup_fetch": True,
                        "subject": subject
                    }
                    
                    self._cache_schema(subject, cache_data)
                    warmup_results["fetched_subjects"].append(subject)
                    
                except Exception as e:
                    self._logger.warning("Failed to warmup cache for subject", 
                                       subject=subject, error=str(e))
                    warmup_results["failed_subjects"].append({
                        "subject": subject,
                        "error": str(e)
                    })
            
            warmup_results["total_fetched"] = len(warmup_results["fetched_subjects"])
            warmup_results["total_failed"] = len(warmup_results["failed_subjects"])
            warmup_results["duration"] = time.time() - start_time
            
            self._logger.info("Cache warmup completed", **{
                k: v for k, v in warmup_results.items() 
                if k not in ["fetched_subjects", "failed_subjects"]
            })
            
        except Exception as e:
            warmup_results["success"] = False
            warmup_results["error"] = str(e)
            warmup_results["duration"] = time.time() - start_time
            
            self._logger.error("Cache warmup failed", error=str(e))
        
        return warmup_results