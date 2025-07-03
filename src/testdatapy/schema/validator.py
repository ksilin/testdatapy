"""Comprehensive validation system for protobuf configurations and schemas.

This module provides validation functionality for:
- Protobuf configuration settings
- Schema compilation testing
- Dependency checking and resolution
"""

import json
import logging
import shutil
import tempfile
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Set

from .compiler import ProtobufCompiler
from .manager import SchemaManager
from .exceptions import ValidationError, SchemaNotFoundError, CompilationError, SchemaDependencyError
from ..config.correlation_config import CorrelationConfig


logger = logging.getLogger(__name__)


class ProtobufConfigValidator:
    """Validates protobuf configuration settings and schemas."""
    
    def __init__(self, config: Optional[CorrelationConfig] = None):
        """Initialize validator with optional configuration.
        
        Args:
            config: CorrelationConfig instance with protobuf settings
        """
        self.config = config
        try:
            self.compiler = ProtobufCompiler()
        except Exception:
            # Create compiler without protoc detection for testing
            self.compiler = None
        self.manager = SchemaManager() if config is None else None
    
    def validate_all(self, config_path: Optional[str] = None) -> Dict[str, Any]:
        """Perform comprehensive validation of protobuf configuration.
        
        Args:
            config_path: Optional path to configuration file
            
        Returns:
            Dictionary with validation results
        """
        results = {
            "valid": True,
            "config_validation": {},
            "schema_compilation": {},
            "dependency_validation": {},
            "warnings": [],
            "errors": [],
            "summary": {}
        }
        
        try:
            # Load configuration if path provided
            if config_path:
                self.config = CorrelationConfig.from_yaml_file(config_path)
            
            if not self.config:
                results["errors"].append("No configuration provided")
                results["valid"] = False
                return results
            
            # 1. Validate configuration settings
            logger.info("Validating protobuf configuration settings...")
            config_results = self.validate_config_settings()
            results["config_validation"] = config_results
            
            if not config_results["valid"]:
                results["valid"] = False
                results["errors"].extend(config_results["errors"])
            
            results["warnings"].extend(config_results.get("warnings", []))
            
            # 2. Test schema compilation (only if config is valid)
            if config_results["valid"]:
                logger.info("Testing schema compilation...")
                compilation_results = self.test_schema_compilation()
                results["schema_compilation"] = compilation_results
                
                if not compilation_results["valid"]:
                    results["valid"] = False
                    results["errors"].extend(compilation_results["errors"])
                
                results["warnings"].extend(compilation_results.get("warnings", []))
            
            # 3. Validate dependencies
            logger.info("Validating protobuf dependencies...")
            dependency_results = self.validate_dependencies()
            results["dependency_validation"] = dependency_results
            
            if not dependency_results["valid"]:
                results["valid"] = False
                results["errors"].extend(dependency_results["errors"])
            
            results["warnings"].extend(dependency_results.get("warnings", []))
            
            # Generate summary
            results["summary"] = self._generate_summary(results)
            
        except Exception as e:
            logger.error(f"Validation failed with error: {e}")
            results["valid"] = False
            results["errors"].append(f"Validation process failed: {str(e)}")
        
        return results
    
    def validate_config_settings(self) -> Dict[str, Any]:
        """Validate protobuf configuration settings.
        
        Returns:
            Dictionary with configuration validation results
        """
        results = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "validated_settings": {},
            "entities_checked": 0
        }
        
        if not self.config:
            results["errors"].append("No configuration provided")
            results["valid"] = False
            return results
        
        try:
            # Validate global protobuf settings
            global_settings = self.config.get_protobuf_settings()
            if global_settings:
                settings_validation = self._validate_global_settings(global_settings)
                results["validated_settings"]["global"] = settings_validation
                
                if not settings_validation["valid"]:
                    results["valid"] = False
                    results["errors"].extend(settings_validation["errors"])
                
                results["warnings"].extend(settings_validation.get("warnings", []))
            else:
                results["warnings"].append("No global protobuf_settings configured")
            
            # Validate entity-specific protobuf configurations
            entity_results = self._validate_entity_configs()
            results["validated_settings"]["entities"] = entity_results
            results["entities_checked"] = len(entity_results)
            
            for entity_name, entity_result in entity_results.items():
                if not entity_result["valid"]:
                    results["valid"] = False
                    results["errors"].extend([f"Entity {entity_name}: {error}" for error in entity_result["errors"]])
                
                results["warnings"].extend([f"Entity {entity_name}: {warning}" for warning in entity_result.get("warnings", [])])
        
        except Exception as e:
            results["valid"] = False
            results["errors"].append(f"Configuration validation error: {str(e)}")
        
        return results
    
    def test_schema_compilation(self) -> Dict[str, Any]:
        """Test schema compilation without side effects.
        
        Returns:
            Dictionary with compilation test results
        """
        results = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "compiled_schemas": [],
            "failed_schemas": [],
            "test_details": {}
        }
        
        if not self.config:
            results["errors"].append("No configuration provided")
            results["valid"] = False
            return results
        
        # Get all proto files to test
        proto_files = self._collect_proto_files()
        
        if not proto_files:
            results["warnings"].append("No proto files found for compilation testing")
            return results
        
        # Use temporary directory for compilation testing
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_output = Path(temp_dir)
            
            for proto_file, entity_info in proto_files.items():
                logger.debug(f"Testing compilation of {proto_file}")
                
                try:
                    # Get effective schema paths for this entity
                    schema_paths = self.config.get_effective_schema_paths(
                        entity_info["entity_name"], 
                        entity_info["is_master"]
                    )
                    
                    # Test compilation
                    compilation_result = self.compiler.compile_proto(
                        proto_file_path=proto_file,
                        output_dir=str(temp_output),
                        proto_paths=schema_paths,
                        validate_only=True  # Don't actually generate files
                    )
                    
                    if compilation_result:
                        results["compiled_schemas"].append({
                            "proto_file": proto_file,
                            "entity": entity_info["entity_name"],
                            "is_master": entity_info["is_master"],
                            "output_files": compilation_result.get("generated_files", [])
                        })
                    else:
                        results["failed_schemas"].append({
                            "proto_file": proto_file,
                            "entity": entity_info["entity_name"],
                            "error": "Compilation returned no result"
                        })
                        results["valid"] = False
                        results["errors"].append(f"Failed to compile {proto_file}")
                
                except CompilationError as e:
                    results["failed_schemas"].append({
                        "proto_file": proto_file,
                        "entity": entity_info["entity_name"],
                        "error": str(e),
                        "details": e.details
                    })
                    results["valid"] = False
                    results["errors"].append(f"Compilation failed for {proto_file}: {str(e)}")
                
                except Exception as e:
                    results["failed_schemas"].append({
                        "proto_file": proto_file,
                        "entity": entity_info["entity_name"],
                        "error": f"Unexpected error: {str(e)}"
                    })
                    results["valid"] = False
                    results["errors"].append(f"Unexpected error compiling {proto_file}: {str(e)}")
        
        # Generate test summary
        total_files = len(proto_files)
        successful_compilations = len(results["compiled_schemas"])
        failed_compilations = len(results["failed_schemas"])
        
        results["test_details"] = {
            "total_files": total_files,
            "successful_compilations": successful_compilations,
            "failed_compilations": failed_compilations,
            "success_rate": successful_compilations / total_files if total_files > 0 else 0
        }
        
        if failed_compilations > 0:
            results["warnings"].append(f"{failed_compilations}/{total_files} schema compilations failed")
        
        return results
    
    def validate_dependencies(self) -> Dict[str, Any]:
        """Validate proto dependencies and imports.
        
        Returns:
            Dictionary with dependency validation results
        """
        results = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "dependency_graph": {},
            "missing_dependencies": [],
            "circular_dependencies": [],
            "validation_details": {}
        }
        
        if not self.config:
            results["errors"].append("No configuration provided")
            results["valid"] = False
            return results
        
        try:
            # Collect all proto files and their dependencies
            proto_files = self._collect_proto_files()
            
            if not proto_files:
                results["warnings"].append("No proto files found for dependency validation")
                return results
            
            dependency_graph = {}
            all_dependencies = set()
            
            for proto_file, entity_info in proto_files.items():
                logger.debug(f"Validating dependencies for {proto_file}")
                
                try:
                    # Get schema paths for dependency resolution
                    schema_paths = self.config.get_effective_schema_paths(
                        entity_info["entity_name"],
                        entity_info["is_master"]
                    )
                    
                    # Resolve dependencies
                    dependencies = self.compiler.resolve_dependencies(proto_file, schema_paths)
                    dependency_graph[proto_file] = dependencies
                    all_dependencies.update(dependencies)
                    
                except Exception as e:
                    results["errors"].append(f"Failed to resolve dependencies for {proto_file}: {str(e)}")
                    results["valid"] = False
            
            # Check for missing dependencies
            missing_deps = self._check_missing_dependencies(dependency_graph, proto_files.keys())
            if missing_deps:
                results["missing_dependencies"] = missing_deps
                results["valid"] = False
                results["errors"].extend([f"Missing dependency: {dep}" for dep in missing_deps])
            
            # Check for circular dependencies
            circular_deps = self._detect_circular_dependencies(dependency_graph)
            if circular_deps:
                results["circular_dependencies"] = circular_deps
                results["valid"] = False
                results["errors"].extend([f"Circular dependency detected: {' -> '.join(cycle)}" for cycle in circular_deps])
            
            results["dependency_graph"] = dependency_graph
            results["validation_details"] = {
                "total_proto_files": len(proto_files),
                "total_dependencies": len(all_dependencies),
                "missing_count": len(missing_deps),
                "circular_count": len(circular_deps)
            }
        
        except Exception as e:
            results["valid"] = False
            results["errors"].append(f"Dependency validation error: {str(e)}")
        
        return results
    
    def _validate_global_settings(self, settings: Dict[str, Any]) -> Dict[str, Any]:
        """Validate global protobuf settings.
        
        Args:
            settings: Global protobuf settings dictionary
            
        Returns:
            Validation results for global settings
        """
        results = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "checked_settings": {}
        }
        
        # Check protoc availability
        protoc_path = settings.get("protoc_path", "protoc")
        if not shutil.which(protoc_path):
            results["errors"].append(f"protoc compiler not found at: {protoc_path}")
            results["valid"] = False
        else:
            results["checked_settings"]["protoc_available"] = True
        
        # Validate schema paths
        schema_paths = settings.get("schema_paths", [])
        invalid_paths = []
        for path in schema_paths:
            path_obj = Path(path)
            if not path_obj.exists():
                invalid_paths.append(path)
            elif not path_obj.is_dir():
                invalid_paths.append(f"{path} (not a directory)")
        
        if invalid_paths:
            results["errors"].extend([f"Invalid schema path: {path}" for path in invalid_paths])
            results["valid"] = False
        
        results["checked_settings"]["schema_paths"] = {
            "total": len(schema_paths),
            "valid": len(schema_paths) - len(invalid_paths),
            "invalid": invalid_paths
        }
        
        # Validate timeout setting
        timeout = settings.get("timeout", 60)
        if not isinstance(timeout, (int, float)) or timeout <= 0:
            results["errors"].append(f"Invalid timeout value: {timeout} (must be positive number)")
            results["valid"] = False
        else:
            results["checked_settings"]["timeout"] = timeout
        
        # Check boolean settings
        boolean_settings = ["auto_compile", "temp_compilation", "validate_dependencies"]
        for setting in boolean_settings:
            value = settings.get(setting)
            if value is not None and not isinstance(value, bool):
                results["errors"].append(f"Setting {setting} must be boolean, got: {type(value).__name__}")
                results["valid"] = False
            else:
                results["checked_settings"][setting] = value
        
        return results
    
    def _validate_entity_configs(self) -> Dict[str, Dict[str, Any]]:
        """Validate entity-specific protobuf configurations.
        
        Returns:
            Dictionary mapping entity names to validation results
        """
        entity_results = {}
        
        # Check master data entities
        for entity_name, entity_config in self.config.config.get("master_data", {}).items():
            if self.config.has_protobuf_config(entity_name, is_master=True):
                entity_results[f"master.{entity_name}"] = self._validate_single_entity_config(
                    entity_name, entity_config, is_master=True
                )
        
        # Check transactional data entities
        for entity_name, entity_config in self.config.config.get("transactional_data", {}).items():
            if self.config.has_protobuf_config(entity_name, is_master=False):
                entity_results[f"transactional.{entity_name}"] = self._validate_single_entity_config(
                    entity_name, entity_config, is_master=False
                )
        
        return entity_results
    
    def _validate_single_entity_config(self, entity_name: str, entity_config: Dict[str, Any], is_master: bool) -> Dict[str, Any]:
        """Validate configuration for a single entity.
        
        Args:
            entity_name: Name of the entity
            entity_config: Entity configuration dictionary
            is_master: Whether this is master data
            
        Returns:
            Validation results for the entity
        """
        results = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "protobuf_config": {}
        }
        
        try:
            # Get merged protobuf configuration
            merged_config = self.config.get_merged_protobuf_config(entity_name, is_master)
            results["protobuf_config"] = merged_config
            
            # Check required fields
            if not merged_config.get("protobuf_module"):
                results["errors"].append("Missing protobuf_module")
                results["valid"] = False
            
            if not merged_config.get("protobuf_class"):
                results["errors"].append("Missing protobuf_class")
                results["valid"] = False
            
            # Validate proto file path if specified
            proto_file_path = merged_config.get("proto_file_path")
            if proto_file_path:
                schema_paths = self.config.get_effective_schema_paths(entity_name, is_master)
                
                proto_file_found = False
                for schema_path in schema_paths:
                    candidate = Path(schema_path) / proto_file_path
                    if candidate.exists():
                        proto_file_found = True
                        break
                
                if not proto_file_found:
                    results["errors"].append(f"Proto file not found: {proto_file_path} in paths {schema_paths}")
                    results["valid"] = False
            
            # Validate schema paths
            schema_paths = self.config.get_effective_schema_paths(entity_name, is_master)
            if not schema_paths:
                results["warnings"].append("No schema paths configured")
            else:
                invalid_paths = []
                for path in schema_paths:
                    if not Path(path).exists():
                        invalid_paths.append(path)
                
                if invalid_paths:
                    results["errors"].extend([f"Invalid schema path: {path}" for path in invalid_paths])
                    results["valid"] = False
        
        except Exception as e:
            results["valid"] = False
            results["errors"].append(f"Configuration validation error: {str(e)}")
        
        return results
    
    def _collect_proto_files(self) -> Dict[str, Dict[str, Any]]:
        """Collect all proto files from configuration.
        
        Returns:
            Dictionary mapping proto file paths to entity information
        """
        proto_files = {}
        
        # Collect from master data
        for entity_name, entity_config in self.config.config.get("master_data", {}).items():
            if self.config.has_protobuf_config(entity_name, is_master=True):
                merged_config = self.config.get_merged_protobuf_config(entity_name, is_master=True)
                proto_file_path = merged_config.get("proto_file_path")
                
                if proto_file_path:
                    schema_paths = self.config.get_effective_schema_paths(entity_name, is_master=True)
                    
                    for schema_path in schema_paths:
                        candidate = Path(schema_path) / proto_file_path
                        if candidate.exists():
                            proto_files[str(candidate)] = {
                                "entity_name": entity_name,
                                "is_master": True,
                                "proto_file_path": proto_file_path,
                                "schema_paths": schema_paths
                            }
                            break
        
        # Collect from transactional data
        for entity_name, entity_config in self.config.config.get("transactional_data", {}).items():
            if self.config.has_protobuf_config(entity_name, is_master=False):
                merged_config = self.config.get_merged_protobuf_config(entity_name, is_master=False)
                proto_file_path = merged_config.get("proto_file_path")
                
                if proto_file_path:
                    schema_paths = self.config.get_effective_schema_paths(entity_name, is_master=False)
                    
                    for schema_path in schema_paths:
                        candidate = Path(schema_path) / proto_file_path
                        if candidate.exists():
                            proto_files[str(candidate)] = {
                                "entity_name": entity_name,
                                "is_master": False,
                                "proto_file_path": proto_file_path,
                                "schema_paths": schema_paths
                            }
                            break
        
        return proto_files
    
    def _check_missing_dependencies(self, dependency_graph: Dict[str, List[str]], known_files: Set[str]) -> List[str]:
        """Check for missing dependencies in the dependency graph.
        
        Args:
            dependency_graph: Mapping of files to their dependencies
            known_files: Set of known proto files
            
        Returns:
            List of missing dependency file paths
        """
        missing = []
        all_dependencies = set()
        
        for deps in dependency_graph.values():
            all_dependencies.update(deps)
        
        for dep in all_dependencies:
            if dep not in known_files and not Path(dep).exists():
                missing.append(dep)
        
        return missing
    
    def _detect_circular_dependencies(self, dependency_graph: Dict[str, List[str]]) -> List[List[str]]:
        """Detect circular dependencies in the dependency graph.
        
        Args:
            dependency_graph: Mapping of files to their dependencies
            
        Returns:
            List of circular dependency chains
        """
        def has_cycle(node: str, path: List[str], visited: Set[str]) -> Optional[List[str]]:
            if node in path:
                # Found cycle - return the cycle portion
                cycle_start = path.index(node)
                return path[cycle_start:] + [node]
            
            if node in visited:
                return None
            
            visited.add(node)
            
            for dep in dependency_graph.get(node, []):
                cycle = has_cycle(dep, path + [node], visited)
                if cycle:
                    return cycle
            
            return None
        
        cycles = []
        global_visited = set()
        
        for node in dependency_graph:
            if node not in global_visited:
                cycle = has_cycle(node, [], set())
                if cycle:
                    cycles.append(cycle)
                    global_visited.update(cycle)
        
        return cycles
    
    def _generate_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate validation summary.
        
        Args:
            results: Complete validation results
            
        Returns:
            Summary dictionary
        """
        summary = {
            "overall_valid": results["valid"],
            "total_errors": len(results["errors"]),
            "total_warnings": len(results["warnings"]),
            "components_checked": []
        }
        
        # Config validation summary
        config_validation = results.get("config_validation", {})
        if config_validation:
            summary["components_checked"].append({
                "component": "configuration",
                "valid": config_validation.get("valid", False),
                "entities_checked": config_validation.get("entities_checked", 0),
                "errors": len(config_validation.get("errors", [])),
                "warnings": len(config_validation.get("warnings", []))
            })
        
        # Schema compilation summary
        schema_compilation = results.get("schema_compilation", {})
        if schema_compilation:
            test_details = schema_compilation.get("test_details", {})
            summary["components_checked"].append({
                "component": "schema_compilation",
                "valid": schema_compilation.get("valid", False),
                "total_files": test_details.get("total_files", 0),
                "successful": test_details.get("successful_compilations", 0),
                "failed": test_details.get("failed_compilations", 0),
                "success_rate": test_details.get("success_rate", 0.0)
            })
        
        # Dependency validation summary
        dependency_validation = results.get("dependency_validation", {})
        if dependency_validation:
            validation_details = dependency_validation.get("validation_details", {})
            summary["components_checked"].append({
                "component": "dependencies",
                "valid": dependency_validation.get("valid", False),
                "total_proto_files": validation_details.get("total_proto_files", 0),
                "total_dependencies": validation_details.get("total_dependencies", 0),
                "missing_dependencies": validation_details.get("missing_count", 0),
                "circular_dependencies": validation_details.get("circular_count", 0)
            })
        
        return summary


def validate_protobuf_config(config_path: str, verbose: bool = False) -> Dict[str, Any]:
    """Standalone function to validate protobuf configuration.
    
    Args:
        config_path: Path to configuration file
        verbose: Whether to include verbose output
        
    Returns:
        Validation results dictionary
    """
    validator = ProtobufConfigValidator()
    return validator.validate_all(config_path)


def generate_validation_report(results: Dict[str, Any], output_format: str = "text") -> str:
    """Generate human-readable validation report.
    
    Args:
        results: Validation results from validate_all()
        output_format: Output format ("text" or "json")
        
    Returns:
        Formatted validation report
    """
    if output_format == "json":
        return json.dumps(results, indent=2)
    
    # Generate text report
    lines = []
    lines.append("=" * 60)
    lines.append("PROTOBUF CONFIGURATION VALIDATION REPORT")
    lines.append("=" * 60)
    
    summary = results.get("summary", {})
    overall_valid = summary.get("overall_valid", False)
    
    # Overall status
    status_icon = "✅" if overall_valid else "❌"
    lines.append(f"\n{status_icon} Overall Status: {'VALID' if overall_valid else 'INVALID'}")
    lines.append(f"   Total Errors: {summary.get('total_errors', 0)}")
    lines.append(f"   Total Warnings: {summary.get('total_warnings', 0)}")
    
    # Component summaries
    lines.append("\n📋 COMPONENT SUMMARY:")
    for component in summary.get("components_checked", []):
        comp_name = component["component"]
        comp_valid = component["valid"]
        comp_icon = "✅" if comp_valid else "❌"
        
        lines.append(f"\n{comp_icon} {comp_name.title()}:")
        
        if comp_name == "configuration":
            lines.append(f"   Entities Checked: {component.get('entities_checked', 0)}")
        elif comp_name == "schema_compilation":
            total = component.get('total_files', 0)
            successful = component.get('successful', 0)
            success_rate = component.get('success_rate', 0.0)
            lines.append(f"   Files Tested: {total}")
            lines.append(f"   Successful: {successful}")
            lines.append(f"   Success Rate: {success_rate:.1%}")
        elif comp_name == "dependencies":
            lines.append(f"   Proto Files: {component.get('total_proto_files', 0)}")
            lines.append(f"   Dependencies: {component.get('total_dependencies', 0)}")
            lines.append(f"   Missing: {component.get('missing_dependencies', 0)}")
            lines.append(f"   Circular: {component.get('circular_dependencies', 0)}")
    
    # Errors section
    if results.get("errors"):
        lines.append("\n❌ ERRORS:")
        for error in results["errors"]:
            lines.append(f"   • {error}")
    
    # Warnings section
    if results.get("warnings"):
        lines.append("\n⚠️  WARNINGS:")
        for warning in results["warnings"]:
            lines.append(f"   • {warning}")
    
    # Failed schemas section
    schema_compilation = results.get("schema_compilation", {})
    failed_schemas = schema_compilation.get("failed_schemas", [])
    if failed_schemas:
        lines.append("\n🚫 FAILED SCHEMA COMPILATIONS:")
        for failed in failed_schemas:
            lines.append(f"   • {failed['proto_file']} (Entity: {failed['entity']})")
            lines.append(f"     Error: {failed['error']}")
    
    # Dependency issues section
    dependency_validation = results.get("dependency_validation", {})
    missing_deps = dependency_validation.get("missing_dependencies", [])
    circular_deps = dependency_validation.get("circular_dependencies", [])
    
    if missing_deps:
        lines.append("\n🔍 MISSING DEPENDENCIES:")
        for dep in missing_deps:
            lines.append(f"   • {dep}")
    
    if circular_deps:
        lines.append("\n🔄 CIRCULAR DEPENDENCIES:")
        for cycle in circular_deps:
            lines.append(f"   • {' -> '.join(cycle)}")
    
    lines.append("\n" + "=" * 60)
    
    return "\n".join(lines)