"""Configuration validation for enhanced features."""
import os
from pathlib import Path
from typing import Dict, Any, List


class ConfigurationError(Exception):
    """Raised when configuration validation fails."""
    pass


def validate_csv_export_configuration(entity_config: Dict[str, Any], entity_name: str) -> None:
    """Validate CSV export configuration.
    
    Args:
        entity_config: Configuration for a specific entity
        entity_name: Name of the entity for error messages
        
    Raises:
        ConfigurationError: If configuration is invalid
    """
    csv_export = entity_config.get("csv_export", {})
    if not csv_export.get("enabled", False):
        return  # No validation needed if export is disabled
    
    export_file = csv_export.get("file")
    if not export_file:
        raise ConfigurationError(
            f"Entity '{entity_name}': csv_export is enabled but no 'file' path specified. "
            "Please provide a valid file path for CSV export."
        )
    
    # Check if the directory exists and is writable
    export_path = Path(export_file)
    parent_dir = export_path.parent
    
    if not parent_dir.exists():
        try:
            parent_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise ConfigurationError(
                f"Entity '{entity_name}': Cannot create directory '{parent_dir}' for CSV export. "
                f"Error: {e}. Please ensure the path is valid and you have write permissions."
            )
    
    if not os.access(parent_dir, os.W_OK):
        raise ConfigurationError(
            f"Entity '{entity_name}': Directory '{parent_dir}' is not writable for CSV export. "
            "Please check your file permissions or choose a different path."
        )


def validate_csv_source_configuration(entity_config: Dict[str, Any], entity_name: str) -> None:
    """Validate CSV source configuration.
    
    Args:
        entity_config: Configuration for a specific entity
        entity_name: Name of the entity for error messages
        
    Raises:
        ConfigurationError: If configuration is invalid
    """
    source = entity_config.get("source")
    if source != "csv":
        return  # No validation needed for non-CSV sources
    
    csv_file = entity_config.get("file")
    if not csv_file:
        raise ConfigurationError(
            f"Entity '{entity_name}': source is 'csv' but no 'file' path specified. "
            "Please provide a valid CSV file path."
        )
    
    csv_path = Path(csv_file)
    if not csv_path.exists():
        raise ConfigurationError(
            f"Entity '{entity_name}': CSV file '{csv_file}' does not exist. "
            "Please provide a valid path to an existing CSV file."
        )
    
    if not csv_path.is_file():
        raise ConfigurationError(
            f"Entity '{entity_name}': Path '{csv_file}' is not a file. "
            "Please provide a path to a CSV file, not a directory."
        )
    
    if not os.access(csv_path, os.R_OK):
        raise ConfigurationError(
            f"Entity '{entity_name}': CSV file '{csv_file}' is not readable. "
            "Please check your file permissions."
        )


def validate_bulk_load_configuration(entity_config: Dict[str, Any], entity_name: str) -> None:
    """Validate bulk load configuration.
    
    Args:
        entity_config: Configuration for a specific entity
        entity_name: Name of the entity for error messages
        
    Raises:
        ConfigurationError: If configuration is invalid
    """
    bulk_load = entity_config.get("bulk_load", True)  # Default to True
    source = entity_config.get("source", "faker")
    kafka_topic = entity_config.get("kafka_topic")
    
    # If bulk_load is true but source is CSV and no kafka_topic, that's problematic
    if bulk_load and source == "csv" and not kafka_topic:
        raise ConfigurationError(
            f"Entity '{entity_name}': bulk_load is true with CSV source but no kafka_topic specified. "
            "Either set bulk_load to false (for reference-only CSV data) or specify a kafka_topic "
            "to produce the CSV data to Kafka."
        )
    
    # If bulk_load is false but source is not CSV, warn about potential confusion
    if not bulk_load and source != "csv" and kafka_topic:
        # This is not an error, but could be confusing - user might expect data to be produced
        pass  # We'll just allow this configuration


def validate_schema_consistency(entity_config: Dict[str, Any], entity_name: str) -> None:
    """Validate schema consistency between CSV export and import.
    
    Args:
        entity_config: Configuration for a specific entity
        entity_name: Name of the entity for error messages
        
    Raises:
        ConfigurationError: If configuration is invalid
    """
    source = entity_config.get("source")
    schema = entity_config.get("schema", {})
    
    if not schema:
        if source == "csv":
            raise ConfigurationError(
                f"Entity '{entity_name}': CSV source requires a schema definition. "
                "Please provide a schema that matches your CSV file structure."
            )
        return
    
    # Check for required fields in CSV context
    if source == "csv":
        id_field = entity_config.get("id_field")
        if id_field and id_field not in schema:
            raise ConfigurationError(
                f"Entity '{entity_name}': id_field '{id_field}' is not defined in schema. "
                f"Please add '{id_field}' to the schema or update the id_field setting."
            )


def validate_correlation_config(config: Dict[str, Any]) -> List[str]:
    """Validate the entire correlation configuration.
    
    Args:
        config: The full correlation configuration
        
    Returns:
        List of warning messages (non-fatal issues)
        
    Raises:
        ConfigurationError: If configuration has fatal errors
    """
    warnings = []
    
    # Validate master_data section
    master_data = config.get("master_data", {})
    for entity_name, entity_config in master_data.items():
        try:
            validate_csv_export_configuration(entity_config, entity_name)
            validate_csv_source_configuration(entity_config, entity_name)
            validate_bulk_load_configuration(entity_config, entity_name)
            validate_schema_consistency(entity_config, entity_name)
        except ConfigurationError:
            raise  # Re-raise fatal errors
        except Exception as e:
            warnings.append(f"Entity '{entity_name}': Unexpected validation error: {e}")
    
    # Validate transactional_data section
    transactional_data = config.get("transactional_data", {})
    for entity_name, entity_config in transactional_data.items():
        try:
            # Transactional data doesn't typically use CSV import, but can use CSV export
            validate_csv_export_configuration(entity_config, entity_name)
            
            # Check for potential correlation issues
            correlation_fields = entity_config.get("correlation_fields", {})
            if correlation_fields:
                for field_name, correlation_config in correlation_fields.items():
                    ref_type = correlation_config.get("reference_type")
                    if ref_type and ref_type not in master_data:
                        warnings.append(
                            f"Transactional entity '{entity_name}' field '{field_name}' "
                            f"references unknown master data type '{ref_type}'. "
                            "This may cause correlation failures."
                        )
        except ConfigurationError:
            raise  # Re-raise fatal errors
        except Exception as e:
            warnings.append(f"Transactional entity '{entity_name}': Unexpected validation error: {e}")
    
    return warnings


def validate_and_warn(config: Dict[str, Any]) -> None:
    """Validate configuration and print warnings for non-fatal issues.
    
    Args:
        config: The full correlation configuration
        
    Raises:
        ConfigurationError: If configuration has fatal errors
    """
    warnings = validate_correlation_config(config)
    
    if warnings:
        print("Configuration warnings:")
        for warning in warnings:
            print(f"  ⚠️  {warning}")
        print()