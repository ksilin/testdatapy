"""Master data generator for bulk loading reference data."""
import csv
from pathlib import Path
from typing import Dict, Any, List, Optional
import uuid

from faker import Faker

from testdatapy.generators.base import DataGenerator
from testdatapy.generators.reference_pool import ReferencePool
from testdatapy.config.correlation_config import CorrelationConfig
from testdatapy.producers.base import KafkaProducer
from testdatapy.utils.data_flattening import DataFlattener


class MasterDataGenerator:
    """Handles bulk loading of master data from various sources.
    
    This generator loads master data (customers, products, etc.) from
    CSV files or generates it using Faker, then populates the reference
    pool and optionally bulk produces to Kafka.
    """
    
    def __init__(
        self,
        config: CorrelationConfig,
        reference_pool: ReferencePool,
        producer: Optional[KafkaProducer] = None
    ):
        """Initialize the master data generator.
        
        Args:
            config: Correlation configuration
            reference_pool: Reference pool to populate
            producer: Optional Kafka producer for bulk loading
        """
        self.config = config
        self.reference_pool = reference_pool
        self.producer = producer
        self.loaded_data: Dict[str, List[Dict[str, Any]]] = {}
        self.faker = Faker()
        self._sequence_counters: Dict[str, int] = {}
        
        # Track loading performance
        self._loading_stats: Dict[str, Dict[str, Any]] = {}
        
        # Initialize record cache in reference pool if not exists
        if not hasattr(self.reference_pool, '_record_cache'):
            self.reference_pool._record_cache = {}
    
    def load_all(self) -> None:
        """Load all master data defined in configuration."""
        import time
        start_time = time.time()
        
        master_config = self.config.config.get("master_data", {})
        
        # Sort by dependency order if needed
        entity_order = self._get_dependency_order(master_config)
        
        for entity_type in entity_order:
            if entity_type in master_config:
                entity_start = time.time()
                entity_config = master_config[entity_type]
                self.load_entity(entity_type, entity_config)
                entity_duration = time.time() - entity_start
                
                # Track loading performance for scale analysis
                self._loading_stats[entity_type] = {
                    "duration_seconds": entity_duration,
                    "record_count": len(self.loaded_data.get(entity_type, [])),
                    "records_per_second": len(self.loaded_data.get(entity_type, [])) / max(entity_duration, 0.001)
                }
        
        total_duration = time.time() - start_time
        self._loading_stats["_total"] = {
            "duration_seconds": total_duration,
            "total_records": sum(len(data) for data in self.loaded_data.values())
        }
    
    def load_entity(self, entity_type: str, entity_config: Dict[str, Any]) -> None:
        """Load a single entity type.
        
        Args:
            entity_type: Type of entity (e.g., 'customers')
            entity_config: Configuration for this entity
        """
        source = entity_config.get("source", "faker")
        
        if source == "csv":
            self._load_from_csv(entity_type, entity_config)
        elif source == "faker":
            self._generate_with_faker(entity_type, entity_config)
        else:
            raise ValueError(f"Unknown source type: {source}")
        
        # Populate reference pool
        self._populate_reference_pool(entity_type, entity_config)
        
        # Build indices after bulk loading
        if len(self.loaded_data.get(entity_type, [])) > 1000:  # Only for large datasets
            self.reference_pool.build_indices_for_entity(entity_type)
    
    def _load_from_csv(self, entity_type: str, entity_config: Dict[str, Any]) -> None:
        """Load data from CSV file with enhanced structure reconstruction and type conversion."""
        file_path = entity_config.get("file")
        if not file_path:
            raise ValueError(f"CSV source requires 'file' parameter for {entity_type}")
        
        csv_path = Path(file_path)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        # Load raw CSV data
        raw_data = []
        with open(csv_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                raw_data.append(dict(row))  # Convert OrderedDict to regular dict
        
        if not raw_data:
            print(f"Warning: CSV file {file_path} is empty")
            self.loaded_data[entity_type] = []
            return
        
        # Detect if CSV data is flattened (contains dot-notation keys)
        sample_row = raw_data[0]
        has_flattened_keys = any('.' in key for key in sample_row.keys())
        
        if has_flattened_keys:
            # Reconstruct nested structure from flattened CSV
            print(f"📋 Reconstructing nested structure for {entity_type} from flattened CSV")
            structured_data = []
            for row in raw_data:
                try:
                    unflattened = DataFlattener.unflatten_dict(row)
                    structured_data.append(unflattened)
                except Exception as e:
                    print(f"Warning: Failed to unflatten row in {entity_type}: {e}")
                    # Fallback to flat structure
                    structured_data.append(row)
        else:
            # CSV data is already flat/structured
            structured_data = raw_data
        
        # Apply type conversions based on schema if available
        schema = entity_config.get("schema", {})
        if schema:
            typed_data = []
            for record in structured_data:
                try:
                    typed_record = self._apply_schema_types(record, schema)
                    typed_data.append(typed_record)
                except Exception as e:
                    print(f"Warning: Failed to apply types to record in {entity_type}: {e}")
                    # Fallback to original record
                    typed_data.append(record)
            
            self.loaded_data[entity_type] = typed_data
        else:
            self.loaded_data[entity_type] = structured_data
        
        print(f"✅ Loaded {len(self.loaded_data[entity_type])} {entity_type} records from CSV")
    
    def _apply_schema_types(self, record: Dict[str, Any], schema: Dict[str, Any], path: str = "") -> Dict[str, Any]:
        """Apply type conversions to a record based on schema definitions.
        
        Args:
            record: Record to convert
            schema: Schema with type definitions 
            path: Current path for nested objects
            
        Returns:
            Record with proper types applied
        """
        if not isinstance(record, dict) or not isinstance(schema, dict):
            return record
        
        converted = {}
        
        for key, value in record.items():
            current_path = f"{path}.{key}" if path else key
            
            # Find matching schema field (handles both nested and flat schemas)
            field_schema = None
            if key in schema:
                field_schema = schema[key]
            elif current_path in schema:
                field_schema = schema[current_path]
            
            if field_schema and isinstance(field_schema, dict):
                field_type = field_schema.get("type")
                
                if field_type and isinstance(value, str) and value:
                    # Convert string values based on type
                    try:
                        if field_type == "integer":
                            converted[key] = int(value)
                        elif field_type == "float":
                            converted[key] = float(value)
                        elif field_type == "boolean":
                            # Only convert known boolean values, otherwise keep as string
                            lower_value = value.lower()
                            if lower_value in ("true", "1", "yes", "on"):
                                converted[key] = True
                            elif lower_value in ("false", "0", "no", "off"):
                                converted[key] = False
                            else:
                                raise ValueError(f"Invalid boolean value: {value}")
                        elif field_type == "timestamp":
                            # Handle various timestamp formats
                            if value.isdigit():
                                converted[key] = int(value)  # Unix timestamp
                            else:
                                converted[key] = value  # Keep as string for ISO format
                        elif field_type == "timestamp_millis":
                            converted[key] = int(value) if value.isdigit() else value
                        else:
                            converted[key] = value  # Keep as string for other types
                    except (ValueError, TypeError) as e:
                        print(f"Warning: Failed to convert {current_path} to {field_type}: {e}")
                        converted[key] = value  # Keep original value
                elif isinstance(value, dict):
                    # Recursively convert nested objects
                    if field_type == "object" and "properties" in field_schema:
                        converted[key] = self._apply_schema_types(value, field_schema["properties"], current_path)
                    else:
                        converted[key] = self._apply_schema_types(value, schema, current_path)
                else:
                    converted[key] = value
            elif isinstance(value, dict):
                # Handle nested objects without explicit schema
                converted[key] = self._apply_schema_types(value, schema, current_path)
            else:
                converted[key] = value
        
        return converted
    
    def _generate_with_faker(self, entity_type: str, entity_config: Dict[str, Any]) -> None:
        """Generate data using Faker."""
        count = entity_config.get("count", 100)
        schema = entity_config.get("schema", {})
        
        # Default schema if not provided
        if not schema:
            schema = self._get_default_schema(entity_type)
        
        data = []
        for i in range(count):
            record = {}
            # First pass: generate non-reference fields
            for field_name, field_config in schema.items():
                if field_config.get("type") != "reference":
                    record[field_name] = self._generate_field(
                        field_name, field_config, entity_type, i
                    )
            
            # Second pass: generate reference fields (now that other fields exist)
            for field_name, field_config in schema.items():
                if field_config.get("type") == "reference":
                    record[field_name] = self._generate_field_with_record(
                        field_name, field_config, entity_type, i, record
                    )
            data.append(record)
        
        self.loaded_data[entity_type] = data
    
    def _generate_field(
        self, 
        field_name: str, 
        field_config: Dict[str, Any],
        entity_type: str,
        index: int
    ) -> Any:
        """Generate a single field value based on configuration."""
        field_type = field_config.get("type", "string")
        
        if field_type == "string":
            format_str = field_config.get("format")
            template_str = field_config.get("template")
            
            if template_str:
                # Handle template strings with field substitution
                return self._process_template(template_str, field_config, {"index": index})
            elif format_str and "{seq:" in format_str:
                # Handle sequential numbering
                return self._format_sequential(format_str, entity_type, field_name)
            elif format_str:
                return format_str.format(index=index)
            else:
                return f"{field_name}_{index}"
        
        elif field_type == "object":
            # Handle nested object structures
            properties = field_config.get("properties", {})
            obj = {}
            # First pass: generate non-reference fields
            for prop_name, prop_config in properties.items():
                if prop_config.get("type") != "reference":
                    obj[prop_name] = self._generate_field(prop_name, prop_config, entity_type, index)
            
            # Second pass: generate reference fields with object context
            for prop_name, prop_config in properties.items():
                if prop_config.get("type") == "reference":
                    # Build the correct context path based on field_name
                    if field_name == "Vehicle":
                        # We're generating inside Vehicle object, so context is full.Vehicle
                        temp_record = {"full": {"Vehicle": obj}}
                    elif field_name == "Job":
                        # We're generating inside Job object, so context is full.Job
                        temp_record = {"full": {"Job": obj}}
                    elif field_name == "full":
                        # We're at the top level "full" object
                        temp_record = {"full": obj}
                    else:
                        # Generic nested object
                        temp_record = {field_name: obj}
                    resolved_value = self._resolve_reference_field(prop_config, temp_record)
                    if resolved_value is not None:
                        obj[prop_name] = resolved_value
            return obj
        
        elif field_type == "faker":
            method = field_config.get("method", "word")
            text = field_config.get("text", "")
            format_type = field_config.get("format", "")
            faker_method = getattr(self.faker, method, None)
            if faker_method:
                if text and method == "bothify":
                    return faker_method(text=text)
                elif method == "date_time_between":
                    # Handle relative date ranges
                    start_date = field_config.get("start_date", "-1y")
                    end_date = field_config.get("end_date", "+1y")
                    
                    # Convert relative dates to actual dates
                    from datetime import datetime, timedelta
                    import re
                    
                    now = datetime.now()
                    
                    # Parse relative dates like "-1w", "+2d", etc.
                    def parse_relative_date(date_str):
                        if date_str.startswith('+') or date_str.startswith('-'):
                            match = re.match(r'([+-])(\d+)([wdmy])', date_str)
                            if match:
                                sign, amount, unit = match.groups()
                                amount = int(amount)
                                if sign == '-':
                                    amount = -amount
                                
                                if unit == 'd':
                                    delta = timedelta(days=amount)
                                elif unit == 'w':
                                    delta = timedelta(weeks=amount)
                                elif unit == 'm':
                                    delta = timedelta(days=amount * 30)  # Approximate
                                elif unit == 'y':
                                    delta = timedelta(days=amount * 365)  # Approximate
                                else:
                                    delta = timedelta(days=amount)
                                
                                return now + delta
                        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    
                    start_dt = parse_relative_date(start_date)
                    end_dt = parse_relative_date(end_date)
                    
                    generated_dt = faker_method(start_date=start_dt, end_date=end_dt)
                    
                    # Return in requested format
                    if format_type == "iso8601":
                        return generated_dt.isoformat()
                    else:
                        return generated_dt
                else:
                    return faker_method()
            else:
                return self.faker.word()
        
        elif field_type == "uuid":
            return str(uuid.uuid4())
        
        elif field_type == "integer":
            min_val = field_config.get("min", 0)
            max_val = field_config.get("max", 1000)
            return self.faker.random_int(min=min_val, max=max_val)
        
        elif field_type == "float":
            min_val = field_config.get("min", 0.0)
            max_val = field_config.get("max", 1000.0)
            return round(self.faker.random.uniform(min_val, max_val), 2)
        
        elif field_type == "choice":
            # Handle choice fields
            choices = field_config.get("choices", [])
            if choices:
                return self.faker.random.choice(choices)
            else:
                return f"{field_name}_choice"
        
        elif field_type == "weighted_choice":
            # Handle weighted choice fields
            return self._generate_weighted_choice(field_config)
        
        elif field_type == "template":
            # Handle template fields with variable substitution
            template_str = field_config.get("template", "")
            if template_str:
                return self._process_template(template_str, field_config, {"index": index})
            else:
                return f"{field_name}_template"
        
        elif field_type == "timestamp":
            # Handle timestamp fields
            from datetime import datetime
            format_type = field_config.get("format", "iso8601")
            if format_type == "iso8601":
                return datetime.now().isoformat()
            else:
                return int(datetime.now().timestamp())
        
        elif field_type == "timestamp_millis":
            # Handle timestamp in milliseconds
            from datetime import datetime
            return int(datetime.now().timestamp() * 1000)
        
        elif field_type == "reference":
            # Handle reference fields with formatting support
            # For second pass, we need the current record context
            return None  # Will be handled in second pass
        
        else:
            return f"{field_name}_value"
    
    def _generate_field_with_record(
        self, 
        field_name: str, 
        field_config: Dict[str, Any],
        entity_type: str,
        index: int,
        record: Dict[str, Any]
    ) -> Any:
        """Generate a field value with access to the current record for reference resolution."""
        field_type = field_config.get("type", "string")
        
        if field_type == "reference":
            return self._resolve_reference_field(field_config, record)
        else:
            # For non-reference fields, use the normal method
            return self._generate_field(field_name, field_config, entity_type, index)
    
    def _format_sequential(self, format_str: str, entity_type: str, field_name: str) -> str:
        """Handle sequential formatting like PROD_{seq:05d}."""
        # Extract the sequence format
        import re
        match = re.search(r'\{seq:(\d+)d\}', format_str)
        if match:
            width = int(match.group(1))
            counter_key = f"{entity_type}_{field_name}"
            
            if counter_key not in self._sequence_counters:
                self._sequence_counters[counter_key] = 1
            
            current = self._sequence_counters[counter_key]
            self._sequence_counters[counter_key] += 1
            
            # Replace the sequence placeholder
            seq_str = str(current).zfill(width)
            return format_str.replace(match.group(0), seq_str)
        
        return format_str
    
    def _get_default_schema(self, entity_type: str) -> Dict[str, Dict[str, Any]]:
        """Get default schema for common entity types."""
        schemas = {
            "customers": {
                "customer_id": {"type": "string", "format": "CUST_{seq:04d}"},
                "name": {"type": "faker", "method": "name"},
                "email": {"type": "faker", "method": "email"},
                "created_at": {"type": "faker", "method": "iso8601"}
            },
            "products": {
                "product_id": {"type": "string", "format": "PROD_{seq:04d}"},
                "name": {"type": "faker", "method": "word"},
                "price": {"type": "float", "min": 10.0, "max": 1000.0},
                "category": {"type": "faker", "method": "word"}
            }
        }
        
        return schemas.get(entity_type, {
            f"{entity_type[:-1]}_id": {"type": "string", "format": f"{entity_type.upper()[:4]}_{'{seq:04d}'}"},
            "name": {"type": "faker", "method": "word"}
        })
    
    def _populate_reference_pool(self, entity_type: str, entity_config: Dict[str, Any]) -> None:
        """Populate the reference pool with loaded data."""
        id_field = entity_config.get("id_field", f"{entity_type[:-1]}_id")
        data = self.loaded_data.get(entity_type, [])
        
        # Extract IDs and add to reference pool
        ids = []
        for record in data:
            record_id = None
            
            # Handle nested ID fields
            if "." in id_field:
                record_id = self._get_nested_field_value(record, id_field)
            elif id_field in record:
                record_id = record[id_field]
            
            if record_id:
                ids.append(str(record_id))
        
        if ids:
            self.reference_pool.add_references(entity_type, ids)
            
            # Cache full records for efficient lookups
            if entity_type not in self.reference_pool._record_cache:
                self.reference_pool._record_cache[entity_type] = {}
            
            for record in data:
                record_id = None
                if "." in id_field:
                    record_id = self._get_nested_field_value(record, id_field)
                elif id_field in record:
                    record_id = record[id_field]
                
                if record_id:
                    self.reference_pool._record_cache[entity_type][str(record_id)] = record
            
            # Build indices for efficient correlation lookups
            self._build_correlation_indices(entity_type, data, entity_config)
    
    def produce_all(self) -> None:
        """Bulk produce all loaded master data to Kafka and export CSV files."""
        master_config = self.config.config.get("master_data", {})
        kafka_errors = []
        
        for entity_type, entity_config in master_config.items():
            # Check if Kafka production is needed
            has_topic = entity_config.get("kafka_topic") is not None
            bulk_load = entity_config.get("bulk_load", has_topic)  # Default to True if topic is configured
            
            # Produce to Kafka if enabled (with error handling)
            if bulk_load and has_topic:
                try:
                    if not self.producer:
                        raise ValueError(f"No producer configured for bulk loading entity '{entity_type}'")
                    self.produce_entity(entity_type, entity_config)
                except Exception as e:
                    kafka_errors.append(f"Failed to produce {entity_type} to Kafka: {e}")
            
            # CSV export (always runs independent of Kafka production success/failure)
            if "csv_export" in entity_config:
                try:
                    self._export_to_csv(entity_type, entity_config)
                except Exception as e:
                    print(f"Warning: Failed to export {entity_type} to CSV: {e}")
        
        # Flush producer only if we have one and used it successfully
        if self.producer and not kafka_errors:
            kafka_production_attempted = any(
                entity_config.get("bulk_load", entity_config.get("kafka_topic") is not None) and 
                entity_config.get("kafka_topic") is not None
                for entity_config in master_config.values()
            )
            if kafka_production_attempted:
                self.producer.flush()
        
        # Raise Kafka errors after CSV export is complete
        if kafka_errors:
            raise ValueError("; ".join(kafka_errors))
    
    def produce_entity(self, entity_type: str, entity_config: Dict[str, Any]) -> None:
        """Produce a single entity type to Kafka.
        
        Args:
            entity_type: Type of entity to produce
            entity_config: Configuration for this entity
        """
        if not self.producer:
            return
        
        topic = entity_config.get("kafka_topic")
        if not topic:
            raise ValueError(f"No kafka_topic configured for {entity_type}")
        
        data = self.loaded_data.get(entity_type, [])
        
        # Use consistent key field priority logic: key_field > id_field > default
        key_field = self.config.get_key_field(entity_type, is_master=True)
        
        # Create topic-specific producer if needed
        if not hasattr(self.producer, '_topic_producers'):
            self.producer._topic_producers = {}
        
        if topic not in self.producer._topic_producers:
            from testdatapy.producers import JsonProducer
            self.producer._topic_producers[topic] = JsonProducer(
                bootstrap_servers=self.producer.bootstrap_servers,
                topic=topic,
                config=self.producer.config,
                key_field=key_field,  # Pass key_field for automatic key extraction
                auto_create_topic=True
            )
        
        topic_producer = self.producer._topic_producers[topic]
        
        for record in data:
            # Let JsonProducer handle key extraction using its key_field parameter
            # This ensures consistency with other producers and automatic key extraction
            topic_producer.produce(
                key=None,  # Let JsonProducer extract key from value using key_field
                value=record
            )
        
        # CSV export is now handled in produce_all() method
    
    def get_loaded_data(self, entity_type: str) -> List[Dict[str, Any]]:
        """Get loaded data for a specific entity type."""
        return self.loaded_data.get(entity_type, [])
    
    def get_sample(self, entity_type: str, count: int = 5) -> List[Dict[str, Any]]:
        """Get sample records from loaded data."""
        data = self.loaded_data.get(entity_type, [])
        return data[:count]
    
    # TODO - this is bullshit - too specialized for single use case
    def _get_dependency_order(self, master_config: Dict[str, Any]) -> List[str]:
        """Get dependency order for master data loading."""
        vehicle_order = ["appointments", "customers", "products"]
        
        # Ensure all configured entities are included
        all_entities = list(master_config.keys())
        ordered_entities = []
        
        # Add priority entities first
        for entity in vehicle_order:
            if entity in all_entities:
                ordered_entities.append(entity)
                all_entities.remove(entity)
        
        # Add remaining entities
        ordered_entities.extend(all_entities)
        
        return ordered_entities
    
    def _get_nested_field_value(self, record: Dict[str, Any], field_path: str) -> Any:
        """Get value from nested field path like 'full.Job.cKeyJob'."""
        parts = field_path.split(".")
        current = record
        
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        
        return current
    
    def _process_template(self, template_str: str, field_config: Dict[str, Any], record: Dict[str, Any]) -> str:
        """Process template strings with field substitution."""
        import re
        
        # Get template fields configuration
        template_fields = field_config.get("fields", {})
        
        # Find all template variables in format {variable_name}
        variables = re.findall(r'\{([^}]+)\}', template_str)
        
        result = template_str
        for var_name in variables:
            # Generate value for this template variable
            if var_name in template_fields:
                var_config = template_fields[var_name]
                var_value = self._generate_field_value_from_config(var_config, record)
            elif var_name in record:
                # Direct field reference
                var_value = record[var_name]
            else:
                # Default fallback
                var_value = f"unknown_{var_name}"
            
            # Replace the variable in the template
            result = result.replace(f"{{{var_name}}}", str(var_value))
        
        return result
    
    def _apply_reference_formatting(self, value: Any, format_type: str) -> Any:
        """Apply formatting to reference field values."""
        if format_type == "license_plate" and isinstance(value, str) and len(value) == 6:
            # Convert MAB123 to M-AB 123 format
            return f"{value[0]}-{value[1:3]} {value[3:6]}"
        return value
    
    def _apply_time_offset(self, value: Any, offset_min: int, offset_max: int) -> Any:
        """Apply random time offset to a timestamp value."""
        import random
        from datetime import datetime, timedelta
        
        if isinstance(value, str):
            try:
                # Parse ISO8601 timestamp
                dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                
                # Apply random offset
                offset_minutes = random.uniform(offset_min, offset_max)
                new_dt = dt + timedelta(minutes=offset_minutes)
                
                # Return in same format
                return new_dt.isoformat()
            except Exception:
                # If parsing fails, return original value
                return value
        elif isinstance(value, datetime):
            # Apply random offset
            offset_minutes = random.uniform(offset_min, offset_max)
            new_dt = value + timedelta(minutes=offset_minutes)
            return new_dt.isoformat()
        else:
            # Return original value if not a recognized timestamp format
            return value
    
    def _resolve_reference_field(self, field_config: Dict[str, Any], record: Dict[str, Any]) -> Any:
        """Resolve reference field values with support for nested paths, formatting, and time offsets."""
        source = field_config.get("source", "")
        format_type = field_config.get("format", "")
        offset_minutes_min = field_config.get("offset_minutes_min")
        offset_minutes_max = field_config.get("offset_minutes_max")
        
        # Handle self-references (most common case for master data)
        if source.startswith("self."):
            field_path = source[5:]  # Remove "self."
            value = self._get_nested_field_value(record, field_path)
            
            # Apply time offset if specified and value is a timestamp
            if offset_minutes_min is not None and offset_minutes_max is not None:
                value = self._apply_time_offset(value, offset_minutes_min, offset_minutes_max)
            
            return self._apply_reference_formatting(value, format_type)
        
        # For master data, we typically don't have cross-entity references
        # but we can add a fallback
        return f"reference_to_{source}"
    
    def _get_nested_field_value(self, record: Dict[str, Any], field_path: str) -> Any:
        """Get value from nested field path like 'full.Vehicle.cLicenseNrCleaned'."""
        parts = field_path.split(".")
        current = record
        
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        
        return current
    
    def _generate_field_value_from_config(self, field_config: Dict[str, Any], record: Dict[str, Any]) -> Any:
        """Generate field value from configuration (used by template fields)."""
        field_type = field_config.get("type")
        
        if field_type == "string":
            if "format" in field_config:
                # Handle formatted strings like license plates
                format_str = field_config["format"]
                return self._process_format_string(format_str)
            else:
                return field_config.get("initial_value", "default_value")
        
        elif field_type == "choice":
            choices = field_config.get("choices", [])
            return self.faker.random.choice(choices) if choices else None
        
        elif field_type == "weighted_choice":
            # Handle weighted choice fields
            return self._generate_weighted_choice(field_config)
        
        elif field_type == "integer":
            min_val = field_config.get("min", 0)
            max_val = field_config.get("max", 1000)
            return self.faker.random_int(min=min_val, max=max_val)
        
        elif field_type == "float":
            min_val = field_config.get("min", 0.0)
            max_val = field_config.get("max", 1000.0)
            return round(self.faker.random.uniform(min_val, max_val), 2)
        
        else:
            # Fallback to basic generation
            return f"template_value"
    
    def _process_format_string(self, format_str: str) -> str:
        """Process format strings with random generators."""
        import re
        
        result = format_str
        
        # Handle {random_letters:N} patterns
        letter_matches = re.findall(r'\{random_letters:(\d+)\}', result)
        for count in letter_matches:
            random_letters = ''.join(self.faker.random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=int(count)))
            result = result.replace(f'{{random_letters:{count}}}', random_letters, 1)
        
        # Handle {random_digits:N} patterns
        digit_matches = re.findall(r'\{random_digits:(\d+)\}', result)
        for count in digit_matches:
            random_digits = ''.join(self.faker.random.choices('0123456789', k=int(count)))
            result = result.replace(f'{{random_digits:{count}}}', random_digits, 1)
        
        return result
    
    def _generate_weighted_choice(self, field_config: Dict[str, Any]) -> Any:
        """Generate weighted choice with precise distributions."""
        choices = field_config.get("choices", [])
        weights = field_config.get("weights", [])
        
        if not choices:
            return None
        
        if not weights or len(weights) != len(choices):
            # Fall back to uniform choice if weights are missing or mismatched
            return self.faker.random.choice(choices)
        
        # Ensure weights are normalized and sum to 1.0 for precision
        total_weight = sum(weights)
        normalized_weights = [w / total_weight for w in weights]
        
        # Use random.choices for proper weighted selection
        return self.faker.random.choices(choices, weights=normalized_weights)[0]
    
    def get_loading_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get loading performance statistics for optimization analysis."""
        return self._loading_stats
    
    def get_memory_usage(self) -> Dict[str, Any]:
        """Get memory usage statistics for loaded data."""
        import sys
        
        usage = {
            "loaded_entities": len(self.loaded_data),
            "total_records": sum(len(data) for data in self.loaded_data.values()),
            "entities": {}
        }
        
        for entity_type, data in self.loaded_data.items():
            try:
                entity_size = sys.getsizeof(data)
                usage["entities"][entity_type] = {
                    "record_count": len(data),
                    "memory_bytes": entity_size,
                    "memory_mb": entity_size / (1024 * 1024)
                }
            except:
                usage["entities"][entity_type] = {
                    "record_count": len(data),
                    "memory_bytes": -1,
                    "memory_mb": -1
                }
        
        return usage
    
    def _export_to_csv(self, entity_type: str, entity_config: Dict[str, Any]) -> None:
        """Export loaded entity data to CSV file.
        
        Args:
            entity_type: Type of entity to export
            entity_config: Configuration for this entity including csv_export
        """
        csv_config = entity_config["csv_export"]
        
        # Parse CSV configuration
        if isinstance(csv_config, str):
            csv_file = csv_config
            include_headers = True
            flatten_objects = True
            delimiter = ","
        else:
            csv_file = csv_config["file"]
            include_headers = csv_config.get("include_headers", True)
            flatten_objects = csv_config.get("flatten_objects", True)
            delimiter = csv_config.get("delimiter", ",")
        
        # Get data to export
        data = self.loaded_data.get(entity_type, [])
        if not data:
            return
        
        # Flatten objects if requested
        if flatten_objects:
            data = [self._flatten_record(record) for record in data]
        
        # Ensure parent directory exists
        csv_path = Path(csv_file)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write CSV
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            if data:
                writer = csv.DictWriter(f, fieldnames=data[0].keys(), delimiter=delimiter)
                if include_headers:
                    writer.writeheader()
                writer.writerows(data)
        
        print(f"✅ Exported {len(data)} {entity_type} records to {csv_file}")
    
    def _flatten_record(self, record: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
        """Flatten nested objects into dot-notation fields.
        
        Args:
            record: Record with potentially nested objects
            prefix: Current field prefix for nested fields
            
        Returns:
            Flattened record with dot-notation field names
        """
        # Use the new DataFlattener utility for enhanced functionality
        return DataFlattener.flatten_for_csv(record)
    
    def _flatten_record_legacy(self, record: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
        """Legacy flatten implementation for backward compatibility.
        
        Args:
            record: Record with potentially nested objects
            prefix: Current field prefix for nested fields
            
        Returns:
            Flattened record with dot-notation field names
        """
        flattened = {}
        
        for key, value in record.items():
            full_key = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, dict):
                # Recursively flatten nested objects
                flattened.update(self._flatten_record_legacy(value, full_key))
            else:
                # Convert non-dict values to appropriate CSV-safe format
                if value is None:
                    flattened[full_key] = ""
                elif isinstance(value, bool):
                    flattened[full_key] = "true" if value else "false"
                else:
                    flattened[full_key] = str(value)
        
        return flattened
    
    def _build_correlation_indices(self, entity_type: str, data: List[Dict[str, Any]], entity_config: Dict[str, Any]) -> None:
        """Build indices for efficient correlation lookups.
        
        This method creates field indices for commonly used correlation fields to enable O(1) lookups
        instead of O(n) linear searches, which is critical for performance at scale.
        
        Args:
            entity_type: Type of entity (e.g., 'appointments')
            data: List of loaded data records
            entity_config: Configuration for this entity
        """
        if not data:
            return
            
        # Get schema to understand field structure
        schema = entity_config.get("schema", {})
        
        # Common correlation field paths that need indexing for performance
        index_field_paths = [
            "full.Vehicle.cLicenseNr",
            "full.Vehicle.cLicenseNrCleaned", 
            "full.Customer.cKeyCustomer",
            "full.Job.cKeyJob",
            "jobid",
            "id"
        ]
        
        # Also check for any reference fields in the config that might be used for correlation
        # by scanning the entire configuration for references to this entity
        try:
            config_str = str(self.config.config)
            if f"{entity_type}." in config_str:
                # Extract potential field paths from references
                import re
                ref_patterns = re.findall(rf'{entity_type}\.([a-zA-Z0-9_.]+)', config_str)
                for pattern in ref_patterns:
                    if pattern not in index_field_paths:
                        index_field_paths.append(pattern)
        except Exception:
            # If pattern extraction fails, just use the common paths
            pass
        
        # Build indices for each field path
        indexed_count = 0
        for record in data:
            # Get the record ID for indexing
            id_field = entity_config.get("id_field", "id")
            record_id = record.get(id_field)
            if not record_id:
                continue
                
            # Index each field path
            for field_path in index_field_paths:
                field_value = self._get_nested_field_value(record, field_path)
                if field_value is not None:
                    # Add to reference pool index for O(1) lookup
                    self.reference_pool.add_field_index(entity_type, field_path, str(record_id), str(field_value))
                    indexed_count += 1
        
        if indexed_count > 0:
            print(f"🔍 Built {indexed_count} correlation indices for {entity_type} across {len(index_field_paths)} field paths")
    
    def _get_nested_field_value(self, record: Dict[str, Any], field_path: str) -> Any:
        """Get value from nested field path like 'full.Vehicle.cLicenseNr'.
        
        Args:
            record: Record to traverse
            field_path: Dot-notation field path
            
        Returns:
            Field value or None if not found
        """
        parts = field_path.split(".")
        current = record
        
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        
        return current
