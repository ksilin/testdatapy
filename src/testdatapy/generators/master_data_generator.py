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
        """Load data from CSV file."""
        file_path = entity_config.get("file")
        if not file_path:
            raise ValueError(f"CSV source requires 'file' parameter for {entity_type}")
        
        csv_path = Path(file_path)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        data = []
        with open(csv_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
        
        self.loaded_data[entity_type] = data
    
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
    
    def produce_all(self) -> None:
        """Bulk produce all loaded master data to Kafka."""
        if not self.producer:
            raise ValueError("No producer configured for bulk loading")
        
        master_config = self.config.config.get("master_data", {})
        
        for entity_type, entity_config in master_config.items():
            # Produce master data if it has a kafka_topic configured
            # bulk_load is now optional - default to True if kafka_topic is present
            has_topic = entity_config.get("kafka_topic") is not None
            bulk_load = entity_config.get("bulk_load", has_topic)  # Default to True if topic is configured
            
            if bulk_load and has_topic:
                self.produce_entity(entity_type, entity_config)
        
        self.producer.flush()
    
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
