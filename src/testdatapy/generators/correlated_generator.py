"""Correlated data generator for creating related test data."""
import random
import time
from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable
import uuid

from testdatapy.generators.base import DataGenerator
from testdatapy.generators.reference_pool import ReferencePool
from testdatapy.config.correlation_config import CorrelationConfig


class CorrelatedDataGenerator(DataGenerator):
    """Generates test data with proper references to master data.
    
    This generator creates transactional data that properly references
    master data entities, ensuring referential integrity for testing
    Flink joins and aggregations.
    """
    
    def __init__(
        self,
        entity_type: str,
        config: CorrelationConfig,
        reference_pool: ReferencePool,
        rate_per_second: float = None,
        max_messages: int | None = None,
    ):
        """Initialize the correlated data generator.
        
        Args:
            entity_type: Type of entity to generate (e.g., 'orders', 'payments')
            config: Correlation configuration
            reference_pool: Pool of reference IDs for relationships
            rate_per_second: Override rate from config
            max_messages: Maximum number of messages to generate
        """
        self.entity_type = entity_type
        self.config = config
        self.reference_pool = reference_pool
        
        # Get entity configuration
        self.entity_config = config.get_transaction_config(entity_type)
        
        # Add sequence counter for string formatting
        self._sequence_counters = {}
        
        # Add record cache for reference lookups
        if not hasattr(reference_pool, '_record_cache'):
            reference_pool._record_cache = {}
        
        # Set rate from config or override
        if rate_per_second is None:
            rate_per_second = self.entity_config.get("rate_per_second", 10.0)
        
        super().__init__(rate_per_second, max_messages)
        
        # Pre-calculate sleep time for rate limiting
        self._sleep_time = 1.0 / rate_per_second if rate_per_second > 0 else 0
    
    def generate(self) -> Iterator[dict[str, Any]]:
        """Generate correlated data records.
        
        Yields:
            Dict containing generated data with proper references
        """
        start_time = time.time()
        last_message_time = start_time
        
        while self.should_continue():
            # Generate base record
            record = self._generate_record()
            
            yield record
            self.increment_count()
            
            # Rate limiting
            if self._sleep_time > 0:
                current_time = time.time()
                elapsed = current_time - last_message_time
                if elapsed < self._sleep_time:
                    time.sleep(self._sleep_time - elapsed)
                last_message_time = time.time()
    
    def _generate_record(self) -> Dict[str, Any]:
        """Generate a single data record with relationships."""
        record = {}
        
        # Add ID field (if not referenced from elsewhere AND not in derived_fields)
        id_field = self.entity_config.get("id_field", f"{self.entity_type[:-1]}_id")
        relationships = self.entity_config.get("relationships", {})
        derived_fields = self.entity_config.get("derived_fields", {})
        
        # Only auto-generate if not in relationships AND not in derived_fields
        if id_field not in relationships and id_field not in derived_fields:
            record[id_field] = str(uuid.uuid4())
        
        # Generate fields from relationships and store mapped field values
        mapped_field_values = {}
        for field_name, rel_config in relationships.items():
            relationship_value = self._generate_relationship_value(field_name, rel_config)
            record[field_name] = relationship_value
            
            # Handle mapped_field if specified and correlation exists
            mapped_field_config = rel_config.get("mapped_field")
            if mapped_field_config and relationship_value is not None:
                mapped_values = self._apply_mapped_field(relationship_value, rel_config, mapped_field_config)
                mapped_field_values.update(mapped_values)
        
        # Generate derived fields (these take precedence over auto-generation)
        # First pass: Generate key_only fields so they're available for template references
        key_only_fields = {}
        regular_fields = {}
        
        for field_name, field_config in derived_fields.items():
            if field_config.get("key_only", False):
                field_value = self._generate_derived_field(field_name, field_config, record)
                key_only_fields[field_name] = field_value
                # Make key_only fields temporarily available in record for template processing
                record[field_name] = field_value
            else:
                regular_fields[field_name] = field_config
        
        # Second pass: Generate regular fields (which can now reference key_only fields)
        for field_name, field_config in regular_fields.items():
            # Check if this field has a mapped value from relationships
            if field_name in mapped_field_values:
                # Use mapped value instead of generating
                record[field_name] = mapped_field_values[field_name]
            else:
                # Generate normally
                field_value = self._generate_derived_field(field_name, field_config, record)
                record[field_name] = field_value
        
        # Third pass: Remove key_only fields from record and store separately
        for field_name in key_only_fields:
            if field_name in record:
                del record[field_name]
        
        # Store key_only fields in a special record attribute
        if key_only_fields:
            record["_key_only_fields"] = key_only_fields
        
        # Remove relationship fields from final record (they're only used internally for correlation)
        for field_name in relationships.keys():
            if field_name in record:
                del record[field_name]
        
        # Track in reference pool for future references
        # Always add to main pool so other generators can reference this entity
        self.reference_pool.add_references(self.entity_type, [record[id_field]])
        
        # Store the full record for reference lookups
        if self.entity_type not in self.reference_pool._record_cache:
            self.reference_pool._record_cache[self.entity_type] = {}
        self.reference_pool._record_cache[self.entity_type][record[id_field]] = record
        
        # Also track in recent items if configured for recency bias
        if self.entity_config.get("track_recent", False):
            self.reference_pool.add_recent(self.entity_type, record[id_field])
        
        return record
    
    def _generate_relationship_value(self, field_name: str, rel_config: Dict[str, Any]) -> Any:
        """Generate value for a relationship field."""
        if rel_config.get("type") == "array":
            return self._generate_array_relationship(field_name, rel_config)
        else:
            return self._generate_simple_relationship(field_name, rel_config)
    
    def _generate_simple_relationship(self, field_name: str, rel_config: Dict[str, Any]) -> Any:
        """Generate a simple reference value with percentage-based correlation support."""
        references = rel_config.get("references", "")
        if not references:
            return None
        
        # Handle percentage-based correlation
        percentage = rel_config.get("percentage")
        if percentage is not None:
            # Deterministic percentage-based selection
            if not self._should_correlate(percentage):
                return None  # No correlation for this record
        
        # Parse reference format: "entity_type.field_name" or complex paths
        ref_parts = references.split(".")
        if len(ref_parts) < 2:
            return None
        
        ref_type = ref_parts[0]
        ref_field_path = ".".join(ref_parts[1:])
        
        # Handle temporal relationships
        if rel_config.get("recency_bias", False) and self.reference_pool.get_recent(ref_type):
            try:
                ref_id = self.reference_pool.get_random_recent(ref_type, bias_recent=True)
                return self._get_reference_field_value(ref_type, ref_id, ref_field_path)
            except ValueError:
                # Fall back to regular random if no recent items
                pass
        
        # Handle weighted distribution
        distribution = rel_config.get("distribution", "uniform")
        
        if distribution == "weighted":
            weight_func = self._get_weight_function(rel_config.get("weight_field"))
            ref_id = self.reference_pool.get_weighted_random(ref_type, weight_func)
            return self._get_reference_field_value(ref_type, ref_id, ref_field_path)
        elif distribution == "zipf":
            # Implement Zipf distribution
            alpha = rel_config.get("alpha", 1.5)
            weight_func = self._get_zipf_weight_function(alpha)
            ref_id = self.reference_pool.get_weighted_random(ref_type, weight_func)
            return self._get_reference_field_value(ref_type, ref_id, ref_field_path)
        else:
            # Default uniform distribution
            ref_id = self.reference_pool.get_random(ref_type)
            return self._get_reference_field_value(ref_type, ref_id, ref_field_path)
    
    def _generate_array_relationship(self, field_name: str, rel_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate an array of related items."""
        min_items = rel_config.get("min_items", 1)
        max_items = rel_config.get("max_items", 5)
        item_count = random.randint(min_items, max_items)
        
        items = []
        item_schema = rel_config.get("item_schema", {})
        
        for _ in range(item_count):
            item = {}
            
            # Generate each field in the item
            for item_field, item_config in item_schema.items():
                if isinstance(item_config, dict) and "references" in item_config:
                    # It's a reference
                    item[item_field] = self._generate_simple_relationship(item_field, item_config)
                else:
                    # It's a value specification
                    item[item_field] = self._generate_field_value(item_field, item_config)
            
            items.append(item)
        
        return items
    
    def _generate_field_value(self, field_name: str, field_config: Dict[str, Any]) -> Any:
        """Generate a value based on field configuration."""
        field_type = field_config.get("type", "string")
        
        if field_type == "integer":
            min_val = field_config.get("min", 0)
            max_val = field_config.get("max", 100)
            return random.randint(min_val, max_val)
        elif field_type == "float":
            min_val = field_config.get("min", 0.0)
            max_val = field_config.get("max", 100.0)
            return round(random.uniform(min_val, max_val), 2)
        elif field_type == "string":
            return field_config.get("default", f"{field_name}_value")
        else:
            return None
    
    def _generate_derived_field(self, field_name: str, field_config: Dict[str, Any], record: Dict[str, Any]) -> Any:
        """Generate a derived field value."""
        field_type = field_config.get("type")
        
        if field_type == "uuid":
            return str(uuid.uuid4())
        
        elif field_type == "timestamp":
            format_type = field_config.get("format", "iso8601")
            if format_type == "iso8601":
                return datetime.now().isoformat()
            else:
                return int(datetime.now().timestamp())
        
        elif field_type == "string":
            # Handle string formatting with sequences and templates
            format_str = field_config.get("format")
            template_str = field_config.get("template")
            
            if template_str:
                # Handle template strings with field substitution
                return self._process_template(template_str, field_config, record)
            elif format_str and "{seq:" in format_str:
                return self._format_sequential(format_str, self.entity_type, field_name)
            else:
                return field_config.get("initial_value", f"{field_name}_value")
        
        elif field_type == "float":
            min_val = field_config.get("min", 0.0)
            max_val = field_config.get("max", 100.0)
            return round(random.uniform(min_val, max_val), 2)
        
        elif field_type == "random_float":
            min_val = field_config.get("min", 0.0)
            max_val = field_config.get("max", 100.0)
            return round(random.uniform(min_val, max_val), 2)
        
        elif field_type == "calculated":
            # This would require expression evaluation
            # For now, return a placeholder
            return 0.0
        
        elif field_type == "conditional":
            return self._evaluate_conditional(field_config, record)
        
        elif field_type == "choice":
            # Handle choice fields with random selection from choices
            choices = field_config.get("choices", [])
            if choices:
                return random.choice(choices)
            else:
                return None
        
        elif field_type == "weighted_choice":
            # Handle weighted choice fields with precise distributions
            return self._generate_weighted_choice(field_config)
        
        elif field_type == "random_boolean":
            # Handle random boolean generation
            probability = field_config.get("probability", 0.5)
            return random.random() < probability
        
        elif field_type == "timestamp_millis":
            # Handle relative timestamp generation in milliseconds
            return self._generate_relative_timestamp(field_config, record)
        
        elif field_type == "reference":
            # Handle reference fields that get values from other records
            return self._resolve_reference_field(field_config, record)
        
        elif field_type == "template":
            # Handle template fields with variable substitution
            template_str = field_config.get("template", "")
            if template_str:
                return self._process_template(template_str, field_config, record)
            else:
                return f"template_value"
        
        else:
            return None
    
    def _evaluate_conditional(self, field_config: Dict[str, Any], record: Dict[str, Any]) -> Any:
        """Evaluate conditional field rules with conditions."""
        # conditional with condition_field and condition_value
        condition_field = field_config.get("condition_field")
        condition_value = field_config.get("condition_value")
        when_true = field_config.get("when_true")
        when_false = field_config.get("when_false")
        
        if condition_field and "condition_value" in field_config:
            if condition_field in record and record[condition_field] == condition_value:
                if when_true:
                    return self._generate_field_value_from_config(when_true, record)
            else:
                if when_false:
                    return self._generate_field_value_from_config(when_false, record)
        
        # Legacy array-style conditions for backward compatibility
        conditions = field_config.get("conditions", [])
        for condition in conditions:
            if "if" in condition:
                condition_str = condition["if"]
                
                # Basic evaluation for testing
                if " > " in condition_str:
                    field, value = condition_str.split(" > ")
                    if field in record and record[field] > float(value):
                        return condition.get("then")
                elif " <= " in condition_str:
                    field, value = condition_str.split(" <= ")
                    if field in record and record[field] <= float(value):
                        return condition.get("then")
            elif "else" in condition:
                return condition["else"]
        
        return None
    
    def _format_sequential(self, format_str: str, entity_type: str, field_name: str) -> str:
        """Handle sequential formatting like ORDER_{seq:05d}."""
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
    
    def _get_weight_function(self, weight_field: str) -> Callable[[str], float]:
        """Get a weight function for weighted selection."""
        # In a real implementation, this would look up actual field values
        # For testing, return a mock function
        def weight_func(ref_id: str) -> float:
            # Mock implementation
            return 10.0 if "VIP" in ref_id else 1.0
        return weight_func
    
    def _get_zipf_weight_function(self, alpha: float) -> Callable[[str], float]:
        """Get a Zipf distribution weight function."""
        def zipf_weight(ref_id: str) -> float:
            # In real implementation, would map IDs to ranks
            # For testing, use a simple hash-based approach
            rank = (hash(ref_id) % 100) + 1
            return 1.0 / (rank ** alpha)
        return zipf_weight
    
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
    
    def _generate_field_value_from_config(self, field_config: Dict[str, Any], record: Dict[str, Any]) -> Any:
        """Generate field value from configuration (used by conditional and template fields)."""
        field_type = field_config.get("type")
        
        if field_type == "string":
            if "format" in field_config:
                # Handle formatted strings like license plates
                format_str = field_config["format"]
                return self._process_format_string(format_str)
            else:
                return field_config.get("initial_value", "default_value")
        
        elif field_type == "reference":
            return self._resolve_reference_field(field_config, record)
        
        elif field_type == "choice":
            choices = field_config.get("choices", [])
            return random.choice(choices) if choices else None
        
        elif field_type == "conditional":
            return self._evaluate_conditional(field_config, record)
        
        elif field_type == "faker":
            # Handle faker fields in templates
            from faker import Faker
            fake = Faker()
            method = field_config.get("method", "word")
            text = field_config.get("text", "")
            
            if hasattr(fake, method):
                faker_method = getattr(fake, method)
                if text:
                    # For bothify method with text parameter
                    return faker_method(text=text)
                else:
                    return faker_method()
            else:
                return f"faker_{method}"
        
        elif field_type == "weighted_choice":
            # Handle weighted choice fields in templates
            return self._generate_weighted_choice(field_config)
        
        else:
            # Fallback to the main field generation method
            return self._generate_derived_field("temp_field", field_config, record)
    
    def _process_format_string(self, format_str: str) -> str:
        """Process format strings with random generators."""
        import re
        
        result = format_str
        
        # Handle {random_letters:N} patterns
        letter_matches = re.findall(r'\{random_letters:(\d+)\}', result)
        for count in letter_matches:
            random_letters = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=int(count)))
            result = result.replace(f'{{random_letters:{count}}}', random_letters, 1)
        
        # Handle {random_digits:N} patterns
        digit_matches = re.findall(r'\{random_digits:(\d+)\}', result)
        for count in digit_matches:
            random_digits = ''.join(random.choices('0123456789', k=int(count)))
            result = result.replace(f'{{random_digits:{count}}}', random_digits, 1)
        
        return result
    
    def _apply_reference_formatting(self, value: Any, format_type: str) -> Any:
        """Apply formatting to reference field values."""
        if format_type == "license_plate" and isinstance(value, str) and len(value) == 6:
            # Convert MAB123 to M-AB 123 format
            return f"{value[0]}-{value[1:3]} {value[3:6]}"
        return value
    
    def _resolve_reference_field(self, field_config: Dict[str, Any], record: Dict[str, Any]) -> Any:
        """Resolve reference field values with support for nested paths and formatting."""
        source = field_config.get("source", "")
        via = field_config.get("via", "")
        format_type = field_config.get("format", "")
        
        # Handle self-references
        if source.startswith("self."):
            field_path = source[5:]  # Remove "self."
            value = self._get_nested_field_value(record, field_path)
            return self._apply_reference_formatting(value, format_type)
        
        # Handle cross-entity references with via field
        if source and via and via in record:
            if "." in source:
                ref_entity, ref_field_path = source.split(".", 1)
                ref_id = record[via]
                
                if (hasattr(self.reference_pool, '_record_cache') and 
                    ref_entity in self.reference_pool._record_cache and
                    ref_id in self.reference_pool._record_cache[ref_entity]):
                    ref_record = self.reference_pool._record_cache[ref_entity][ref_id]
                    value = self._get_nested_field_value(ref_record, ref_field_path)
                    return self._apply_reference_formatting(value, format_type)
        
        # Handle direct source references
        if source and "." in source:
            parts = source.split(".")
            if len(parts) >= 2:
                entity_type = parts[0]
                field_path = ".".join(parts[1:])
                
                # Try to get a random reference from the pool
                try:
                    ref_id = self.reference_pool.get_random(entity_type)
                    if (hasattr(self.reference_pool, '_record_cache') and 
                        entity_type in self.reference_pool._record_cache and
                        ref_id in self.reference_pool._record_cache[entity_type]):
                        ref_record = self.reference_pool._record_cache[entity_type][ref_id]
                        value = self._get_nested_field_value(ref_record, field_path)
                        return self._apply_reference_formatting(value, format_type)
                except ValueError:
                    pass  # No references available
        
        # Fallback to random value
        return f"ref_{random.randint(1000, 9999)}"
    
    def _get_nested_field_value(self, record: Dict[str, Any], field_path: str) -> Any:
        """Get value from nested field path like 'full.Vehicle.cLicenseNr'."""
        parts = field_path.split(".")
        current = record
        
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        
        return current
    
    def _generate_relative_timestamp(self, field_config: Dict[str, Any], record: Dict[str, Any]) -> int:
        """Generate relative timestamp in milliseconds with reference support."""
        relative_to_reference = field_config.get("relative_to_reference")
        offset_minutes_min = field_config.get("offset_minutes_min", -30)
        offset_minutes_max = field_config.get("offset_minutes_max", 30)
        fallback = field_config.get("fallback", "now")
        
        base_timestamp = None
        
        # Try to find the reference timestamp
        if relative_to_reference:
            base_timestamp = self._resolve_reference_timestamp(relative_to_reference, record)
        
        # Use current time as base if no reference found
        if base_timestamp is None:
            if fallback == "now":
                base_timestamp = datetime.now()
            else:
                # Try to parse fallback as datetime if it's not "now"
                try:
                    base_timestamp = datetime.fromisoformat(fallback)
                except:
                    base_timestamp = datetime.now()
        
        # Add random offset
        offset_minutes = random.uniform(offset_minutes_min, offset_minutes_max)
        final_timestamp = base_timestamp + timedelta(minutes=offset_minutes)
        
        # Return milliseconds since epoch
        return int(final_timestamp.timestamp() * 1000)
    
    def _resolve_reference_timestamp(self, reference_path: str, record: Dict[str, Any]) -> Optional[datetime]:
        """Resolve timestamp from reference path for time-based correlation."""
        try:
            if "." in reference_path:
                parts = reference_path.split(".")
                if len(parts) >= 2:
                    entity_type = parts[0]
                    field_path = ".".join(parts[1:])
                    
                    # First check if we have a correlation field in current record
                    # find  that this event should correlate with
                    correlation_field = getattr(self, '_current_correlation_ref', None)
                    if correlation_field and entity_type in self.reference_pool._record_cache:
                        # Use the correlated timestamp
                        ref_record = self.reference_pool._record_cache[entity_type].get(correlation_field)
                        if ref_record:
                            timestamp_value = self._get_nested_field_value(ref_record, field_path)
                            if timestamp_value:
                                return self._parse_timestamp_value(timestamp_value)
                    
                    # Fallback: get random reference
                    try:
                        ref_id = self.reference_pool.get_random(entity_type)
                        if (hasattr(self.reference_pool, '_record_cache') and 
                            entity_type in self.reference_pool._record_cache and
                            ref_id in self.reference_pool._record_cache[entity_type]):
                            ref_record = self.reference_pool._record_cache[entity_type][ref_id]
                            timestamp_value = self._get_nested_field_value(ref_record, field_path)
                            if timestamp_value:
                                return self._parse_timestamp_value(timestamp_value)
                    except ValueError:
                        pass  # No references available
        
        except Exception:
            pass  # Parsing failed
        
        return None
    
    def _parse_timestamp_value(self, timestamp_value: Any) -> Optional[datetime]:
        """Parse various timestamp formats to datetime object."""
        if isinstance(timestamp_value, datetime):
            return timestamp_value
        
        if isinstance(timestamp_value, (int, float)):
            # Assume Unix timestamp (seconds or milliseconds)
            if timestamp_value > 1e10:  # Likely milliseconds
                return datetime.fromtimestamp(timestamp_value / 1000)
            else:  # Likely seconds
                return datetime.fromtimestamp(timestamp_value)
        
        if isinstance(timestamp_value, str):
            try:
                # Try ISO format first
                return datetime.fromisoformat(timestamp_value.replace('Z', '+00:00'))
            except:
                try:
                    # Try other common formats
                    from dateutil.parser import parse
                    return parse(timestamp_value)
                except:
                    pass
        
        return None
    
    def _generate_weighted_choice(self, field_config: Dict[str, Any]) -> Any:
        """Generate weighted choice with precise distributions."""
        choices = field_config.get("choices", [])
        weights = field_config.get("weights", [])
        
        if not choices:
            return None
        
        if not weights or len(weights) != len(choices):
            # Fall back to uniform choice if weights are missing or mismatched
            return random.choice(choices)
        
        # Ensure weights are normalized and sum to 1.0 for precision
        total_weight = sum(weights)
        normalized_weights = [w / total_weight for w in weights]
        
        # Use random.choices for proper weighted selection
        return random.choices(choices, weights=normalized_weights)[0]
    
    def _should_correlate(self, percentage: float) -> bool:
        """Determine if current record should be correlated based on percentage."""
        # Initialize correlation counter if not exists
        if not hasattr(self, '_correlation_counter'):
            self._correlation_counter = 0
            self._correlation_target = percentage / 100.0  # Convert to decimal
        
        self._correlation_counter += 1
        
        # Calculate expected correlations so far
        expected_correlations = self._correlation_counter * self._correlation_target
        
        # Track actual correlations
        if not hasattr(self, '_actual_correlations'):
            self._actual_correlations = 0
        
        # Decide if this record should correlate to maintain target percentage
        should_correlate = self._actual_correlations < expected_correlations
        
        if should_correlate:
            self._actual_correlations += 1
        
        return should_correlate
    
    def _get_reference_field_value(self, ref_type: str, ref_id: str, ref_field_path: str) -> Any:
        """Get field value from referenced record with support for nested paths."""
        if (hasattr(self.reference_pool, '_record_cache') and 
            ref_type in self.reference_pool._record_cache and
            ref_id in self.reference_pool._record_cache[ref_type]):
            ref_record = self.reference_pool._record_cache[ref_type][ref_id]
            return self._get_nested_field_value(ref_record, ref_field_path)
        return None
    
    def _apply_mapped_field(self, relationship_value: Any, rel_config: Dict[str, Any], mapped_field_config: Dict[str, Any]) -> Dict[str, Any]:
        """Apply mapped_field configuration to create derived field values from relationship.
        
        Args:
            relationship_value: The value returned from the relationship (not null, correlation exists)
            rel_config: The relationship configuration
            mapped_field_config: The mapped_field configuration with source_field, target_field, mapping
            
        Returns:
            Dictionary mapping target field names to their mapped values
        """
        mapped_values = {}
        
        # Get configuration
        source_field = mapped_field_config.get("source_field")
        target_field = mapped_field_config.get("target_field") 
        mapping_name = mapped_field_config.get("mapping")
        
        if not all([source_field, target_field, mapping_name]):
            return mapped_values
        
        # Get the mapping table from config
        mappings = self.config.config.get("mappings", {})
        mapping_table = mappings.get(mapping_name, {})
        
        if not mapping_table:
            return mapped_values
        
        # Parse reference to get the referenced record
        references = rel_config.get("references", "")
        ref_parts = references.split(".")
        if len(ref_parts) < 2:
            return mapped_values
            
        ref_type = ref_parts[0]
        
        # Get a random referenced record ID to get the record for source field lookup
        # In a correlation context, we need to find which record was actually referenced
        # For now, we'll get the source field value from the relationship itself
        
        # Use O(1) index lookup instead of O(n) linear search
        try:
            if self.reference_pool.get_type_count(ref_type) > 0:
                # Get the field path from the relationship reference
                ref_field_path = ".".join(ref_parts[1:])
                
                # Use index to find the record ID that has our relationship_value (O(1) lookup!)
                ref_id = self.reference_pool.find_by_field_value(ref_type, ref_field_path, str(relationship_value))
                
                if ref_id:
                    # Get the actual record using the found ID
                    if (hasattr(self.reference_pool, '_record_cache') and 
                        ref_type in self.reference_pool._record_cache and
                        ref_id in self.reference_pool._record_cache[ref_type]):
                        
                        ref_record = self.reference_pool._record_cache[ref_type][ref_id]
                        
                        # Get the source field value for mapping
                        source_value = self._get_nested_field_value(ref_record, source_field)
                        if source_value and str(source_value) in mapping_table:
                            # Map the source value to target value
                            mapped_values[target_field] = mapping_table[str(source_value)]
                            
        except Exception:
            # If anything fails, just skip the mapping
            pass
            
        return mapped_values
