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
        
        # Add ID field (if not referenced from elsewhere)
        id_field = self.entity_config.get("id_field", f"{self.entity_type[:-1]}_id")
        if id_field not in self.entity_config.get("relationships", {}):
            record[id_field] = str(uuid.uuid4())
        
        # Generate fields from relationships
        for field_name, rel_config in self.entity_config.get("relationships", {}).items():
            record[field_name] = self._generate_relationship_value(field_name, rel_config)
        
        # Generate derived fields
        for field_name, field_config in self.entity_config.get("derived_fields", {}).items():
            record[field_name] = self._generate_derived_field(field_name, field_config, record)
        
        # Track in reference pool if needed
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
        """Generate a simple reference value."""
        references = rel_config.get("references", "")
        if not references:
            return None
        
        # Parse reference format: "entity_type.field_name"
        ref_parts = references.split(".")
        if len(ref_parts) != 2:
            return None
        
        ref_type, ref_field = ref_parts
        
        # Handle temporal relationships
        if rel_config.get("recency_bias", False) and self.reference_pool.get_recent(ref_type):
            try:
                return self.reference_pool.get_random_recent(ref_type, bias_recent=True)
            except ValueError:
                # Fall back to regular random if no recent items
                pass
        
        # Handle weighted distribution
        distribution = rel_config.get("distribution", "uniform")
        
        if distribution == "weighted":
            weight_func = self._get_weight_function(rel_config.get("weight_field"))
            return self.reference_pool.get_weighted_random(ref_type, weight_func)
        elif distribution == "zipf":
            # Implement Zipf distribution
            alpha = rel_config.get("alpha", 1.5)
            weight_func = self._get_zipf_weight_function(alpha)
            return self.reference_pool.get_weighted_random(ref_type, weight_func)
        else:
            # Default uniform distribution
            return self.reference_pool.get_random(ref_type)
    
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
        
        if field_type == "timestamp":
            format_type = field_config.get("format", "iso8601")
            if format_type == "iso8601":
                return datetime.now().isoformat()
            else:
                return int(datetime.now().timestamp())
        
        elif field_type == "string":
            return field_config.get("initial_value", "")
        
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
        
        else:
            return None
    
    def _evaluate_conditional(self, field_config: Dict[str, Any], record: Dict[str, Any]) -> Any:
        """Evaluate conditional field rules."""
        conditions = field_config.get("conditions", [])
        
        for condition in conditions:
            if "if" in condition:
                # Simple evaluation (in real implementation, use safe expression evaluator)
                condition_str = condition["if"]
                
                # Very basic evaluation for testing
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
