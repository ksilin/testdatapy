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
    
    def load_all(self) -> None:
        """Load all master data defined in configuration."""
        master_config = self.config.config.get("master_data", {})
        
        for entity_type, entity_config in master_config.items():
            self.load_entity(entity_type, entity_config)
    
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
            for field_name, field_config in schema.items():
                record[field_name] = self._generate_field(
                    field_name, field_config, entity_type, i
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
            if format_str and "{seq:" in format_str:
                # Handle sequential numbering
                return self._format_sequential(format_str, entity_type, field_name)
            elif format_str:
                return format_str.format(index=index)
            else:
                return f"{field_name}_{index}"
        
        elif field_type == "faker":
            method = field_config.get("method", "word")
            faker_method = getattr(self.faker, method, None)
            if faker_method:
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
        
        else:
            return f"{field_name}_value"
    
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
            if id_field in record:
                ids.append(record[id_field])
        
        if ids:
            self.reference_pool.add_references(entity_type, ids)
    
    def produce_all(self) -> None:
        """Bulk produce all loaded master data to Kafka."""
        if not self.producer:
            raise ValueError("No producer configured for bulk loading")
        
        master_config = self.config.config.get("master_data", {})
        
        for entity_type, entity_config in master_config.items():
            if entity_config.get("bulk_load", False):
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
        id_field = entity_config.get("id_field", f"{entity_type[:-1]}_id")
        
        for record in data:
            key = record.get(id_field)
            self.producer.produce(
                key=key,
                value=record,
                topic=topic  # Some producers might need this
            )
    
    def get_loaded_data(self, entity_type: str) -> List[Dict[str, Any]]:
        """Get loaded data for a specific entity type."""
        return self.loaded_data.get(entity_type, [])
    
    def get_sample(self, entity_type: str, count: int = 5) -> List[Dict[str, Any]]:
        """Get sample records from loaded data."""
        data = self.loaded_data.get(entity_type, [])
        return data[:count]
