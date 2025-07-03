"""Enhanced Faker integration for realistic test data generation.

This module provides comprehensive Faker integration with custom providers,
localization support, and advanced data generation patterns.
"""

import re
import random
from typing import Any, Dict, List, Optional, Type, Union, Callable
from faker import Faker
from faker.providers import BaseProvider
from datetime import datetime, timedelta
import uuid
import json

from .function_registry import FunctionRegistry, FunctionCategory, FunctionMetadata
from ..exceptions import TestDataPyException
from ..logging_config import get_schema_logger

logger = get_schema_logger(__name__)


class TestDataProvider(BaseProvider):
    """Custom Faker provider for test data generation."""
    
    def test_id(self, prefix: str = "TEST", length: int = 8) -> str:
        """Generate a test ID with prefix."""
        suffix = ''.join(random.choices('0123456789ABCDEF', k=length))
        return f"{prefix}-{suffix}"
    
    def correlation_id(self) -> str:
        """Generate a correlation ID for tracking."""
        return str(uuid.uuid4())
    
    def api_key(self, length: int = 32) -> str:
        """Generate an API key."""
        chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
        return ''.join(random.choices(chars, k=length))
    
    def json_data(self, schema: Optional[Dict[str, Any]] = None) -> str:
        """Generate JSON data based on schema."""
        if schema is None:
            schema = {
                "id": "{{random_int}}",
                "name": "{{name}}",
                "active": "{{boolean}}"
            }
        
        # Simple template replacement
        data = {}
        for key, template in schema.items():
            if isinstance(template, str) and template.startswith("{{") and template.endswith("}}"):
                method_name = template[2:-2]
                if hasattr(self.generator, method_name):
                    data[key] = getattr(self.generator, method_name)()
                else:
                    data[key] = template
            else:
                data[key] = template
        
        return json.dumps(data)
    
    def nested_object(self, depth: int = 2, width: int = 3) -> Dict[str, Any]:
        """Generate a nested object structure."""
        if depth <= 0:
            return self.generator.pystr()
        
        obj = {}
        for i in range(width):
            key = self.generator.word()
            if random.choice([True, False]):
                obj[key] = self.nested_object(depth - 1, width)
            else:
                obj[key] = self.generator.pystr()
        
        return obj
    
    def business_identifier(self, identifier_type: str = "EIN") -> str:
        """Generate business identifiers."""
        if identifier_type.upper() == "EIN":
            # Employer Identification Number format: XX-XXXXXXX
            return f"{random.randint(10, 99)}-{random.randint(1000000, 9999999)}"
        elif identifier_type.upper() == "DUNS":
            # DUNS number format: XXXXXXXXX
            return f"{random.randint(100000000, 999999999)}"
        else:
            return f"{identifier_type}-{random.randint(100000, 999999)}"
    
    def measurement(self, unit: str = "kg", min_val: float = 0.0, max_val: float = 100.0) -> str:
        """Generate measurements with units."""
        value = round(random.uniform(min_val, max_val), 2)
        return f"{value} {unit}"
    
    def version_number(self, format_type: str = "semantic") -> str:
        """Generate version numbers."""
        if format_type == "semantic":
            major = random.randint(0, 10)
            minor = random.randint(0, 20)
            patch = random.randint(0, 100)
            return f"{major}.{minor}.{patch}"
        elif format_type == "build":
            return f"{random.randint(1000, 9999)}.{random.randint(1, 365)}.{random.randint(1, 24)}"
        else:
            return f"v{random.randint(1, 10)}.{random.randint(0, 9)}"


class PatternProvider(BaseProvider):
    """Provider for pattern-based data generation."""
    
    def pattern_string(self, pattern: str) -> str:
        """Generate string based on pattern.
        
        Pattern syntax:
        - # = random digit (0-9)
        - ? = random letter (a-zA-Z)
        - @ = random lowercase letter (a-z)
        - ^ = random uppercase letter (A-Z)
        - ! = random special character
        - \\ = escape character
        """
        result = ""
        i = 0
        while i < len(pattern):
            char = pattern[i]
            
            if char == '\\' and i + 1 < len(pattern):
                # Escaped character
                result += pattern[i + 1]
                i += 2
            elif char == '#':
                result += str(random.randint(0, 9))
                i += 1
            elif char == '?':
                result += random.choice('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')
                i += 1
            elif char == '@':
                result += random.choice('abcdefghijklmnopqrstuvwxyz')
                i += 1
            elif char == '^':
                result += random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
                i += 1
            elif char == '!':
                result += random.choice('!@#$%^&*()_+-=[]{}|;:,.<>?')
                i += 1
            else:
                result += char
                i += 1
        
        return result
    
    def regex_pattern(self, pattern: str, max_length: int = 50) -> str:
        """Generate string matching regex pattern (simplified)."""
        # This is a simplified regex generator - for production use,
        # consider using libraries like 'exrex' or 'rstr'
        
        # Handle some common patterns
        if pattern == r'\d+':
            return str(random.randint(1, 999999))
        elif pattern == r'\w+':
            length = random.randint(3, 10)
            return ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=length))
        elif pattern == r'[a-z]+':
            length = random.randint(3, 10)
            return ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=length))
        elif pattern == r'[A-Z]+':
            length = random.randint(3, 10)
            return ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=length))
        else:
            # Fallback to pattern_string syntax
            return self.pattern_string(pattern)


class LocalizedDataProvider(BaseProvider):
    """Provider for localized data generation."""
    
    def localized_name(self, locale: str = "en_US") -> str:
        """Generate localized name."""
        # Create temporary faker with specific locale
        temp_faker = Faker(locale)
        return temp_faker.name()
    
    def localized_address(self, locale: str = "en_US") -> str:
        """Generate localized address."""
        temp_faker = Faker(locale)
        return temp_faker.address()
    
    def localized_phone(self, locale: str = "en_US") -> str:
        """Generate localized phone number."""
        temp_faker = Faker(locale)
        return temp_faker.phone_number()
    
    def currency_amount(self, currency: str = "USD", min_amount: float = 0.01, max_amount: float = 10000.0) -> str:
        """Generate currency amount."""
        amount = round(random.uniform(min_amount, max_amount), 2)
        
        currency_symbols = {
            "USD": "$", "EUR": "€", "GBP": "£", "JPY": "¥",
            "CNY": "¥", "INR": "₹", "KRW": "₩", "CAD": "C$"
        }
        
        symbol = currency_symbols.get(currency, currency)
        return f"{symbol}{amount:,.2f}"


class FakerIntegration:
    """Enhanced Faker integration with comprehensive data generation capabilities."""
    
    def __init__(
        self,
        locale: Union[str, List[str]] = "en_US",
        providers: Optional[List[Type[BaseProvider]]] = None,
        seed: Optional[int] = None
    ):
        """Initialize Faker integration.
        
        Args:
            locale: Locale(s) for data generation
            providers: Additional custom providers
            seed: Random seed for reproducible data
        """
        self.faker = Faker(locale)
        
        if seed is not None:
            Faker.seed(seed)
            random.seed(seed)
        
        # Add custom providers
        self.faker.add_provider(TestDataProvider)
        self.faker.add_provider(PatternProvider)
        self.faker.add_provider(LocalizedDataProvider)
        
        if providers:
            for provider in providers:
                self.faker.add_provider(provider)
        
        self.locale = locale
        self._function_cache: Dict[str, Callable] = {}
        
        logger.info(f"Initialized Faker integration with locale: {locale}")
    
    def register_with_registry(self, registry: FunctionRegistry, namespace: str = "faker") -> int:
        """Register all Faker functions with the function registry.
        
        Args:
            registry: Function registry to register with
            namespace: Namespace for Faker functions
            
        Returns:
            Number of functions registered
        """
        registered_count = 0
        
        # Get all available Faker methods (skip deprecated/problematic methods)
        skip_methods = {'seed', 'random'}  # These are deprecated or cause issues
        faker_methods = [
            method for method in dir(self.faker)
            if not method.startswith('_') 
            and method not in skip_methods
            and callable(getattr(self.faker, method, None))
        ]
        
        for method_name in faker_methods:
            try:
                method = getattr(self.faker, method_name)
                
                # Create wrapper function that handles arguments properly
                def create_wrapper(method_name: str):
                    def wrapper(*args, **kwargs):
                        faker_method = getattr(self.faker, method_name)
                        return faker_method(*args, **kwargs)
                    wrapper.__name__ = method_name
                    return wrapper
                
                wrapper = create_wrapper(method_name)
                
                # Register with registry
                success = registry.register(
                    name=method_name,
                    func=wrapper,
                    description=f"Faker method: {method_name}",
                    category=FunctionCategory.FAKER,
                    namespace=namespace,
                    tags={"faker", "data_generation"},
                    is_safe=True,
                    overwrite=True
                )
                
                if success:
                    registered_count += 1
            
            except Exception as e:
                logger.warning(f"Failed to register Faker method {method_name}: {e}")
        
        # Register pattern-based functions
        pattern_functions = {
            "pattern_string": "Generate string from pattern (#=digit, ?=letter, @=lowercase, ^=uppercase)",
            "regex_pattern": "Generate string matching regex pattern (simplified)",
            "test_id": "Generate test ID with prefix",
            "correlation_id": "Generate correlation ID",
            "api_key": "Generate API key",
            "json_data": "Generate JSON data from schema",
            "nested_object": "Generate nested object structure",
            "business_identifier": "Generate business identifiers (EIN, DUNS, etc.)",
            "measurement": "Generate measurements with units",
            "version_number": "Generate version numbers",
            "localized_name": "Generate localized name",
            "localized_address": "Generate localized address",
            "localized_phone": "Generate localized phone number",
            "currency_amount": "Generate currency amount"
        }
        
        for func_name, description in pattern_functions.items():
            if hasattr(self.faker, func_name):
                method = getattr(self.faker, func_name)
                
                def create_wrapper(method_name: str):
                    def wrapper(*args, **kwargs):
                        faker_method = getattr(self.faker, method_name)
                        return faker_method(*args, **kwargs)
                    wrapper.__name__ = method_name
                    return wrapper
                
                wrapper = create_wrapper(func_name)
                
                success = registry.register(
                    name=func_name,
                    func=wrapper,
                    description=description,
                    category=FunctionCategory.FAKER,
                    namespace=namespace,
                    tags={"faker", "custom", "pattern"},
                    is_safe=True,
                    overwrite=True
                )
                
                if success:
                    registered_count += 1
        
        logger.info(f"Registered {registered_count} Faker functions with registry",
                   namespace=namespace)
        
        return registered_count
    
    def create_contextual_data(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Create contextual fake data based on provided context.
        
        Args:
            context: Context dictionary with hints for data generation
            
        Returns:
            Dictionary with generated contextual data
        """
        result = {}
        
        for key, hint in context.items():
            if isinstance(hint, str):
                if "name" in hint.lower():
                    result[key] = self.faker.name()
                elif "email" in hint.lower():
                    result[key] = self.faker.email()
                elif "phone" in hint.lower():
                    result[key] = self.faker.phone_number()
                elif "address" in hint.lower():
                    result[key] = self.faker.address()
                elif "company" in hint.lower():
                    result[key] = self.faker.company()
                elif "date" in hint.lower():
                    result[key] = self.faker.date()
                elif "time" in hint.lower():
                    result[key] = self.faker.time()
                elif "url" in hint.lower():
                    result[key] = self.faker.url()
                elif "text" in hint.lower():
                    result[key] = self.faker.text()
                elif "number" in hint.lower() or "id" in hint.lower():
                    result[key] = self.faker.random_int()
                else:
                    result[key] = self.faker.word()
            elif isinstance(hint, dict):
                # Nested context
                result[key] = self.create_contextual_data(hint)
            else:
                result[key] = str(hint)
        
        return result
    
    def generate_dataset(
        self,
        schema: Dict[str, Any],
        count: int = 100,
        relationships: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Generate a dataset based on schema and relationships.
        
        Args:
            schema: Schema defining data structure
            count: Number of records to generate
            relationships: Relationships between fields
            
        Returns:
            List of generated data records
        """
        dataset = []
        
        for i in range(count):
            record = {}
            
            for field_name, field_config in schema.items():
                if isinstance(field_config, str):
                    # Simple field type
                    record[field_name] = self._generate_field_value(field_config)
                elif isinstance(field_config, dict):
                    # Complex field configuration
                    record[field_name] = self._generate_complex_field(field_config, record, relationships)
            
            dataset.append(record)
        
        return dataset
    
    def _generate_field_value(self, field_type: str) -> Any:
        """Generate value for a field based on type."""
        field_type = field_type.lower()
        
        type_mappings = {
            "name": lambda: self.faker.name(),
            "email": lambda: self.faker.email(),
            "phone": lambda: self.faker.phone_number(),
            "address": lambda: self.faker.address(),
            "company": lambda: self.faker.company(),
            "date": lambda: self.faker.date(),
            "datetime": lambda: self.faker.date_time(),
            "time": lambda: self.faker.time(),
            "url": lambda: self.faker.url(),
            "text": lambda: self.faker.text(),
            "word": lambda: self.faker.word(),
            "sentence": lambda: self.faker.sentence(),
            "paragraph": lambda: self.faker.paragraph(),
            "boolean": lambda: self.faker.boolean(),
            "integer": lambda: self.faker.random_int(),
            "float": lambda: self.faker.pyfloat(),
            "uuid": lambda: self.faker.uuid4(),
            "ipv4": lambda: self.faker.ipv4(),
            "user_agent": lambda: self.faker.user_agent(),
            "credit_card": lambda: self.faker.credit_card_number(),
            "ssn": lambda: self.faker.ssn(),
            "country": lambda: self.faker.country(),
            "currency": lambda: self.faker.currency_code(),
        }
        
        generator = type_mappings.get(field_type, lambda: self.faker.word())
        return generator()
    
    def _generate_complex_field(
        self,
        field_config: Dict[str, Any],
        record: Dict[str, Any],
        relationships: Optional[Dict[str, Any]]
    ) -> Any:
        """Generate value for a complex field configuration."""
        field_type = field_config.get("type", "word")
        
        # Handle constraints
        if "min" in field_config and "max" in field_config:
            if field_type in ["integer", "int"]:
                return self.faker.random_int(min=field_config["min"], max=field_config["max"])
            elif field_type in ["float", "decimal"]:
                return self.faker.pyfloat(
                    min_value=field_config["min"],
                    max_value=field_config["max"]
                )
        
        # Handle patterns
        if "pattern" in field_config:
            return self.faker.pattern_string(field_config["pattern"])
        
        # Handle choices
        if "choices" in field_config:
            return random.choice(field_config["choices"])
        
        # Handle relationships
        if relationships and field_config.get("related_to"):
            related_field = field_config["related_to"]
            if related_field in record:
                # Generate related data based on existing field
                return self._generate_related_value(record[related_field], field_config)
        
        # Default generation
        return self._generate_field_value(field_type)
    
    def _generate_related_value(self, base_value: Any, field_config: Dict[str, Any]) -> Any:
        """Generate a value related to another field."""
        relation_type = field_config.get("relation_type", "similar")
        
        if relation_type == "email_from_name" and isinstance(base_value, str):
            # Generate email from name
            name_parts = base_value.lower().split()
            if len(name_parts) >= 2:
                username = f"{name_parts[0]}.{name_parts[-1]}"
                domain = self.faker.domain_name()
                return f"{username}@{domain}"
        
        elif relation_type == "similar":
            # Generate similar value
            return self._generate_field_value(field_config.get("type", "word"))
        
        return base_value
    
    def get_available_locales(self) -> List[str]:
        """Get list of available Faker locales."""
        return list(Faker._LOCALES.keys())
    
    def get_available_providers(self) -> List[str]:
        """Get list of available Faker providers."""
        providers = []
        
        # Get standard providers
        for provider_name in dir(self.faker.providers):
            if not provider_name.startswith('_'):
                providers.append(provider_name)
        
        return sorted(providers)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get Faker integration statistics."""
        return {
            "locale": self.locale,
            "providers_count": len(self.faker.providers),
            "available_methods": len([
                method for method in dir(self.faker)
                if not method.startswith('_') 
                and method not in {'seed', 'random'}
                and callable(getattr(self.faker, method, None))
            ]),
            "custom_providers": [
                "TestDataProvider",
                "PatternProvider", 
                "LocalizedDataProvider"
            ]
        }