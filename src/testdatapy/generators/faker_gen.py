"""Faker-based data generator implementation."""
import time
from collections.abc import Iterator
from typing import Any

from faker import Faker
from faker.providers import BaseProvider

from testdatapy.generators.base import DataGenerator


class CustomerProvider(BaseProvider):
    """Custom Faker provider for customer-specific data."""

    def customer_id(self) -> int:
        """Generate a customer ID."""
        return self.random_int(min=1000, max=99999)

    def country_code(self) -> str:
        """Generate a 2-letter country code."""
        return self.random_element(["US", "UK", "DE", "FR", "ES", "IT", "CA", "AU", "JP", "BR"])


class FakerGenerator(DataGenerator):
    """Data generator using Faker library."""

    def __init__(
        self,
        rate_per_second: float = 10.0,
        max_messages: int | None = None,
        locale: str = "en_US",
        seed: int | None = None,
    ):
        """Initialize the Faker generator.

        Args:
            rate_per_second: Number of messages to generate per second
            max_messages: Maximum number of messages to generate
            locale: Faker locale for data generation
            seed: Random seed for reproducible data
        """
        super().__init__(rate_per_second, max_messages)
        self.fake = Faker(locale)
        if seed is not None:
            self.fake.seed_instance(seed)

        # Add custom provider
        self.fake.add_provider(CustomerProvider)

        # Pre-calculated sleep time for rate limiting
        self._sleep_time = 1.0 / rate_per_second if rate_per_second > 0 else 0

    def generate(self) -> Iterator[dict[str, Any]]:
        """Generate customer data records.

        Yields:
            Dict containing customer data
        """
        start_time = time.time()
        last_message_time = start_time

        while self.should_continue():
            # Generate customer data
            data = {
                "CustomerID": self.fake.customer_id(),
                "CustomerFirstName": self.fake.first_name(),
                "CustomerLastName": self.fake.last_name(),
                "PostalCode": (
                    int(self.fake.postcode()[:5])
                    if self.fake.postcode().isdigit()
                    else self.fake.random_int(10000, 99999)
                ),
                "Street": self.fake.street_address(),
                "City": self.fake.city(),
                "CountryCode": self.fake.country_code(),
            }

            yield data
            self.increment_count()

            # Rate limiting
            if self._sleep_time > 0:
                current_time = time.time()
                elapsed = current_time - last_message_time
                if elapsed < self._sleep_time:
                    time.sleep(self._sleep_time - elapsed)
                last_message_time = time.time()

    def generate_generic(self, schema: dict[str, Any]) -> Iterator[dict[str, Any]]:
        """Generate data based on an Avro schema.

        Args:
            schema: Avro schema definition

        Yields:
            Dict containing generated data
        """
        start_time = time.time()
        last_message_time = start_time

        while self.should_continue():
            data = self._generate_from_schema(schema)
            yield data
            self.increment_count()

            # Rate limiting
            if self._sleep_time > 0:
                current_time = time.time()
                elapsed = current_time - last_message_time
                if elapsed < self._sleep_time:
                    time.sleep(self._sleep_time - elapsed)
                last_message_time = time.time()

    def _generate_from_schema(self, schema: dict[str, Any]) -> dict[str, Any]:
        """Generate data based on Avro schema.

        Args:
            schema: Avro schema definition

        Returns:
            Generated data dictionary
        """
        if schema["type"] == "record":
            data = {}
            for field in schema["fields"]:
                field_name = field["name"]
                field_type = field["type"]
                data[field_name] = self._generate_field_value(field_name, field_type)
            return data
        else:
            raise ValueError(f"Unsupported schema type: {schema['type']}")

    def _generate_field_value(self, field_name: str, field_type: Any) -> Any:
        """Generate value for a specific field based on its type and name.

        Args:
            field_name: Name of the field
            field_type: Avro field type

        Returns:
            Generated value
        """
        # Handle simple types
        if isinstance(field_type, str):
            if field_type == "string":
                return self._generate_string_value(field_name)
            elif field_type == "int":
                return self._generate_int_value(field_name)
            elif field_type == "long":
                return self.fake.random_int(min=-2147483648, max=2147483647)
            elif field_type == "float":
                return self.fake.pyfloat()
            elif field_type == "double":
                return self.fake.pyfloat()
            elif field_type == "boolean":
                return self.fake.pybool()
            elif field_type == "null":
                return None
            else:
                raise ValueError(f"Unsupported field type: {field_type}")

        # Handle complex types (unions, arrays, etc.)
        elif isinstance(field_type, list):
            # Union type - choose first non-null type
            for t in field_type:
                if t != "null":
                    return self._generate_field_value(field_name, t)
            return None

        elif isinstance(field_type, dict):
            if field_type["type"] == "array":
                items_type = field_type["items"]
                return [
                    self._generate_field_value(field_name, items_type)
                    for _ in range(self.fake.random_int(min=0, max=5))
                ]
            elif field_type["type"] == "map":
                values_type = field_type["values"]
                return {
                    self.fake.word(): self._generate_field_value(field_name, values_type)
                    for _ in range(self.fake.random_int(min=0, max=3))
                }
            elif field_type["type"] == "record":
                return self._generate_from_schema(field_type)

        raise ValueError(f"Cannot generate value for type: {field_type}")

    def _generate_string_value(self, field_name: str) -> str:
        """Generate string value based on field name.

        Args:
            field_name: Name of the field

        Returns:
            Generated string value
        """
        field_lower = field_name.lower()

        # Common patterns
        if "email" in field_lower:
            return self.fake.email()
        elif "first" in field_lower and "name" in field_lower:
            return self.fake.first_name()
        elif "last" in field_lower and "name" in field_lower:
            return self.fake.last_name()
        elif "name" in field_lower:
            return self.fake.name()
        elif "street" in field_lower or "address" in field_lower:
            return self.fake.street_address()
        elif "city" in field_lower:
            return self.fake.city()
        elif "country" in field_lower and "code" in field_lower:
            return self.fake.country_code()
        elif "country" in field_lower:
            return self.fake.country()
        elif "phone" in field_lower:
            return self.fake.phone_number()
        elif "company" in field_lower:
            return self.fake.company()
        elif "uuid" in field_lower or "id" in field_lower:
            return self.fake.uuid4()
        elif "url" in field_lower or "website" in field_lower:
            return self.fake.url()
        else:
            return self.fake.word()

    def _generate_int_value(self, field_name: str) -> int:
        """Generate integer value based on field name.

        Args:
            field_name: Name of the field

        Returns:
            Generated integer value
        """
        field_lower = field_name.lower()

        if "id" in field_lower:
            return self.fake.random_int(min=1000, max=99999)
        elif "age" in field_lower:
            return self.fake.random_int(min=18, max=80)
        elif "postal" in field_lower or "zip" in field_lower:
            return self.fake.random_int(min=10000, max=99999)
        elif "year" in field_lower:
            return self.fake.random_int(min=1950, max=2023)
        else:
            return self.fake.random_int(min=0, max=1000)
