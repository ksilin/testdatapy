"""Unit tests for Faker generator."""
import time

from testdatapy.generators.faker_gen import FakerGenerator


class TestFakerGenerator:
    """Test the FakerGenerator class."""

    def test_initialization(self):
        """Test generator initialization."""
        gen = FakerGenerator(rate_per_second=5.0, max_messages=10, seed=42)
        assert gen.rate_per_second == 5.0
        assert gen.max_messages == 10
        assert gen._sleep_time == 0.2  # 1/5

    def test_generate_customer_data(self):
        """Test generating customer data."""
        gen = FakerGenerator(rate_per_second=100.0, max_messages=5, seed=42)
        data_list = list(gen.generate())

        assert len(data_list) == 5

        # Check structure of generated data
        for data in data_list:
            assert "CustomerID" in data
            assert "CustomerFirstName" in data
            assert "CustomerLastName" in data
            assert "PostalCode" in data
            assert "Street" in data
            assert "City" in data
            assert "CountryCode" in data

            # Check data types
            assert isinstance(data["CustomerID"], int)
            assert isinstance(data["CustomerFirstName"], str)
            assert isinstance(data["CustomerLastName"], str)
            assert isinstance(data["PostalCode"], int)
            assert isinstance(data["Street"], str)
            assert isinstance(data["City"], str)
            assert isinstance(data["CountryCode"], str)
            assert len(data["CountryCode"]) == 2

    def test_rate_limiting(self):
        """Test rate limiting functionality."""
        rate_per_second = 10.0
        num_messages = 5
        gen = FakerGenerator(rate_per_second=rate_per_second, max_messages=num_messages)

        start_time = time.time()
        list(gen.generate())
        elapsed_time = time.time() - start_time

        # Should take approximately (num_messages - 1) / rate_per_second seconds
        expected_time = (num_messages - 1) / rate_per_second
        assert elapsed_time >= expected_time * 0.9  # Allow 10% tolerance

    def test_reproducible_data(self):
        """Test that seed produces reproducible data."""
        gen1 = FakerGenerator(max_messages=3, seed=12345)
        gen2 = FakerGenerator(max_messages=3, seed=12345)

        data1 = list(gen1.generate())
        data2 = list(gen2.generate())

        assert data1 == data2

    def test_generate_from_schema(self):
        """Test generating data from Avro schema."""
        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"},
                {"name": "email", "type": "string"},
                {"name": "active", "type": "boolean"},
            ],
        }

        gen = FakerGenerator(max_messages=3)
        data_list = list(gen.generate_generic(schema))

        assert len(data_list) == 3

        for data in data_list:
            assert "id" in data
            assert "name" in data
            assert "age" in data
            assert "email" in data
            assert "active" in data

            assert isinstance(data["id"], str)
            assert isinstance(data["name"], str)
            assert isinstance(data["age"], int)
            assert isinstance(data["email"], str)
            assert "@" in data["email"]
            assert isinstance(data["active"], bool)

    def test_complex_schema_generation(self):
        """Test generating data from complex schema with nested types."""
        schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "metadata", "type": {"type": "map", "values": "string"}},
                {"name": "email", "type": ["null", "string"]},
            ],
        }

        gen = FakerGenerator(max_messages=2)
        data_list = list(gen.generate_generic(schema))

        assert len(data_list) == 2

        for data in data_list:
            assert isinstance(data["id"], str)
            assert isinstance(data["tags"], list)
            assert all(isinstance(tag, str) for tag in data["tags"])
            assert isinstance(data["metadata"], dict)
            assert all(isinstance(v, str) for v in data["metadata"].values())
            assert data["email"] is None or isinstance(data["email"], str)

    def test_field_name_inference(self):
        """Test that field names influence generated values."""
        gen = FakerGenerator(max_messages=1)

        # Test string field inference
        assert "@" in gen._generate_string_value("user_email")
        assert len(gen._generate_string_value("country_code")) == 2

        # Test integer field inference
        age = gen._generate_int_value("age")
        assert 18 <= age <= 80

        customer_id = gen._generate_int_value("customer_id")
        assert 1000 <= customer_id <= 99999
