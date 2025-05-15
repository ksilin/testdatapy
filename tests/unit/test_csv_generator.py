"""Unit tests for CSV generator."""
from pathlib import Path

import pandas as pd
import pytest

from testdatapy.generators.csv_gen import CSVGenerator


class TestCSVGenerator:
    """Test the CSVGenerator class."""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create a sample CSV file for testing."""
        data = {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [30, 25, 35],
            "active": [True, False, True],
        }
        df = pd.DataFrame(data)
        csv_path = tmp_path / "test.csv"
        df.to_csv(csv_path, index=False)
        return str(csv_path)

    def test_initialization(self, sample_csv):
        """Test generator initialization."""
        gen = CSVGenerator(
            csv_file=sample_csv,
            rate_per_second=5.0,
            max_messages=10,
        )
        assert gen.csv_file == Path(sample_csv)
        assert gen.rate_per_second == 5.0
        assert gen.max_messages == 10
        assert gen.total_rows == 3
        assert gen.current_index == 0

    def test_file_not_found(self):
        """Test initialization with non-existent file."""
        with pytest.raises(FileNotFoundError):
            CSVGenerator(csv_file="/path/to/nonexistent.csv")

    def test_generate_all_rows(self, sample_csv):
        """Test generating all rows from CSV."""
        gen = CSVGenerator(
            csv_file=sample_csv,
            rate_per_second=100.0,  # Fast rate for testing
            max_messages=3,
            cycle=False,
        )

        data_list = list(gen.generate())
        assert len(data_list) == 3

        # Check data
        assert data_list[0]["id"] == 1
        assert data_list[0]["name"] == "Alice"
        assert data_list[1]["id"] == 2
        assert data_list[1]["name"] == "Bob"
        assert data_list[2]["id"] == 3
        assert data_list[2]["name"] == "Charlie"

    def test_generate_with_cycle(self, sample_csv):
        """Test generating with cycling through the CSV."""
        gen = CSVGenerator(
            csv_file=sample_csv,
            rate_per_second=100.0,
            max_messages=7,  # More than CSV rows
            cycle=True,
        )

        data_list = list(gen.generate())
        assert len(data_list) == 7

        # Check cycling
        assert data_list[0]["name"] == "Alice"
        assert data_list[3]["name"] == "Alice"  # Cycled back
        assert data_list[6]["name"] == "Alice"  # Cycled again

    def test_generate_without_cycle(self, sample_csv):
        """Test generating without cycling stops at end."""
        gen = CSVGenerator(
            csv_file=sample_csv,
            rate_per_second=100.0,
            max_messages=10,  # More than CSV rows
            cycle=False,
        )

        data_list = list(gen.generate())
        assert len(data_list) == 3  # Only 3 rows in CSV

    def test_reset(self, sample_csv):
        """Test resetting the generator."""
        gen = CSVGenerator(csv_file=sample_csv, max_messages=2)

        # Generate some data
        list(gen.generate())
        assert gen.current_index == 2
        assert gen.message_count == 2

        # Reset
        gen.reset()
        assert gen.current_index == 0
        assert gen.message_count == 0

    def test_remaining_rows(self, sample_csv):
        """Test getting remaining rows."""
        gen = CSVGenerator(csv_file=sample_csv, rate_per_second=100.0)  # Fast rate for testing

        assert gen.remaining_rows == 3

        # Generate one row - note that current_index is incremented AFTER yield
        # So after the first next(), we're paused at the yield statement
        gen_iter = gen.generate()
        next(gen_iter)
        # At this point, we've yielded the first row but haven't incremented current_index yet
        assert gen.remaining_rows == 3  # Still 3 because increment happens after yield
        
        # Generate another row to see the increment
        next(gen_iter)
        assert gen.remaining_rows == 2  # Now it's 2

    def test_get_column_names(self, sample_csv):
        """Test getting column names."""
        gen = CSVGenerator(csv_file=sample_csv)

        columns = gen.get_column_names()
        assert columns == ["id", "name", "age", "active"]

    def test_get_sample_data(self, sample_csv):
        """Test getting sample data."""
        gen = CSVGenerator(csv_file=sample_csv)

        sample = gen.get_sample_data(n=2)
        assert len(sample) == 2
        assert sample[0]["name"] == "Alice"
        assert sample[1]["name"] == "Bob"

    def test_data_types_preserved(self, sample_csv):
        """Test that data types are preserved."""
        gen = CSVGenerator(csv_file=sample_csv, max_messages=1)

        data = list(gen.generate())[0]
        assert isinstance(data["id"], int | float)  # Pandas may read as float
        assert isinstance(data["name"], str)
        assert isinstance(data["age"], int | float)

    def test_null_handling(self, tmp_path):
        """Test handling of null values."""
        # Create CSV with null values
        data = {
            "id": [1, 2, 3],
            "name": ["Alice", None, "Charlie"],
            "age": [30, 25, None],
        }
        df = pd.DataFrame(data)
        csv_path = tmp_path / "null_test.csv"
        df.to_csv(csv_path, index=False)

        gen = CSVGenerator(csv_file=str(csv_path), max_messages=3, cycle=False)
        data_list = list(gen.generate())

        # Check null handling
        assert data_list[1]["name"] is None
        assert data_list[2]["age"] is None

    def test_rate_limiting(self, sample_csv):
        """Test rate limiting functionality."""
        import time

        rate_per_second = 10.0
        num_messages = 3
        gen = CSVGenerator(
            csv_file=sample_csv,
            rate_per_second=rate_per_second,
            max_messages=num_messages,
        )

        start_time = time.time()
        list(gen.generate())
        elapsed_time = time.time() - start_time

        # Should take approximately (num_messages - 1) / rate_per_second seconds
        expected_time = (num_messages - 1) / rate_per_second
        assert elapsed_time >= expected_time * 0.9  # Allow 10% tolerance

    def test_timestamp_handling(self, tmp_path):
        """Test handling of timestamp columns."""
        # Create CSV with timestamps
        data = {
            "id": [1, 2],
            "created_at": pd.to_datetime(["2024-01-01", "2024-01-02"]),
        }
        df = pd.DataFrame(data)
        csv_path = tmp_path / "timestamp_test.csv"
        df.to_csv(csv_path, index=False)

        gen = CSVGenerator(csv_file=str(csv_path), max_messages=2, cycle=False)
        data_list = list(gen.generate())

        # Check timestamp conversion
        assert isinstance(data_list[0]["created_at"], str)
        assert "2024-01-01" in data_list[0]["created_at"]
