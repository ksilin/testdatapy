"""CSV-based data generator implementation."""
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pandas as pd

from testdatapy.generators.base import DataGenerator


class CSVGenerator(DataGenerator):
    """Data generator that reads from CSV files."""

    def __init__(
        self,
        csv_file: str,
        rate_per_second: float = 10.0,
        max_messages: int | None = None,
        cycle: bool = True,
    ):
        """Initialize the CSV generator.

        Args:
            csv_file: Path to CSV file
            rate_per_second: Number of messages to generate per second
            max_messages: Maximum number of messages to generate
            cycle: Whether to cycle through the CSV when reaching the end
        """
        super().__init__(rate_per_second, max_messages)
        self.csv_file = Path(csv_file)
        self.cycle = cycle

        if not self.csv_file.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_file}")

        # Load the CSV data
        self.df = pd.read_csv(self.csv_file)
        self.total_rows = len(self.df)
        self.current_index = 0

        # Pre-calculated sleep time for rate limiting
        self._sleep_time = 1.0 / rate_per_second if rate_per_second > 0 else 0

    def generate(self) -> Iterator[dict[str, Any]]:
        """Generate data records from CSV.

        Yields:
            Dict containing row data from CSV
        """
        start_time = time.time()
        last_message_time = start_time

        while self.should_continue():
            # Check if we've reached the end of the CSV
            if self.current_index >= self.total_rows:
                if self.cycle:
                    self.current_index = 0
                else:
                    break

            # Get the current row
            row = self.df.iloc[self.current_index]
            data = row.to_dict()

            # Convert data types as needed
            for key, value in data.items():
                if pd.isna(value):
                    data[key] = None
                elif isinstance(value, pd.Timestamp):
                    data[key] = value.isoformat()

            yield data

            self.current_index += 1
            self.increment_count()

            # Rate limiting
            if self._sleep_time > 0:
                current_time = time.time()
                elapsed = current_time - last_message_time
                if elapsed < self._sleep_time:
                    time.sleep(self._sleep_time - elapsed)
                last_message_time = time.time()

    def reset(self) -> None:
        """Reset the generator to the beginning of the CSV."""
        self.current_index = 0
        self._message_count = 0

    @property
    def remaining_rows(self) -> int:
        """Get the number of remaining rows in the CSV."""
        return self.total_rows - self.current_index

    def get_column_names(self) -> list[str]:
        """Get the column names from the CSV.

        Returns:
            List of column names
        """
        return self.df.columns.tolist()

    def get_sample_data(self, n: int = 5) -> list[dict[str, Any]]:
        """Get sample data from the CSV.

        Args:
            n: Number of sample rows to return

        Returns:
            List of sample data dictionaries
        """
        sample_df = self.df.head(n)
        return sample_df.to_dict(orient="records")
