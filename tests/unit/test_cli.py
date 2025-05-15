"""Unit tests for CLI interface."""
import json
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from testdatapy.cli import cli


class TestCLI:
    """Test the CLI interface."""

    @pytest.fixture
    def runner(self):
        """Create a CLI test runner."""
        return CliRunner()

    @pytest.fixture
    def temp_config(self, tmp_path):
        """Create a temporary configuration file."""
        config = {
            "bootstrap.servers": "localhost:9092",
            "schema.registry.url": "http://localhost:8081",
            "rate_per_second": 5,
        }
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config))
        return str(config_file)

    @pytest.fixture
    def temp_schema(self, tmp_path):
        """Create a temporary schema file."""
        schema = {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
            ],
        }
        schema_file = tmp_path / "schema.avsc"
        schema_file.write_text(json.dumps(schema))
        return str(schema_file)

    @pytest.fixture
    def temp_csv(self, tmp_path):
        """Create a temporary CSV file."""
        csv_content = "id,name,age\n1,Alice,30\n2,Bob,25\n"
        csv_file = tmp_path / "data.csv"
        csv_file.write_text(csv_content)
        return str(csv_file)

    def test_version(self, runner):
        """Test version command."""
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "version" in result.output.lower()

    def test_help(self, runner):
        """Test help command."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Test data generation tool" in result.output

    def test_list_generators(self, runner):
        """Test list-generators command."""
        result = runner.invoke(cli, ["list-generators"])
        assert result.exit_code == 0
        assert "faker" in result.output
        assert "csv" in result.output

    def test_list_formats(self, runner):
        """Test list-formats command."""
        result = runner.invoke(cli, ["list-formats"])
        assert result.exit_code == 0
        assert "json" in result.output
        assert "avro" in result.output

    def test_validate_config(self, runner, temp_config):
        """Test validate command with config."""
        result = runner.invoke(cli, ["validate", "--config", temp_config])
        assert result.exit_code == 0
        assert "Configuration file is valid" in result.output

    def test_validate_schema(self, runner, temp_schema):
        """Test validate command with schema."""
        result = runner.invoke(cli, ["validate", "--schema-file", temp_schema])
        assert result.exit_code == 0
        assert "Schema file is valid" in result.output

    def test_validate_invalid_config(self, runner, tmp_path):
        """Test validate with invalid config."""
        invalid_config = tmp_path / "invalid.json"
        invalid_config.write_text("{invalid json")

        result = runner.invoke(cli, ["validate", "--config", str(invalid_config)])
        assert result.exit_code == 1
        assert "invalid" in result.output.lower()

    @patch("testdatapy.cli.JsonProducer")
    @patch("testdatapy.cli.FakerGenerator")
    def test_produce_json_faker(self, mock_generator, mock_producer, runner):
        """Test produce command with JSON format and Faker generator."""
        # Setup mocks
        mock_gen_instance = MagicMock()
        mock_gen_instance.generate.return_value = [
            {"id": 1, "name": "Test1"},
            {"id": 2, "name": "Test2"},
        ]
        mock_generator.return_value = mock_gen_instance

        mock_prod_instance = MagicMock()
        mock_prod_instance.flush.return_value = 0
        mock_producer.return_value = mock_prod_instance

        result = runner.invoke(
            cli,
            [
                "produce",
                "--topic", "test-topic",
                "--format", "json",
                "--generator", "faker",
                "--count", "2",
            ],
        )

        assert result.exit_code == 0
        assert "Starting data generation" in result.output
        assert "Total messages: 2" in result.output

        # Verify calls
        mock_generator.assert_called_once()
        mock_producer.assert_called_once()
        assert mock_prod_instance.produce.call_count == 2

    @patch("testdatapy.cli.JsonProducer")
    @patch("testdatapy.cli.CSVGenerator")
    def test_produce_json_csv(self, mock_generator, mock_producer, runner, temp_csv):
        """Test produce command with JSON format and CSV generator."""
        # Setup mocks
        mock_gen_instance = MagicMock()
        mock_gen_instance.generate.return_value = [
            {"id": "1", "name": "Alice", "age": "30"},
            {"id": "2", "name": "Bob", "age": "25"},
        ]
        mock_generator.return_value = mock_gen_instance

        mock_prod_instance = MagicMock()
        mock_prod_instance.flush.return_value = 0
        mock_producer.return_value = mock_prod_instance

        result = runner.invoke(
            cli,
            [
                "produce",
                "--topic", "test-topic",
                "--format", "json",
                "--generator", "csv",
                "--csv-file", temp_csv,
                "--count", "2",
            ],
        )

        assert result.exit_code == 0
        assert "Starting data generation" in result.output
        assert "Generator: csv" in result.output

    def test_produce_dry_run(self, runner):
        """Test produce command with dry-run flag."""
        result = runner.invoke(
            cli,
            [
                "produce",
                "--topic", "test-topic",
                "--dry-run",
                "--count", "1",
            ],
        )

        assert result.exit_code == 0
        assert "Key:" in result.output
        assert "Value:" in result.output

    def test_produce_with_config(self, runner, temp_config):
        """Test produce command with configuration file."""
        result = runner.invoke(
            cli,
            [
                "produce",
                "--topic", "test-topic",
                "--config", temp_config,
                "--dry-run",
                "--count", "1",
            ],
        )

        assert result.exit_code == 0
        assert "Rate: 5.0 msg/s" in result.output  # From config file

    def test_produce_avro_requires_schema(self, runner):
        """Test that Avro format requires schema file."""
        result = runner.invoke(
            cli,
            [
                "produce",
                "--topic", "test-topic",
                "--format", "avro",
            ],
        )

        assert result.exit_code != 0
        assert "requires --schema-file" in result.output

    def test_produce_csv_requires_file(self, runner):
        """Test that CSV generator requires CSV file."""
        result = runner.invoke(
            cli,
            [
                "produce",
                "--topic", "test-topic",
                "--generator", "csv",
            ],
        )

        assert result.exit_code != 0
        assert "requires --csv-file" in result.output

    @patch("testdatapy.cli.FakerGenerator")
    def test_produce_with_duration(self, mock_generator, runner):
        """Test produce command with duration limit."""
        # Setup mock that generates many messages
        mock_gen_instance = MagicMock()
        mock_gen_instance.generate.return_value = (
            {"id": i, "name": f"Test{i}"} for i in range(1000)
        )
        mock_generator.return_value = mock_gen_instance

        result = runner.invoke(
            cli,
            [
                "produce",
                "--topic", "test-topic",
                "--dry-run",
                "--duration", "0.1",  # Very short duration
                "--rate", "10",
            ],
        )

        assert result.exit_code == 0
        # Should stop due to duration, not message count
        assert "Duration: 0." in result.output

    def test_produce_with_key_field(self, runner):
        """Test produce command with key field extraction."""
        result = runner.invoke(
            cli,
            [
                "produce",
                "--topic", "test-topic",
                "--key-field", "id",
                "--dry-run",
                "--count", "1",
            ],
        )

        assert result.exit_code == 0
        assert "Key:" in result.output
