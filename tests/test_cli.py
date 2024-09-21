import pytest
from click.testing import CliRunner
from unittest.mock import patch, AsyncMock
from pathlib import Path

from geonames.cli import cli

@pytest.fixture
def mock_config(tmp_path):
    class MockConfig:
        DATABASE_FILEPATH = tmp_path / "test_db.db"
        TXT_FILE = tmp_path / "test_input.txt"
        ZIP_FILE = tmp_path / "test_input.zip"
    return MockConfig()


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_db_file(tmp_path):
    db_file = tmp_path / "test_db.db"
    db_file.touch()
    return str(db_file)


@pytest.fixture
def mock_database_exists():
    with patch("geonames.database.database_exists", new_callable=AsyncMock, return_value=True) as mock:
        yield mock

@pytest.fixture
def mock_database_not_exists():
    with patch("geonames.database.database_exists", new_callable=AsyncMock, return_value=False) as mock:
        yield mock


@pytest.fixture
def mock_input_file(tmp_path):
    input_file = tmp_path / "test_input.txt"
    input_file.write_text("Sample data")
    return str(input_file)


def test_search_by_name(runner, mock_db_file, mock_database_exists):
    mock_results = [
        {"name": "Test City", "country": "TC", "latitude": 0.0, "longitude": 0.0}
    ]
    with patch("geonames.database.search_by_name", new_callable=AsyncMock, return_value=mock_results):
        result = runner.invoke(cli, ["search", "--db-file", mock_db_file, "--name", "Test"])
        assert result.exit_code == 0
        assert f"Searching in {mock_db_file}" in result.output
        assert "Found: {'name': 'Test City'" in result.output


def test_search_by_postal_code(runner, mock_db_file, mock_database_exists):
    mock_results = [
        {"name": "Test City", "country": "US", "latitude": 0.0, "longitude": 0.0}
    ]
    with patch("geonames.database.search_by_postal_code", return_value=mock_results):
        result = runner.invoke(
            cli,
            [
                "search",
                "--db-file",
                mock_db_file,
                "--postal-code",
                "12345",
                "--country-code",
                "US",
            ],
        )
        assert result.exit_code == 0
        assert f"Searching in {mock_db_file}" in result.output
        assert "Found: {'name': 'Test City'" in result.output


def test_search_by_country_code(runner, mock_db_file, mock_database_exists):
    mock_results = [
        {"name": "Test City", "country": "US", "latitude": 0.0, "longitude": 0.0}
    ]
    with patch("geonames.database.search_by_country_code", return_value=mock_results):
        result = runner.invoke(
            cli, ["search", "--db-file", mock_db_file, "--country-code", "US"]
        )
        assert result.exit_code == 0
        assert f"Searching in {mock_db_file}" in result.output
        assert "Found: {'name': 'Test City'" in result.output


def test_search_by_coordinates(runner, mock_db_file, mock_database_exists):
    mock_results = [
        {"name": "Test City", "country": "US", "latitude": 0.1, "longitude": 0.1}
    ]
    with patch("geonames.database.search_by_coordinates", return_value=mock_results):
        result = runner.invoke(
            cli,
            [
                "search",
                "--db-file",
                mock_db_file,
                "--lat",
                "0.0",
                "--lon",
                "0.0",
                "--radius",
                "20",
            ],
        )
        assert result.exit_code == 0
        assert f"Searching in {mock_db_file}" in result.output
        assert "Found: {'name': 'Test City'" in result.output


def test_search_no_criteria(runner, mock_db_file, mock_database_exists):
    result = runner.invoke(cli, ["search", "--db-file", mock_db_file])
    assert result.exit_code == 1
    assert "Please provide a search criteria" in result.output


def test_search_no_results(runner, mock_db_file, mock_database_exists):
    with patch("geonames.database.search_by_name", return_value=[]):
        result = runner.invoke(
            cli, ["search", "--db-file", mock_db_file, "--name", "Nonexistent"]
        )
        assert result.exit_code == 0
        assert f"Searching in {mock_db_file}" in result.output
        assert "No results found" in result.output


def test_search_error(runner, mock_db_file, mock_database_exists):
    with patch("geonames.database.search_by_name", side_effect=Exception("Test error")):
        result = runner.invoke(cli, ["search", "--db-file", mock_db_file, "--name", "Test"])
        assert result.exit_code == 1
        assert "Error during search: Test error" in result.output


def test_stats(runner, mock_db_file, mock_database_exists):
    with patch("geonames.database.get_total_entries", return_value=1000), patch(
        "geonames.database.get_country_count", return_value=50
    ), patch(
        "geonames.database.get_top_countries",
        return_value=[
            ("Country A", 200),
            ("Country B", 150),
            ("Country C", 100),
            ("Country D", 50),
            ("Country E", 25),
        ],
    ):
        result = runner.invoke(cli, ["stats", "--db-file", mock_db_file])
        assert result.exit_code == 0
        assert f"Displaying statistics for {mock_db_file}" in result.output
        assert "Total entries: 1000" in result.output
        assert "Number of countries: 50" in result.output


def test_stats_error(runner, mock_db_file, mock_database_exists):
    with patch(
        "geonames.database.get_total_entries", side_effect=Exception("Test error")
    ):
        result = runner.invoke(cli, ["stats", "--db-file", mock_db_file])
        assert result.exit_code == 1  # Changed from 0 to 1
        assert f"Displaying statistics for {mock_db_file}" in result.output
        assert "Error retrieving statistics: Test error" in result.output
        assert "If the problem persists, please ensure the database is properly set up" in result.output


def test_search_database_not_found(runner, mock_config):
    non_existent_db = mock_config.DATABASE_FILEPATH.parent / "non_existent.db"
    result = runner.invoke(cli, ["search", "--db-file", str(non_existent_db), "--name", "Test"])
    assert result.exit_code != 0
    assert f"Database file not found at {non_existent_db}" in result.output


def test_search_database_tables_not_found(runner, mock_db_file, mock_database_not_exists):
    result = runner.invoke(cli, ["search", "--db-file", mock_db_file, "--name", "Test"])
    assert result.exit_code == 1
    assert "Database tables not found. Please run the import-data command first." in result.output


def test_import_data(runner, mock_config):
    with patch("geonames.cli.Config", return_value=mock_config), \
         patch("geonames.database.setup_database", new_callable=AsyncMock), \
         patch("geonames.database.get_total_entries", new_callable=AsyncMock, return_value=1000), \
         patch("geonames.database.get_country_count", new_callable=AsyncMock, return_value=50):
        
        result = runner.invoke(cli, [
            "import-data",
            "--input-file", str(mock_config.TXT_FILE),
            "--db-file", str(mock_config.DATABASE_FILEPATH),
        ])
        
        assert result.exit_code == 0
        assert f"Importing data from {mock_config.TXT_FILE} into {mock_config.DATABASE_FILEPATH}" in result.output
        assert "Data import completed successfully." in result.output
        assert "Total entries in database: 1000" in result.output
        assert "Number of countries: 50" in result.output


def test_stats_empty_database(runner, mock_db_file, mock_database_exists):
    with patch("geonames.database.get_total_entries", new_callable=AsyncMock, return_value=0), \
         patch("geonames.database.get_country_count", new_callable=AsyncMock, return_value=0), \
         patch("geonames.database.get_top_countries", new_callable=AsyncMock, return_value=[]):
        result = runner.invoke(cli, ["stats", "--db-file", mock_db_file])
        assert result.exit_code == 0
        assert "Total entries: 0" in result.output
        assert "Number of countries: 0" in result.output
        assert "Top 5 countries by number of entries:" in result.output
        assert "No data available" in result.output


def test_import_data(runner, mock_db_file):
    with patch("geonames.database.setup_database", new_callable=AsyncMock), \
         patch("geonames.database.get_total_entries", new_callable=AsyncMock, return_value=1000), \
         patch("geonames.database.get_country_count", new_callable=AsyncMock, return_value=50):
        
        result = runner.invoke(cli, [
            "import-data",
            "--input-file", "test_input.txt",
            "--db-file", mock_db_file,
        ])
        
        assert result.exit_code == 0
        assert f"Importing data from test_input.txt into {mock_db_file}" in result.output
        assert "Data import completed successfully." in result.output
        assert "Total entries in database: 1000" in result.output
        assert "Number of countries: 50" in result.output


def test_import_data_file_not_found(runner, mock_db_file):
    non_existent_file = "non_existent_file.txt"
    with patch("geonames.cli.Path.exists", return_value=False), \
         patch("geonames.cli.check_for_updates", new_callable=AsyncMock, return_value=True), \
         patch("geonames.cli.download_zip", new_callable=AsyncMock), \
         patch("geonames.cli.extract_zip", new_callable=AsyncMock, side_effect=FileNotFoundError(f"[Errno 2] No such file or directory: '{non_existent_file}'")):
        result = runner.invoke(cli, [
            "import-data",
            "--input-file", non_existent_file,
            "--db-file", mock_db_file,
        ])
        assert result.exit_code == 1
        assert "Error during data import: " in result.output
        assert f"[Errno 2] No such file or directory: '{non_existent_file}'" in result.output


def test_import_data_error(runner, mock_db_file):
    with patch("geonames.database.setup_database", side_effect=Exception("Test error")):
        result = runner.invoke(cli, [
            "import-data",
            "--input-file", "test_input.txt",
            "--db-file", mock_db_file,
        ])
        assert result.exit_code == 1
        assert "Error during data import: Test error" in result.output


@pytest.mark.parametrize(
    "command,options,error_message",
    [
        ("search", ["--name", "Test"], "Error during search: Database connection error"),
        ("search", ["--postal-code", "12345", "--country-code", "US"], "Error during search: Database connection error"),
        ("search", ["--country-code", "US"], "Error during search: Database connection error"),
        ("search", ["--lat", "0.0", "--lon", "0.0", "--radius", "20"], "Error during search: Database connection error"),
        ("stats", [], "Error retrieving statistics: Database connection error"),
    ],
)
def test_database_connection_error(runner, mock_db_file, command, options, error_message):
    with patch("geonames.cli.create_async_engine", side_effect=Exception("Database connection error")), \
         patch("geonames.database.database_exists", return_value=True):
        result = runner.invoke(cli, [command, "--db-file", mock_db_file] + options)
        assert result.exit_code == 1
        assert error_message in result.output
        assert "If the problem persists, please ensure the database is properly set up" in result.output


def test_search_invalid_coordinates(runner, mock_db_file):
    result = runner.invoke(cli, [
        "search",
        "--db-file", mock_db_file,
        "--lat", "invalid",
        "--lon", "0.0",
        "--radius", "20"
    ])
    assert result.exit_code == 2  # Click's exit code for invalid input
    assert "Invalid value for '--lat'" in result.output
