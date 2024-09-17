import pytest
from click.testing import CliRunner
from geonames.cli import cli
from unittest.mock import patch

@pytest.fixture
def runner():
    return CliRunner()

def test_import_data(runner):
    with patch('geonames.cli.sync_wrapper') as mock_sync_wrapper:
        result = runner.invoke(cli, ['import-data', '--input-file', 'test_input.txt', '--db-file', 'test_db.db'])
        assert result.exit_code == 0
        assert "Importing data from test_input.txt into test_db.db" in result.output
        assert "Data import completed successfully." in result.output
        mock_sync_wrapper.assert_called_once()
        assert mock_sync_wrapper.call_args[0][0].__name__ == 'setup_database'

def test_import_data_error(runner):
    with patch('geonames.cli.sync_wrapper', side_effect=Exception("Test error")):
        result = runner.invoke(cli, ['import-data', '--input-file', 'test_input.txt', '--db-file', 'test_db.db'])
        assert result.exit_code == 0
        assert "Importing data from test_input.txt into test_db.db" in result.output
        assert "Error during data import: Test error" in result.output

def test_search_by_name(runner):
    mock_results = [{"name": "Test City", "country": "TC", "latitude": 0.0, "longitude": 0.0}]
    with patch('geonames.database.search_by_name', return_value=mock_results):
        result = runner.invoke(cli, ['search', '--db-file', 'test_db.db', '--name', 'Test'])
        assert result.exit_code == 0
        assert "Searching in test_db.db" in result.output
        assert "Found: {'name': 'Test City', 'country': 'TC', 'latitude': 0.0, 'longitude': 0.0}" in result.output

def test_search_by_postal_code(runner):
    mock_results = [{"name": "Test City", "country": "US", "latitude": 0.0, "longitude": 0.0}]
    with patch('geonames.database.search_by_postal_code', return_value=mock_results):
        result = runner.invoke(cli, ['search', '--db-file', 'test_db.db', '--postal-code', '12345', '--country-code', 'US'])
        assert result.exit_code == 0
        assert "Searching in test_db.db" in result.output
        assert "Found: {'name': 'Test City', 'country': 'US', 'latitude': 0.0, 'longitude': 0.0}" in result.output

def test_search_by_country_code(runner):
    mock_results = [{"name": "Test City", "country": "US", "latitude": 0.0, "longitude": 0.0}]
    with patch('geonames.database.search_by_country_code', return_value=mock_results):
        result = runner.invoke(cli, ['search', '--db-file', 'test_db.db', '--country-code', 'US'])
        assert result.exit_code == 0
        assert "Searching in test_db.db" in result.output
        assert "Found: {'name': 'Test City', 'country': 'US', 'latitude': 0.0, 'longitude': 0.0}" in result.output

def test_search_by_coordinates(runner):
    mock_results = [{"name": "Test City", "country": "US", "latitude": 0.1, "longitude": 0.1}]
    with patch('geonames.database.search_by_coordinates', return_value=mock_results):
        result = runner.invoke(cli, ['search', '--db-file', 'test_db.db', '--lat', '0.0', '--lon', '0.0', '--radius', '20'])
        assert result.exit_code == 0
        assert "Searching in test_db.db" in result.output
        assert "Found: {'name': 'Test City', 'country': 'US', 'latitude': 0.1, 'longitude': 0.1}" in result.output

def test_search_no_criteria(runner):
    result = runner.invoke(cli, ['search', '--db-file', 'test_db.db'])
    assert result.exit_code == 0
    assert "Please provide a search criteria" in result.output

def test_search_no_results(runner):
    with patch('geonames.database.search_by_name', return_value=[]):
        result = runner.invoke(cli, ['search', '--db-file', 'test_db.db', '--name', 'Nonexistent'])
        assert result.exit_code == 0
        assert "Searching in test_db.db" in result.output
        assert "No results found" in result.output

def test_search_error(runner):
    with patch('geonames.database.search_by_name', side_effect=Exception("Test error")):
        result = runner.invoke(cli, ['search', '--db-file', 'test_db.db', '--name', 'Test'])
        assert result.exit_code == 0
        assert "Searching in test_db.db" in result.output
        assert "Error during search: Test error" in result.output

def test_stats(runner):
    with patch('geonames.database.get_total_entries', return_value=1000), \
         patch('geonames.database.get_country_count', return_value=50), \
         patch('geonames.database.get_top_countries', return_value=[("Country A", 200), ("Country B", 150), ("Country C", 100), ("Country D", 50), ("Country E", 25)]):
        result = runner.invoke(cli, ['stats', '--db-file', 'test_db.db'])
        assert result.exit_code == 0
        assert "Displaying statistics for test_db.db" in result.output
        assert "Total entries: 1000" in result.output
        assert "Number of countries: 50" in result.output
        assert "Top 5 countries by number of entries:" in result.output
        assert "Country A: 200" in result.output

def test_stats_error(runner):
    with patch('geonames.cli.sync_wrapper', side_effect=Exception("Test error")):
        result = runner.invoke(cli, ['stats', '--db-file', 'test_db.db'])
        assert result.exit_code == 0
        assert "Displaying statistics for test_db.db" in result.output
        assert "Error retrieving statistics: Test error" in result.output