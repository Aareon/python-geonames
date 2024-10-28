from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from loguru import logger
from sqlalchemy import Insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from geonames.config import Config
from geonames.database import (
    bulk_insert_data,
    check_database_update_needed,
    create_async_session,
    create_database,
    database_exists,
    execute_query,
    get_country_count,
    get_geolocation,
    get_top_countries,
    get_total_entries,
    optimize_database,
    search_by_coordinates,
    search_by_country_code,
    search_by_name,
    search_by_postal_code,
    search_locations,
    setup_database,
)
from geonames.models import Base, Geoname


@pytest.fixture
def mock_getmtime():
    """Mock os.path.getmtime to return a fixed timestamp."""
    with patch(
        "os.path.getmtime",
        return_value=datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp(),
    ):
        yield


@pytest.fixture
def config(tmp_path):
    """Create a test configuration with temporary paths."""
    config = Config()
    config.DATABASE_FILEPATH = tmp_path / "test.db"
    config.ZIP_FILE = tmp_path / "test.zip"
    config.URL = "http://example.com/test.zip"
    return config


@pytest.fixture(autouse=True)
def setup_logger(caplog):
    """Set up logging for tests."""
    handler_id = logger.add(caplog.handler, level="INFO")
    yield
    logger.remove(handler_id)


@pytest.fixture
async def mock_engine():
    """Create a mock SQLAlchemy async engine with proper cleanup."""
    engine = AsyncMock(spec=AsyncEngine)
    yield engine
    await engine.dispose()


@pytest.fixture
def mock_conn():
    """Create a mock database connection."""
    conn = AsyncMock()
    conn.execute = AsyncMock()
    return conn


@pytest.fixture
def test_data():
    """Create test data for database operations."""
    return [{"country_code": "US", "postal_code": "12345"}]


@pytest.mark.asyncio
async def test_create_async_session(mock_engine):
    """Test creating an async database session."""
    session = await create_async_session(mock_engine)
    assert session is not None


@pytest.mark.asyncio
async def test_execute_query(mock_engine):
    """Test executing a database query."""
    mock_session = AsyncMock()

    async def mock_query_func(session, *args):
        return [1, 2, 3]

    with patch(
        "geonames.database.create_async_session",
        return_value=AsyncMock(return_value=mock_session),
    ):
        result = await execute_query(mock_engine, mock_query_func)
        assert result == [1, 2, 3]


@pytest.mark.asyncio
async def test_execute_query_error(mock_engine):
    """Test handling of database query errors."""

    async def error_query(session, *args):
        raise SQLAlchemyError("Query execution failed")

    with pytest.raises(SQLAlchemyError):
        await execute_query(mock_engine, error_query)


@pytest.mark.asyncio
async def test_check_database_update_needed(mock_engine, config, mock_getmtime):
    """Test detecting when database needs updates."""
    # Create both database and zip files
    config.DATABASE_FILEPATH.touch()
    config.ZIP_FILE.touch()

    # Mock connection context manager
    mock_conn = AsyncMock()
    mock_result = AsyncMock()
    mock_result.scalar_one.return_value = 100
    mock_conn.execute.return_value = mock_result
    mock_engine.connect.return_value.__aenter__.return_value = mock_conn

    # Mock methods directly with simple return values
    with patch(
        "geonames.database.create_async_engine", return_value=mock_engine
    ), patch("geonames.database.database_exists", return_value=True), patch(
        "geonames.database.check_for_updates", return_value=False
    ), patch("geonames.database.get_total_entries", return_value=100):
        # Execute test
        result = await check_database_update_needed(config)
        assert result is False

        # Test update scenario by changing check_for_updates return
        with patch("geonames.database.check_for_updates", return_value=True):
            result = await check_database_update_needed(config)
            assert result is True

    # Clean up test files
    config.DATABASE_FILEPATH.unlink(missing_ok=True)
    config.ZIP_FILE.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_check_database_update_needed_empty_database(
    mock_engine, config, mock_getmtime
):
    """Test handling of empty database."""
    # Create both files
    config.DATABASE_FILEPATH.touch()
    config.ZIP_FILE.touch()

    # Mock connection context manager
    mock_conn = AsyncMock()
    mock_result = AsyncMock()
    mock_result.scalar_one.return_value = 0
    mock_conn.execute.return_value = mock_result
    mock_engine.connect.return_value.__aenter__.return_value = mock_conn

    with patch(
        "geonames.database.create_async_engine", return_value=mock_engine
    ), patch("geonames.database.database_exists", return_value=True), patch(
        "geonames.database.check_for_updates", return_value=True
    ), patch("geonames.database.get_total_entries", return_value=0):
        result = await check_database_update_needed(config)
        assert result is True

    # Clean up test files
    config.DATABASE_FILEPATH.unlink(missing_ok=True)
    config.ZIP_FILE.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_check_database_update_needed_missing_database(mock_engine, config):
    """Test handling of missing database."""
    with patch(
        "geonames.database.create_async_engine", return_value=mock_engine
    ), patch(
        "geonames.database.database_exists", new_callable=AsyncMock, return_value=False
    ):
        result = await check_database_update_needed(config)
        assert result is True


@pytest.mark.asyncio
async def test_check_database_update_needed_error(mock_engine, config):
    """Test handling of database connection errors."""
    with patch("geonames.database.create_async_engine", return_value=mock_engine):
        mock_engine.connect.side_effect = SQLAlchemyError("Connection error")
        with pytest.raises(SQLAlchemyError):
            await check_database_update_needed(config)


@pytest.mark.asyncio
async def test_database_exists(mock_engine):
    """Test checking database existence."""
    mock_engine.connect.return_value.__aenter__.return_value.run_sync.return_value = (
        True
    )
    assert await database_exists(mock_engine) is True

    mock_engine.connect.return_value.__aenter__.return_value.run_sync.return_value = (
        False
    )
    assert await database_exists(mock_engine) is False

    mock_engine.connect.side_effect = SQLAlchemyError()
    with pytest.raises(SQLAlchemyError):
        await database_exists(mock_engine)


@pytest.mark.asyncio
async def test_create_database(mock_engine):
    """Test database creation."""
    with patch.object(Base.metadata, "create_all") as mock_create_all, patch.object(
        mock_engine, "begin"
    ) as mock_begin:
        mock_conn = AsyncMock()
        mock_begin.return_value.__aenter__.return_value = mock_conn
        await create_database(mock_engine)
        mock_begin.assert_called_once()
        mock_begin.return_value.__aenter__.assert_called_once()
        mock_begin.return_value.__aenter__.return_value.run_sync.assert_called_once_with(
            mock_create_all
        )


@pytest.mark.asyncio
async def test_bulk_insert_data_success(mock_engine, mock_conn, test_data, caplog):
    """Test successful bulk data insertion."""
    mock_engine.begin.return_value.__aenter__.return_value = mock_conn
    await bulk_insert_data(mock_engine, test_data)

    mock_conn.execute.assert_called_once()
    call_args = mock_conn.execute.call_args
    assert call_args is not None
    args, kwargs = call_args

    assert len(args) == 2
    assert isinstance(args[0], Insert)
    assert args[1] == test_data

    assert f"Inserting {len(test_data)} items" in caplog.text
    assert "Insert completed successfully" in caplog.text


@pytest.mark.asyncio
async def test_bulk_insert_data_error(mock_engine):
    """Test handling of bulk insert errors."""
    mock_conn = AsyncMock()
    mock_engine.begin.return_value.__aenter__.return_value = mock_conn
    mock_conn.execute.side_effect = SQLAlchemyError("Database error")

    with pytest.raises(SQLAlchemyError):
        await bulk_insert_data(
            mock_engine, [{"country_code": "US", "postal_code": "12345"}]
        )


@pytest.mark.asyncio
async def test_optimize_database(mock_engine):
    """Test database optimization."""
    mock_conn = AsyncMock()
    mock_engine.begin.return_value.__aenter__.return_value = mock_conn
    await optimize_database(mock_engine)

    mock_conn.execute.assert_called_once()
    call_args = mock_conn.execute.call_args
    assert call_args is not None
    stmt = call_args[0][0]
    assert str(stmt).strip().lower() == "pragma optimize"


@pytest.mark.asyncio
async def test_get_geolocation(mock_engine):
    """Test geolocation retrieval."""
    mock_result = [
        Geoname(
            place_name="Test City",
            country_code="US",
            latitude=1.0,
            longitude=1.0,
            admin_name1="State",
            admin_code1="ST",
        )
    ]
    with patch("geonames.database.execute_query", return_value=mock_result):
        result = await get_geolocation(mock_engine, "US", "12345")
        assert len(result) == 1
        assert result[0]["city"] == "Test City"
        assert result[0]["state"] == "State"


@pytest.mark.asyncio
async def test_get_geolocation_not_found(mock_engine):
    """Test handling of non-existent geolocation."""
    with patch("geonames.database.execute_query", return_value=[]):
        result = await get_geolocation(mock_engine, "XX", "99999")
        assert result == []


@pytest.mark.asyncio
async def test_setup_database(mock_engine, config):
    """Test database setup."""
    with patch(
        "geonames.database.check_database_update_needed", return_value=False
    ), patch("geonames.database.create_async_engine", return_value=mock_engine), patch(
        "geonames.database.Base.metadata.create_all"
    ), patch("geonames.database.get_total_entries", return_value=1):
        result = await setup_database(config)
        assert isinstance(result, AsyncEngine)


@pytest.mark.asyncio
async def test_search_by_name(mock_engine):
    """Test searching locations by name."""
    mock_result = [
        Geoname(place_name="Test City", country_code="US", latitude=1.0, longitude=1.0)
    ]
    with patch("geonames.database.execute_query", return_value=mock_result):
        result = await search_by_name(mock_engine, "Test")
        assert len(result) == 1
        assert result[0]["name"] == "Test City"


@pytest.mark.asyncio
async def test_search_by_postal_code(mock_engine):
    """Test searching locations by postal code."""
    mock_result = [
        Geoname(place_name="Test City", country_code="US", latitude=1.0, longitude=1.0)
    ]
    with patch("geonames.database.execute_query", return_value=mock_result):
        result = await search_by_postal_code(mock_engine, "US", "12345")
        assert len(result) == 1
        assert result[0]["name"] == "Test City"


@pytest.mark.asyncio
async def test_search_by_country_code(mock_engine):
    """Test searching locations by country code."""
    mock_geonames = [
        Geoname(place_name="City1", country_code="US", latitude=1.0, longitude=1.0),
        Geoname(place_name="City2", country_code="US", latitude=2.0, longitude=2.0),
    ]
    with patch("geonames.database.execute_query", return_value=mock_geonames):
        result = await search_by_country_code(mock_engine, "US")
        assert len(result) == 2
        assert result[0]["name"] == "City1"
        assert result[1]["name"] == "City2"


@pytest.mark.asyncio
async def test_search_by_coordinates(mock_engine):
    """Test searching locations by coordinates."""
    mock_result = [
        Geoname(place_name="Test City", country_code="US", latitude=1.0, longitude=1.0)
    ]
    with patch("geonames.database.execute_query", return_value=mock_result):
        result = await search_by_coordinates(mock_engine, 1.0, 1.0, 10)
        assert len(result) == 1
        assert result[0]["name"] == "Test City"


@pytest.mark.asyncio
async def test_search_by_coordinates_invalid_input(mock_engine):
    """Test handling of invalid coordinate inputs."""
    with pytest.raises(ValueError):
        await search_by_coordinates(mock_engine, "invalid", "invalid", 10)


@pytest.mark.asyncio
async def test_search_by_coordinates_error_handling(mock_engine):
    """Test error handling in coordinate search."""
    with patch("geonames.database.execute_query", side_effect=Exception("Test error")):
        result = await search_by_coordinates(mock_engine, 0, 0, 10)
        assert result == []


@pytest.mark.asyncio
async def test_get_total_entries(mock_engine):
    """Test getting total database entries."""
    with patch("geonames.database.execute_query", return_value=100):
        result = await get_total_entries(mock_engine)
        assert result == 100


@pytest.mark.asyncio
async def test_get_country_count(mock_engine):
    """Test getting total number of countries."""
    with patch("geonames.database.execute_query", return_value=50):
        result = await get_country_count(mock_engine)
        assert result == 50


@pytest.mark.asyncio
async def test_get_top_countries(mock_engine):
    """Test getting top countries by entry count."""
    mock_result = [("US", 100), ("CA", 50), ("UK", 25)]
    with patch("geonames.database.execute_query", return_value=mock_result):
        result = await get_top_countries(mock_engine)
        assert len(result) == 3
        assert result[0] == ("US", 100)


@pytest.mark.asyncio
async def test_get_top_countries_empty_result(mock_engine):
    """Test handling of empty result in top countries query."""
    with patch("geonames.database.execute_query", return_value=[]):
        result = await get_top_countries(mock_engine)
        assert result == []


@pytest.mark.asyncio
async def test_search_locations(mock_engine):
    """Test the search_locations helper function."""
    mock_result = [
        Geoname(
            place_name="Test Location", country_code="US", latitude=1.0, longitude=1.0
        )
    ]

    async def mock_query_func(session):
        return mock_result

    with patch("geonames.database.execute_query", return_value=mock_result):
        result = await search_locations(mock_engine, mock_query_func)
        assert len(result) == 1
        assert result[0]["name"] == "Test Location"
        assert result[0]["country"] == "US"
        assert result[0]["latitude"] == 1.0
        assert result[0]["longitude"] == 1.0


@pytest.mark.asyncio
async def test_search_locations_empty_result(mock_engine):
    """Test search_locations with empty results."""

    async def mock_query_func(session):
        return []

    result = await search_locations(mock_engine, mock_query_func)
    assert result == []


@pytest.mark.asyncio
async def test_setup_database_update_needed(mock_engine, config):
    """Test database setup when update is needed."""
    with patch(
        "geonames.database.check_database_update_needed", return_value=True
    ), patch("geonames.database.check_for_updates", return_value=True), patch(
        "geonames.database.download_zip"
    ), patch("geonames.database.extract_zip"), patch(
        "geonames.database.load_data_in_chunks", return_value=[]
    ), patch("geonames.database.create_async_engine", return_value=mock_engine), patch(
        "geonames.database.Base.metadata.create_all"
    ):
        result = await setup_database(config)
        assert isinstance(result, AsyncEngine)


@pytest.mark.asyncio
async def test_setup_database_error_handling(mock_engine, config):
    """Test error handling during database setup."""
    with patch(
        "geonames.database.check_database_update_needed",
        side_effect=Exception("Setup error"),
    ), patch("geonames.database.create_async_engine", return_value=mock_engine):
        with pytest.raises(Exception) as exc_info:
            await setup_database(config)
        assert str(exc_info.value) == "Setup error"


@pytest.mark.asyncio
async def test_search_by_coordinates_with_valid_radius(mock_engine):
    """Test searching locations with valid radius parameter."""
    mock_result = [
        Geoname(
            place_name="Nearby City", country_code="US", latitude=1.1, longitude=1.1
        )
    ]
    with patch("geonames.database.execute_query", return_value=mock_result):
        result = await search_by_coordinates(mock_engine, 1.0, 1.0, 20.0)
        assert len(result) == 1
        assert result[0]["name"] == "Nearby City"


@pytest.mark.asyncio
async def test_search_by_coordinates_zero_radius(mock_engine):
    """Test searching locations with zero radius."""
    # Don't mock execute_query to allow validation to run
    with pytest.raises(ValueError, match="Radius must be positive"):
        await search_by_coordinates(mock_engine, 1.0, 1.0, 0.0)


@pytest.mark.asyncio
async def test_search_by_coordinates_negative_radius(mock_engine):
    """Test searching locations with negative radius."""
    # Don't mock execute_query to allow validation to run
    with pytest.raises(ValueError, match="Radius must be positive"):
        await search_by_coordinates(mock_engine, 1.0, 1.0, -10.0)


@pytest.mark.asyncio
async def test_database_exists_with_error(mock_engine):
    """Test database_exists with connection error."""
    mock_engine.connect.side_effect = SQLAlchemyError("Connection failed")

    with pytest.raises(SQLAlchemyError) as exc_info:
        await database_exists(mock_engine)
    assert "Connection failed" in str(exc_info.value)
