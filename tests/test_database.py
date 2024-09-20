from unittest.mock import AsyncMock, MagicMock, patch, create_autospec

import pytest
from loguru import logger
from sqlalchemy import Table, Insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from pathlib import Path

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


class AsyncIteratorMock:
    def __init__(self, seq):
        self.iter = iter(seq)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.iter)
        except StopIteration:
            raise StopAsyncIteration

class AsyncContextManagerMock:
    def __init__(self, return_value):
        self.return_value = return_value

    async def __aenter__(self):
        return self.return_value

    async def __aexit__(self, exc_type, exc, tb):
        pass


@pytest.fixture(autouse=True)
def setup_logger(caplog):
    handler_id = logger.add(caplog.handler, level="INFO")
    yield
    logger.remove(handler_id)


@pytest.fixture
def mock_engine():
    return AsyncMock(spec=AsyncEngine)


@pytest.mark.asyncio
async def test_create_async_session(mock_engine):
    session = await create_async_session(mock_engine)
    assert session is not None


@pytest.mark.asyncio
async def test_execute_query(mock_engine):
    mock_session = AsyncMock()

    async def mock_query_func(session, *args):
        return [1, 2, 3]

    with patch('geonames.database.create_async_session', return_value=AsyncMock(return_value=mock_session)):
        result = await execute_query(mock_engine, mock_query_func)
        assert result == [1, 2, 3]


@pytest.mark.asyncio
async def test_check_database_update_needed():
    config = Config()
    config.DATABASE_FILEPATH = Path("test.db")
    config.ZIP_FILE = Path("test.zip")
    config.URL = "http://example.com/geonames.zip"

    mock_engine = AsyncMock(spec=AsyncEngine)

    with patch("pathlib.Path.exists", return_value=True), \
         patch("os.path.getmtime", return_value=1000), \
         patch("geonames.database.check_for_updates", new_callable=AsyncMock) as mock_check_updates, \
         patch("geonames.database.create_async_engine", return_value=mock_engine), \
         patch("geonames.database.database_exists", new_callable=AsyncMock) as mock_db_exists:

        mock_check_updates.return_value = False
        mock_db_exists.return_value = True

        # Create a connection context manager mock
        mock_connection = AsyncMock()
        mock_engine.connect.return_value.__aenter__.return_value = mock_connection

        # Mock the execute method to return a result that can be used with scalar_one
        mock_result = AsyncMock()
        mock_result.scalar_one.return_value = 1  # Simulate a non-empty database
        mock_connection.execute.return_value = mock_result

        result = await check_database_update_needed(config)

        assert result is False
        mock_db_exists.assert_awaited_once_with(mock_engine)
        mock_check_updates.assert_awaited_once_with(config.URL, config.ZIP_FILE)
        await mock_engine.dispose()  # Ensure we're awaiting the dispose method

@pytest.mark.asyncio
async def test_database_exists(mock_engine):
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
    with patch.object(Base.metadata, "create_all") as mock_create_all, patch.object(
        mock_engine, "begin"
    ) as mock_begin:
        mock_conn = AsyncMock()
        mock_begin.return_value.__aenter__.return_value = mock_conn
        await create_database(mock_engine)
        mock_begin.assert_called_once()
        # Check if create_all is called within the context manager
        mock_begin.return_value.__aenter__.assert_called_once()
        mock_begin.return_value.__aenter__.return_value.run_sync.assert_called_once_with(
            mock_create_all
        )


@pytest.fixture
def mock_conn():
    conn = AsyncMock()
    conn.execute = AsyncMock()
    return conn


@pytest.fixture
def test_data():
    return [{"country_code": "US", "postal_code": "12345"}]


@pytest.mark.asyncio
async def test_bulk_insert_data_success(mock_engine, mock_conn, test_data, caplog):
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
async def test_bulk_insert_data_error(mock_engine, mock_conn, test_data, caplog):
    mock_engine.begin.return_value.__aenter__.return_value = mock_conn
    mock_conn.execute.side_effect = Exception("Database error")

    with pytest.raises(Exception, match="Database error"):
        with patch("geonames.database.Geoname.__table__"):
            await bulk_insert_data(mock_engine, test_data)

    assert f"Inserting {len(test_data)} items" in caplog.text
    assert "Error during insert: Database error" in caplog.text


@pytest.mark.asyncio
async def test_optimize_database(mock_engine, mock_conn):
    mock_engine.begin.return_value.__aenter__.return_value = mock_conn

    await optimize_database(mock_engine)

    mock_conn.execute.assert_called_once()
    call_args = mock_conn.execute.call_args
    assert call_args is not None, "Execute method was not called"
    stmt = call_args[0][0]
    assert (
        str(stmt).strip().lower() == "pragma optimize"
    ), f"Expected 'PRAGMA optimize', got: {stmt}"


@pytest.mark.asyncio
async def test_geoname_table_structure():
    assert hasattr(
        Geoname, "__table__"
    ), "Geoname model should have a __table__ attribute"
    assert isinstance(
        Geoname.__table__, Table
    ), "Geoname.__table__ should be an instance of sqlalchemy.Table"


@pytest.mark.asyncio
async def test_create_database_structure():
    with patch.object(Base.metadata, "create_all") as mock_create_all:
        mock_engine = AsyncMock(spec=AsyncEngine)
        await create_database(mock_engine)
        mock_engine.begin.assert_called_once()
        context_manager = mock_engine.begin.return_value.__aenter__.return_value
        context_manager.run_sync.assert_called_once_with(mock_create_all)


@pytest.mark.asyncio
async def test_get_geolocation(mock_engine):
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
async def test_check_database_update_needed_file_not_exists(mock_engine):
    config = Config()
    config.DATABASE_FILEPATH = "non_existent.db"
    config.ZIP_FILE = MagicMock()
    config.ZIP_FILE.exists.return_value = False

    with patch(
        "geonames.database.create_async_engine", return_value=mock_engine
    ), patch("geonames.database.database_exists", return_value=False):
        result = await check_database_update_needed(config)
        assert result is True


@pytest.mark.asyncio
async def test_setup_database_update_needed():
    config = Config()
    with patch(
        "geonames.database.check_database_update_needed", return_value=True
    ), patch("geonames.database.check_for_updates", return_value=True), patch(
        "geonames.database.download_zip"
    ), patch("geonames.database.extract_zip"), patch(
        "geonames.database.load_data_in_chunks", return_value=[MagicMock()]
    ), patch("geonames.database.process_chunk", return_value=[]), patch(
        "geonames.database.bulk_insert_data"
    ), patch("geonames.database.optimize_database"), patch(
        "geonames.database.create_async_engine"
    ) as mock_create_engine:
        mock_engine = AsyncMock(spec=AsyncEngine)
        mock_create_engine.return_value = mock_engine
        result = await setup_database(config)
        assert isinstance(result, AsyncEngine)


@pytest.mark.asyncio
async def test_setup_database():
    config = Config()
    with patch(
        "geonames.database.check_database_update_needed", return_value=False
    ), patch("geonames.database.create_async_engine") as mock_create_engine, patch(
        "geonames.database.Base.metadata.create_all"
    ), patch(
        "geonames.database.get_total_entries", return_value=1
    ):
        mock_engine = AsyncMock(spec=AsyncEngine)
        mock_create_engine.return_value = mock_engine

        result = await setup_database(config)
        assert isinstance(result, AsyncEngine)


@pytest.mark.asyncio
async def test_search_locations(mock_engine):
    mock_result = [
        Geoname(place_name="Test City", country_code="US", latitude=1.0, longitude=1.0)
    ]

    async def mock_query_func(session):
        return mock_result

    with patch("geonames.database.execute_query", return_value=mock_result):
        result = await search_locations(mock_engine, mock_query_func)
        assert len(result) == 1
        assert result[0]["name"] == "Test City"


@pytest.mark.asyncio
async def test_search_by_name(mock_engine):
    mock_result = [
        Geoname(place_name="Test City", country_code="US", latitude=1.0, longitude=1.0)
    ]
    with patch("geonames.database.execute_query", return_value=mock_result):
        result = await search_by_name(mock_engine, "Test")
        assert len(result) == 1
        assert result[0]["name"] == "Test City"


@pytest.mark.asyncio
async def test_search_by_postal_code(mock_engine):
    mock_result = [
        Geoname(place_name="Test City", country_code="US", latitude=1.0, longitude=1.0)
    ]
    with patch("geonames.database.execute_query", return_value=mock_result):
        result = await search_by_postal_code(mock_engine, "US", "12345")
        assert len(result) == 1
        assert result[0]["name"] == "Test City"


@pytest.mark.asyncio
async def test_search_by_country_code():
    # Mock the AsyncEngine
    mock_engine = AsyncMock(spec=AsyncEngine)

    # Mock the execute_query function
    mock_geonames = [
        Geoname(place_name="City1", country_code="US", latitude=1.0, longitude=1.0),
        Geoname(place_name="City2", country_code="US", latitude=2.0, longitude=2.0),
    ]

    with patch('geonames.database.execute_query', return_value=mock_geonames):
        result = await search_by_country_code(mock_engine, "US")

    # Check the result
    assert len(result) == 2
    assert result[0] == {
        "name": "City1",
        "country": "US",
        "latitude": 1.0,
        "longitude": 1.0,
    }
    assert result[1] == {
        "name": "City2",
        "country": "US",
        "latitude": 2.0,
        "longitude": 2.0,
    }


@pytest.mark.asyncio
async def test_search_by_coordinates(mock_engine):
    mock_result = [
        Geoname(place_name="Test City", country_code="US", latitude=1.0, longitude=1.0)
    ]
    with patch("geonames.database.execute_query", return_value=mock_result):
        result = await search_by_coordinates(mock_engine, 1.0, 1.0, 10)
        assert len(result) == 1
        assert result[0]["name"] == "Test City"


@pytest.mark.asyncio
async def test_get_total_entries(mock_engine):
    with patch("geonames.database.execute_query", return_value=100):
        result = await get_total_entries(mock_engine)
        assert result == 100


@pytest.mark.asyncio
async def test_get_country_count(mock_engine):
    with patch("geonames.database.execute_query", return_value=50):
        result = await get_country_count(mock_engine)
        assert result == 50


@pytest.mark.asyncio
async def test_get_top_countries(mock_engine):
    mock_result = [("US", 100), ("CA", 50), ("UK", 25)]
    with patch("geonames.database.execute_query", return_value=mock_result):
        result = await get_top_countries(mock_engine)
        assert len(result) == 3
        assert result[0] == ("US", 100)
