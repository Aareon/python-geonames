from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text, and_, func, select
from typing import List, Dict, Any, Tuple, Callable
from loguru import logger
from geonames.models import Base, Geoname
from geonames.config import Config
from geonames.utils import check_for_updates, download_zip, extract_zip
from geonames.data_processing import load_data_in_chunks, process_chunk
import os

async def database_exists(engine: AsyncEngine) -> bool:
    """
    Check if the database exists and is populated.

    Args:
        engine (AsyncEngine): The SQLAlchemy async engine.

    Returns:
        bool: True if the database exists and is populated, False otherwise.
    """
    try:
        async with engine.connect() as conn:
            # Use text() to create a SQL expression
            result = await conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='geonames'"))
            return result.scalar() is not None
    except SQLAlchemyError as e:
        logger.error(f"Error checking database existence: {e}")
        return False

async def create_async_session(engine: AsyncEngine) -> AsyncSession:
    """
    Create and return an async session for the given engine.

    Args:
        engine (AsyncEngine): The SQLAlchemy async engine.

    Returns:
        AsyncSession: An async SQLAlchemy session.
    """
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    return async_session()

async def execute_query(engine: AsyncEngine, query: Callable, *args, **kwargs) -> Any:
    """
    Execute a query using the provided query function and arguments.

    Args:
        engine (AsyncEngine): The SQLAlchemy async engine.
        query (Callable): The query function to execute.
        *args: Positional arguments for the query function.
        **kwargs: Keyword arguments for the query function.

    Returns:
        Any: The result of the query execution.

    Raises:
        SQLAlchemyError: If there's an error executing the query.
    """
    async with await create_async_session(engine) as session:
        try:
            result = await query(session, *args, **kwargs)
            return result
        except SQLAlchemyError as e:
            logger.error(f"Error executing query: {e}")
            raise SQLAlchemyError(f"Database query failed: {str(e)}")

async def create_database(engine: AsyncEngine) -> None:
    """
    Create the database tables.

    Args:
        engine (AsyncEngine): The SQLAlchemy async engine.
    """
    logger.info("Creating database tables")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def bulk_insert_data(engine: AsyncEngine, data: List[Dict[str, Any]]) -> None:
    """
    Perform a bulk insert of data into the database.

    Args:
        engine (AsyncEngine): The SQLAlchemy async engine.
        data (List[Dict[str, Any]]): A list of dictionaries containing the data to insert.
    """
    async with await create_async_session(engine) as session:
        try:
            await session.execute(Geoname.__table__.insert(), data)
            await session.commit()
        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            await session.rollback()
            raise

async def optimize_database(engine: AsyncEngine) -> None:
    """
    Optimize the database after bulk insertion.

    Args:
        engine (AsyncEngine): The async SQLAlchemy engine.
    """
    async with engine.begin() as conn:
        await conn.execute(text("PRAGMA optimize"))

async def get_geolocation(engine: AsyncEngine, country: str, zipcode: str) -> List[Dict[str, Any]]:
    """
    Fetch geolocation data for a given country and zipcode.

    Args:
        engine (AsyncEngine): The SQLAlchemy async engine.
        country (str): The country code.
        zipcode (str): The postal code.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing geolocation data.
    """
    async def query(session, country, zipcode):
        result = await session.execute(
            select(Geoname).where(
                and_(
                    Geoname.postal_code == zipcode,
                    Geoname.country_code == country
                )
            )
        )
        return result.scalars().all()

    geonames = await execute_query(engine, query, country, zipcode)
    return [
        {
            "latitude": geoname.latitude,
            "longitude": geoname.longitude,
            "city": geoname.place_name,
            "state": geoname.admin_name1,
            "country_code": geoname.country_code,
            "state_code": geoname.admin_code1,
            "province": geoname.admin_name2,
            "province_code": geoname.admin_code2,
        }
        for geoname in geonames
    ]

async def check_database_update_needed(config: Config) -> bool:
    """
    Check if the database needs to be updated based on the downloaded zip file.

    Args:
        config (Config): The configuration object.

    Returns:
        bool: True if an update is needed, False otherwise.
    """
    engine = create_async_engine(f"sqlite+aiosqlite:///{config.DATABASE_FILEPATH}", echo=False)
    
    if not await database_exists(engine):
        await engine.dispose()
        return True

    if not config.ZIP_FILE.exists():
        await engine.dispose()
        return True

    db_modification_time = os.path.getmtime(config.DATABASE_FILEPATH)
    zip_modification_time = os.path.getmtime(config.ZIP_FILE)

    if zip_modification_time > db_modification_time:
        await engine.dispose()
        return True

    update_needed = await check_for_updates(config.URL, config.ZIP_FILE)
    await engine.dispose()
    return update_needed

async def setup_database() -> AsyncEngine:
    """
    Set up the database, downloading and processing data if necessary.

    Returns:
        AsyncEngine: An async SQLAlchemy engine for the set up database.
    """
    config = Config()
    
    # Create the SAVE_DIR if it doesn't exist
    config.SAVE_DIR.mkdir(parents=True, exist_ok=True)
    
    engine = create_async_engine(f"sqlite+aiosqlite:///{config.DATABASE_FILEPATH}", echo=False)

    if await check_database_update_needed(config):
        if await check_for_updates(config.URL, config.ZIP_FILE):
            await download_zip(config.URL, config.ZIP_FILE)
            extract_zip(config.ZIP_FILE, config.SAVE_DIR)

        await create_database(engine)
    
        df_chunks = load_data_in_chunks(config.TXT_FILE, config.CHUNK_SIZE)
        for chunk in df_chunks:
            data = process_chunk(chunk)
            await bulk_insert_data(engine, data)

        await optimize_database(engine)
    else:
        logger.info("Database is up to date")

    return engine

async def search_locations(engine: AsyncEngine, query_func: Callable, *args) -> List[Dict[str, Any]]:
    geonames = await execute_query(engine, query_func, *args)
    return [
        {
            "name": geoname.place_name,
            "country": geoname.country_code,
            "latitude": geoname.latitude,
            "longitude": geoname.longitude
        }
        for geoname in geonames
    ]

async def search_by_name(engine: AsyncEngine, name: str) -> List[Dict[str, Any]]:
    async def query(session, name):
        result = await session.execute(
            select(Geoname).where(Geoname.place_name.ilike(f"%{name}%"))
        )
        return result.scalars().all()

    return await search_locations(engine, query, name)

async def search_by_postal_code(db_file: str, country_code: str, postal_code: str) -> List[Dict[str, Any]]:
    """
    Search for locations in the database by postal code and country code.

    Args:
        db_file (str): The path to the SQLite database file.
        country_code (str): The country code to search for.
        postal_code (str): The postal code to search for.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing location data.
    """
    async def query(session, country_code, postal_code):
        result = await session.execute(
            select(Geoname).where(
                and_(
                    Geoname.country_code == country_code,
                    Geoname.postal_code == postal_code
                )
            )
        )
        return result.scalars().all()

    return await search_locations(db_file, query, country_code, postal_code)

async def search_by_country_code(db_file: str, country_code: str) -> List[Dict[str, Any]]:
    """
    Search for locations in the database by country code.

    Args:
        db_file (str): The path to the SQLite database file.
        country_code (str): The country code to search for.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing location data.
    """
    async def query(session, country_code):
        result = await session.execute(
            select(Geoname).where(Geoname.country_code == country_code).limit(100)
        )
        return result.scalars().all()

    return await search_locations(db_file, query, country_code)

async def search_by_coordinates(db_file: str, lat: float, lon: float, radius: float) -> List[Dict[str, Any]]:
    """
    Search for locations in the database by coordinates within a given radius.

    Args:
        db_file (str): The path to the SQLite database file.
        lat (float): The latitude of the center point.
        lon (float): The longitude of the center point.
        radius (float): The search radius in kilometers.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing location data.
    """
    async def query(session, lat, lon, radius):
        result = await session.execute(
            select(Geoname).where(
                and_(
                    Geoname.latitude.between(lat - radius/111, lat + radius/111),
                    Geoname.longitude.between(lon - radius/111, lon + radius/111)
                )
            ).order_by(
                func.abs(Geoname.latitude - lat) + func.abs(Geoname.longitude - lon)
            ).limit(100)
        )
        return result.scalars().all()

    return await search_locations(db_file, query, lat, lon, radius)

async def get_total_entries(engine: AsyncEngine) -> int:
    async def query(session):
        result = await session.execute(select(func.count()).select_from(Geoname))
        return result.scalar_one()

    return await execute_query(engine, query)

async def get_country_count(engine: AsyncEngine) -> int:
    async def query(session):
        result = await session.execute(select(func.count(Geoname.country_code.distinct())))
        return result.scalar_one()

    return await execute_query(engine, query)

async def get_top_countries(engine: AsyncEngine, limit: int = 5) -> List[Tuple[str, int]]:
    async def query(session, limit):
        result = await session.execute(
            select(Geoname.country_code, func.count(Geoname.country_code))
            .group_by(Geoname.country_code)
            .order_by(func.count(Geoname.country_code).desc())
            .limit(limit)
        )
        return result.all()

    return await execute_query(engine, query, limit)