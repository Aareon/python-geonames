import os
from datetime import datetime, timezone

from typing import Any, Callable, Coroutine, Dict, List, Protocol, Tuple, TypeVar
from loguru import logger
import inspect
from sqlalchemy import and_, func, insert, select, text
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from geonames.config import Config
from geonames.data_processing import load_data_in_chunks, process_chunk
from geonames.models import Base, Geoname
from geonames.utils import check_for_updates, download_zip, extract_zip

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


class QueryProtocol(Protocol[T_co]):
    async def __call__(self, session: AsyncSession, *args: Any) -> T_co: ...


def _format_detailed_result(geoname: Geoname) -> Dict[str, Any]:
    """
    Format a Geoname object into a detailed result dictionary.
    
    Args:
        geoname: The Geoname object to format
        
    Returns:
        Dictionary with all available location fields using standardized key names
    """
    return {
        "name": geoname.place_name,  # Match _format_search_result
        "postal_code": geoname.postal_code,
        "country": geoname.country_code,  # Match _format_search_result
        "state": geoname.admin_name1,
        "state_code": geoname.admin_code1,
        "province": geoname.admin_name2,
        "province_code": geoname.admin_code2,
        "community": geoname.admin_name3,
        "community_code": geoname.admin_code3,
        "latitude": geoname.latitude,
        "longitude": geoname.longitude,
        "accuracy": geoname.accuracy,
    }


def _format_search_result(geoname: Geoname) -> Dict[str, Any]:
    """
    Format a Geoname object into a standardized search result dictionary.
    
    Args:
        geoname: The Geoname object to format
        
    Returns:
        Dictionary with standardized location fields
    """
    return {
        "name": geoname.place_name,  # Standardize on "name" for place name
        "country": geoname.country_code,  # Standardize on "country" for country code
        "latitude": geoname.latitude,
        "longitude": geoname.longitude,
    }


async def database_exists(engine: AsyncEngine) -> bool:
    """
    Check if the database exists and is populated.

    Args:
        engine (AsyncEngine): The SQLAlchemy async engine.

    Returns:
        bool: True if the database exists and is populated, False otherwise.

    Raises:
        SQLAlchemyError: If there's an error checking the database existence.
    """
    try:
        async with engine.connect() as conn:
            return await conn.run_sync(
                lambda sync_conn: sa_inspect(sync_conn).has_table("geonames")
            )
    except SQLAlchemyError as e:
        logger.error(f"Error checking database existence: {e}")
        raise


async def create_async_session(engine: AsyncEngine) -> AsyncSession:
    async_session = async_sessionmaker(engine, expire_on_commit=False)
    return async_session()


async def execute_query(
    engine: AsyncEngine,
    query_func: Callable[[AsyncSession, Any], Coroutine[Any, Any, T]],
    *args: Any,
) -> T:
    """
    Execute a database query with the given function and arguments.

    Args:
        engine: The database engine
        query_func: The query function to execute
        *args: Arguments to pass to the query function

    Returns:
        Query results
    """
    async with await create_async_session(engine) as session:
        try:
            # Check if `query_func` expects two parameters: session and a tuple of args
            sig = inspect.signature(query_func)
            param_count = len(sig.parameters)

            # Pass as tuple if only two parameters (session, args)
            if param_count == 2:
                logger.debug(f"Executing query function with args as tuple: {args}")
                result = await query_func(session, args)
            else:
                # Otherwise, unpack `args` for query functions expecting separate parameters
                logger.debug(f"Executing query function with unpacked args: {args}")
                result = await query_func(session, *args)

            logger.debug(f"Query execution completed. Result type: {type(result)}")
            return result
        except Exception as e:
            logger.error(f"Error in execute_query: {str(e)}", exc_info=True)
            raise


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
    Bulk insert data into the database.

    Args:
        engine: The database engine
        data: List of dictionaries containing the data to insert
    """
    async with engine.begin() as conn:
        try:
            logger.info(f"Inserting {len(data)} items")
            await conn.execute(insert(Geoname), data)
            logger.info("Insert completed successfully")
        except Exception as e:
            logger.error(f"Error during insert: {e}")
            raise


async def optimize_database(engine: AsyncEngine) -> None:
    """
    Optimize the database after bulk insertion.

    Args:
        engine (AsyncEngine): The async SQLAlchemy engine.
    """
    async with engine.begin() as conn:
        await conn.execute(text("PRAGMA optimize"))


async def get_geolocation(
    engine: AsyncEngine, 
    country: str, 
    zipcode: str
) -> List[Dict[str, Any]]:
    """Get geolocation data for a postal code."""
    async def query(session: AsyncSession, args: Tuple[str, str]) -> List[Geoname]:
        c, z = args
        result = await session.execute(
            select(Geoname).where(
                and_(Geoname.postal_code == z, Geoname.country_code == c)
            )
        )
        return list(result.scalars().all())

    try:
        geonames: List[Geoname] = await execute_query(
            engine, query, (country.strip().upper(), zipcode.strip())
        )
        results = [_format_detailed_result(geoname) for geoname in geonames]
        logger.debug(f"Processed {len(results)} results")
        return results
    except Exception as e:
        logger.error(f"Error in get_geolocation: {e}", exc_info=True)
        return []


async def check_database_update_needed(config: Config) -> bool:
    """
    Check if the database needs to be updated.

    Args:
        config: Configuration object containing database settings

    Returns:
        bool: True if update is needed, False otherwise
    """
    logger.debug(
        f"Checking if database update is needed for {config.DATABASE_FILEPATH}"
    )

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{config.DATABASE_FILEPATH}", echo=False
    )

    try:
        if not await database_exists(engine):
            logger.debug("Database does not exist. Update needed.")
            return True

        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT COUNT(*) FROM geonames"))
            count = result.scalar_one()
            if count == 0:
                logger.debug("Database is empty. Update needed.")
                return True

        if not config.ZIP_FILE.exists():
            logger.debug("Zip file does not exist. Update needed.")
            return True

        db_modification_time = datetime.fromtimestamp(
            os.path.getmtime(config.DATABASE_FILEPATH), tz=timezone.utc
        )
        zip_modification_time = datetime.fromtimestamp(
            os.path.getmtime(config.ZIP_FILE), tz=timezone.utc
        )

        logger.debug(f"Database last modified: {db_modification_time}")
        logger.debug(f"Zip file last modified: {zip_modification_time}")

        if zip_modification_time > db_modification_time:
            logger.debug("Zip file is newer than database. Update needed.")
            return True

        update_needed = await check_for_updates(config.URL, config.ZIP_FILE)
        if update_needed:
            logger.debug("Remote update available. Update needed.")
        else:
            logger.debug("Database is up to date.")
        return update_needed

    finally:
        await engine.dispose()


async def setup_database(config: Config) -> AsyncEngine:
    """
    Set up the database with initial data if needed.

    Args:
        config: Configuration object containing database settings

    Returns:
        AsyncEngine: The configured database engine
    """
    logger.debug(f"Setting up database at {config.DATABASE_FILEPATH}")
    config.SAVE_DIR.mkdir(parents=True, exist_ok=True)

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{config.DATABASE_FILEPATH}", echo=False
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    update_needed = await check_database_update_needed(config)
    if update_needed or await get_total_entries(engine) == 0:
        logger.debug("Database update needed or empty. Processing data...")
        if await check_for_updates(config.URL, config.ZIP_FILE):
            await download_zip(config.URL, config.ZIP_FILE)
            await extract_zip(config.ZIP_FILE, config.SAVE_DIR)

        logger.debug(f"Loading data from {config.TXT_FILE}")
        df_chunks = load_data_in_chunks(config.TXT_FILE, config.CHUNK_SIZE)
        total_processed = 0
        for chunk in df_chunks:
            logger.debug(f"Processing chunk of size {len(chunk)}")
            data = process_chunk(chunk)
            if data:
                await bulk_insert_data(engine, data)
                total_processed += len(data)
            logger.debug(f"Processed {total_processed} records so far")

        await optimize_database(engine)
        logger.info(f"Total records imported: {total_processed}")
    else:
        logger.debug("Database is up to date")

    return engine


async def search_locations(
    engine: AsyncEngine,
    query_func: QueryProtocol[List[Geoname]],
    *args: Any
) -> List[Dict[str, Any]]:
    """Search locations using the provided query function."""
    try:
        logger.debug(f"Executing search_locations with args: {args}")
        geonames: List[Geoname] = await execute_query(engine, query_func, *args)
        logger.debug(f"Found {len(geonames)} results from search query")
        
        return [_format_search_result(geoname) for geoname in geonames]
    except Exception as e:
        logger.error(f"Error in search_locations: {e}")
        return []


async def search_by_name(engine: AsyncEngine, name: str) -> List[Dict[str, Any]]:
    """
    Search locations by name.

    Args:
        engine: The database engine
        name: Name to search for (case-insensitive partial match)

    Returns:
        List of dictionaries containing location information
    """

    async def query(session: AsyncSession, n: str) -> List[Geoname]:
        result = await session.execute(
            select(Geoname).where(Geoname.place_name.ilike(f"%{n}%"))
        )
        return list(result.scalars().all())

    return await search_locations(engine, query, name)


async def search_by_postal_code(
    engine: AsyncEngine, country_code: str, postal_code: str
) -> List[Dict[str, Any]]:
    """
    Search locations by postal code and country code.

    Args:
        engine: The database engine
        country_code: Country code (e.g., 'US', 'GB')
        postal_code: Postal code to search for

    Returns:
        List of dictionaries containing location information
    """
    logger.debug(
        f"Searching for postal code '{postal_code}' in country '{country_code}'"
    )

    async def query(session: AsyncSession, country: str, postal: str) -> List[Geoname]:
        logger.debug(
            f"Executing query with country='{country}', postal_code='{postal}'"
        )

        stmt = (
            select(Geoname)
            .where(Geoname.country_code == country)
            .where(Geoname.postal_code == postal)
        )

        logger.debug(f"SQL Query: {stmt}")

        result = await session.execute(stmt)
        locations = list(result.scalars().all())
        logger.debug(f"Query returned {len(locations)} results")
        return locations

    try:
        return await search_locations(
            engine, query, country_code.strip().upper(), postal_code.strip()
        )
    except Exception as e:
        logger.error(f"Error in search_by_postal_code: {str(e)}", exc_info=True)
        return []


async def search_by_country_code(
    engine: AsyncEngine, country_code: str
) -> List[Dict[str, Any]]:
    """
    Search locations by country code.

    Args:
        engine: The database engine
        country_code: Country code to search for

    Returns:
        List of dictionaries containing location information
    """

    async def query(session: AsyncSession, cc: str) -> List[Geoname]:
        result = await session.execute(
            select(Geoname).where(Geoname.country_code == cc).limit(100)
        )
        return list(result.scalars().all())

    return await search_locations(engine, query, country_code.strip().upper())


async def search_by_coordinates(
    engine: AsyncEngine,  # Accept AsyncEngine instead of AsyncSession for consistency
    lat: float, 
    lon: float, 
    radius: float, 
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Search for locations within a radius of given coordinates.
    
    Args:
        engine: Database engine
        lat: Latitude
        lon: Longitude
        radius: Search radius in km (must be positive)
        limit: Maximum number of results to return
        
    Returns:
        List of matching locations
    """
    try:
        if radius <= 0:
            raise ValueError("Radius must be positive")
        
        async def query(session: AsyncSession, args: Tuple[float, float, float, int]) -> List[Geoname]:
            lat, lon, radius, limit = args
            result = await session.execute(
                select(Geoname)
                .where(
                    and_(
                        Geoname.latitude.between(lat - radius / 111, lat + radius / 111),
                        Geoname.longitude.between(lon - radius / 111, lon + radius / 111),
                    )
                )
                .order_by(
                    func.abs(Geoname.latitude - lat) + func.abs(Geoname.longitude - lon)
                )
                .limit(limit)
            )
            return list(result.scalars().all())
        
        # Execute the query with provided arguments
        return await search_locations(engine, query, lat, lon, radius, limit)

    except ValueError as e:
        logger.error(f"Invalid input: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error in search_by_coordinates: {e}")
        return []


async def debug_database_content(
    engine: AsyncEngine, country_code: str, postal_code: str
) -> None:
    """
    Debug helper to check database content for troubleshooting.

    Args:
        engine: The database engine
        country_code: Country code to check
        postal_code: Postal code to check
    """
    async with await create_async_session(engine) as session:
        try:
            # Check for any content with given country code
            country_stmt = (
                select(Geoname)
                .where(Geoname.country_code == country_code.strip().upper())
                .limit(5)
            )
            country_result = await session.execute(country_stmt)
            country_samples = list(country_result.scalars().all())
            logger.debug(
                f"Sample entries for country {country_code}: {country_samples}"
            )

            # Check for any content with similar postal code
            postal_stmt = (
                select(Geoname)
                .where(Geoname.postal_code.like(f"%{postal_code.strip()}%"))
                .limit(5)
            )
            postal_result = await session.execute(postal_stmt)
            postal_samples = list(postal_result.scalars().all())
            logger.debug(
                f"Sample entries with similar postal code {postal_code}: {postal_samples}"
            )

            # Get total counts
            count_stmt = select(func.count()).select_from(Geoname)
            count_result = await session.execute(count_stmt)
            total_count = count_result.scalar()
            logger.debug(f"Total records in database: {total_count}")

            # Get counts for specific country
            country_count_stmt = (
                select(func.count())
                .select_from(Geoname)
                .where(Geoname.country_code == country_code.strip().upper())
            )
            country_count_result = await session.execute(country_count_stmt)
            country_count = country_count_result.scalar()
            logger.debug(f"Total records for country {country_code}: {country_count}")

        except Exception as e:
            logger.error(f"Error in debug_database_content: {str(e)}", exc_info=True)


async def get_total_entries(engine: AsyncEngine) -> int:
    """
    Get the total number of entries in the database.

    Args:
        engine: The database engine

    Returns:
        int: Total number of entries
    """

    async def query(session: AsyncSession, _: Any) -> int:
        result = await session.execute(select(func.count()).select_from(Geoname))
        return result.scalar_one()

    return await execute_query(engine, query, None)


async def get_country_count(engine: AsyncEngine) -> int:
    """
    Get the number of unique countries in the database.

    Args:
        engine: The database engine

    Returns:
        int: Number of unique countries
    """

    async def query(session: AsyncSession, _: Any) -> int:
        result = await session.execute(
            select(func.count(Geoname.country_code.distinct()))
        )
        return int(result.scalar_one())

    return await execute_query(engine, query, None)


async def get_top_countries(
    engine: AsyncEngine, limit: int = 5
) -> List[Tuple[str, int]]:
    """
    Get the top countries by number of locations.

    Args:
        engine: The database engine
        limit: Maximum number of countries to return (default: 5)

    Returns:
        List of tuples containing (country_code, location_count)
    """

    async def query(session: AsyncSession, l: Any) -> List[Tuple[str, int]]:
        result = await session.execute(
            select(Geoname.country_code, func.count(Geoname.country_code))
            .group_by(Geoname.country_code)
            .order_by(func.count(Geoname.country_code).desc())
            .limit(l)
        )
        return [(str(row[0]), int(row[1])) for row in result.all()]

    return await execute_query(engine, query, limit)
