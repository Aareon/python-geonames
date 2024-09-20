import os
from typing import List, Iterator, Tuple, Any, TypeVar, Callable, Dict, Iterable
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine, AsyncResult
from sqlalchemy import and_, func, inspect, select, text, insert
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timezone
from loguru import logger

from geonames.config import Config
from geonames.models import Base, Geoname
from geonames.data_processing import load_data_in_chunks, process_chunk
from geonames.utils import check_for_updates, download_zip, extract_zip

T = TypeVar('T')


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
                lambda sync_conn: inspect(sync_conn).has_table("geonames")
            )
    except SQLAlchemyError as e:
        logger.error(f"Error checking database existence: {e}")
        raise


async def create_async_session(engine: AsyncEngine) -> AsyncSession:
    async_session = async_sessionmaker(engine, expire_on_commit=False)
    return async_session()


async def execute_query(engine, query_func, *args, **kwargs):
    async with await create_async_session(engine) as session:
        try:
            logger.debug("Starting query execution")
            result = await query_func(session, *args, **kwargs)
            if hasattr(result, 'scalars'):
                scalars_result = await result.scalars()
                return [item async for item in scalars_result]
            if hasattr(result, 'all'):
                all_result = await result.all()
                return all_result
            if isinstance(result, list):
                return result
            return [result] if result is not None else []
        except Exception as e:
            logger.error(f"Error in execute_query: {e}")
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


async def get_geolocation_query(session: AsyncSession, country: str, zipcode: str) -> Iterator[Geoname]:
    result = await session.execute(
        select(Geoname).where(
            and_(Geoname.postal_code == zipcode, Geoname.country_code == country)
        )
    )
    return iter(result.scalars().all())


async def get_geolocation(engine: AsyncEngine, country: str, zipcode: str) -> List[Dict[str, Any]]:
    geonames = await execute_query(engine, get_geolocation_query, country, zipcode)
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
    logger.debug(f"Checking if database update is needed for {config.DATABASE_FILEPATH}")
    
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

        db_modification_time = datetime.fromtimestamp(os.path.getmtime(config.DATABASE_FILEPATH), tz=timezone.utc)
        zip_modification_time = datetime.fromtimestamp(os.path.getmtime(config.ZIP_FILE), tz=timezone.utc)

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
    engine: AsyncEngine, query_func: Callable[..., Any], *args: Any
) -> List[Dict[str, Any]]:
    geonames = await execute_query(engine, query_func, *args)
    return [
        {
            "name": geoname.place_name,
            "country": geoname.country_code,
            "latitude": geoname.latitude,
            "longitude": geoname.longitude,
        }
        for geoname in geonames
    ]


async def search_by_name(engine: AsyncEngine, name: str) -> List[Dict[str, Any]]:
    async def query(session: AsyncSession, name: str) -> List[Geoname]:
        result = await session.execute(
            select(Geoname).where(Geoname.place_name.ilike(f"%{name}%"))
        )
        return list(result.scalars().all())

    geonames = await execute_query(engine, query, name)
    return [
        {
            "name": geoname.place_name,
            "country": geoname.country_code,
            "latitude": geoname.latitude,
            "longitude": geoname.longitude,
        }
        for geoname in geonames
    ]


async def search_by_postal_code_query(session: AsyncSession, country_code: str, postal_code: str):
    result = await session.execute(
        select(Geoname).where(
            and_(
                Geoname.country_code == country_code,
                Geoname.postal_code == postal_code
            )
        )
    )
    return result.scalars().all()  # Return the result directly

async def search_by_postal_code(
    engine: AsyncEngine, country_code: str, postal_code: str
) -> List[Dict[str, Any]]:
    logger.debug(f"Searching for postal code {postal_code} in country {country_code}")
    geonames = await execute_query(engine, search_by_postal_code_query, country_code, postal_code)
    return [
        {
            "name": geoname.place_name,
            "country": geoname.country_code,
            "latitude": geoname.latitude,
            "longitude": geoname.longitude,
        }
        for geoname in geonames
    ]


async def search_by_country_code(
    engine: AsyncEngine, country_code: str
) -> List[Dict[str, Any]]:
    async def query(session: AsyncSession, country_code: str) -> List[Geoname]:
        result = await session.execute(
            select(Geoname).where(Geoname.country_code == country_code).limit(100)
        )
        return list(result.scalars().all())

    return await search_locations(engine, query, country_code)


async def search_by_coordinates(
    engine: AsyncEngine, lat: float, lon: float, radius: float
) -> List[Dict[str, Any]]:
    async def query(session: AsyncSession, lat: float, lon: float, radius: float) -> List[Geoname]:
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
            .limit(100)
        )
        return list(result.scalars().all())  # Convert to list explicitly

    return await search_locations(engine, query, lat, lon, radius)


async def get_total_entries_query(result: AsyncResult) -> int:
    async for row in result:
        return row[0]
    return 0

async def get_total_entries(engine: AsyncEngine) -> int:
    query = select(func.count()).select_from(Geoname)
    return await execute_query(engine, query, get_total_entries_query)


async def get_country_count_query(session: AsyncSession) -> int:
    result = await session.execute(
        select(func.count(Geoname.country_code.distinct()))
    )
    return result.scalar_one()

async def get_country_count(engine: AsyncEngine) -> int:
    return await execute_query(engine, get_country_count_query)

async def get_top_countries_query(session: AsyncSession, limit: int) -> List[Tuple[str, int]]:
    result = await session.execute(
        select(Geoname.country_code, func.count(Geoname.country_code))
        .group_by(Geoname.country_code)
        .order_by(func.count(Geoname.country_code).desc())
        .limit(limit)
    )
    return [(row[0], row[1]) for row in result.all()]

async def get_top_countries(engine: AsyncEngine, limit: int = 5) -> List[Tuple[str, int]]:
    return await execute_query(engine, get_top_countries_query, limit)
