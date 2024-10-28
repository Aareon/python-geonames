import asyncio

from loguru import logger
from sqlalchemy.exc import SQLAlchemyError

from geonames.config import Config as GeoNamesConfig
from geonames.database import get_geolocation, setup_database


async def safe_get_geolocation(engine, country, postal_code):
    try:
        return await get_geolocation(engine, country, postal_code)
    except SQLAlchemyError as e:
        logger.error(f"Database error occurred: {e}")
        return None


async def main():
    config = GeoNamesConfig()
    try:
        engine = await setup_database(config)
        result = await safe_get_geolocation(engine, "US", "39046")
        if result:
            logger.info(f"Geolocation results: {result}")
        else:
            logger.warning("No results found or an error occurred.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    asyncio.run(main())
