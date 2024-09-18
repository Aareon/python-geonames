import asyncio

from loguru import logger

from geonames.database import get_geolocation, setup_database


async def main():
    engine = await setup_database()

    # Example usage
    results = await get_geolocation(engine, "US", "39046")
    logger.info(f"Geolocation results: {results}")


if __name__ == "__main__":
    asyncio.run(main())
