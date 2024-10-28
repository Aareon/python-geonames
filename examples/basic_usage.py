import asyncio
from pathlib import Path

from loguru import logger

from geonames.config import Config
from geonames.database import get_geolocation, setup_database


async def main():
    # Configure the library
    config = Config()
    config.SAVE_DIR = Path(__file__).parent / "geonames_data"

    # Set up the database
    engine = await setup_database(config)

    # Perform a geolocation query
    results = await get_geolocation(engine, "US", "39046")
    logger.info(f"Geolocation results: {results}")


if __name__ == "__main__":
    asyncio.run(main())
