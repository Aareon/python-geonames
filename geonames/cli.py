import asyncio
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import click
from loguru import logger
from sqlalchemy.ext.asyncio import create_async_engine

from geonames import database
from geonames.config import Config
from geonames.utils import check_for_updates, download_zip, extract_zip


def sync_wrapper(func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if asyncio.iscoroutinefunction(func):
            return asyncio.run(func(*args, **kwargs))
        return func(*args, **kwargs)

    return wrapper


@click.group()
def cli() -> None:
    """GeoNames CLI for managing and querying geographical data."""
    pass


@cli.command()
@click.option(
    "--input-file", default="data/allCountries.txt", help="Path to the input file"
)
@click.option(
    "--db-file", default="data/geonames.db", help="Path to the SQLite database file"
)
@click.option("--debug", is_flag=True, help="Enable debug logging")
def import_data(input_file: str, db_file: str, debug: bool) -> None:
    """Import data from the input file into the SQLite database."""
    if debug:
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")

    click.echo(f"Importing data from {input_file} into {db_file}")
    try:
        config = Config()
        config.TXT_FILE = Path(input_file).resolve()
        config.DATABASE_FILEPATH = Path(db_file).resolve()
        config.ZIP_FILE = config.TXT_FILE.with_suffix(".zip")

        async def import_data_async() -> None:
            if not config.TXT_FILE.exists():
                logger.debug("Input file not found. Attempting to download...")
                try:
                    if await check_for_updates(config.URL, config.ZIP_FILE):
                        await download_zip(config.URL, config.ZIP_FILE)

                    logger.debug(
                        f"Extracting {config.ZIP_FILE} to {config.TXT_FILE.parent}"
                    )
                    extracted_files = await extract_zip(
                        config.ZIP_FILE, config.TXT_FILE.parent
                    )

                    if not extracted_files:
                        raise FileNotFoundError(
                            f"No files were extracted from {config.ZIP_FILE}"
                        )

                    txt_file = next(
                        (f for f in extracted_files if f.endswith(".txt")), None
                    )
                    if txt_file:
                        config.TXT_FILE = config.TXT_FILE.parent / txt_file
                        logger.debug(f"Using extracted file: {config.TXT_FILE}")
                    else:
                        raise FileNotFoundError(
                            f"No .txt file found in extracted files. Extracted files: {extracted_files}"
                        )

                except Exception as e:
                    logger.exception("Error during download or extraction")
                    raise

            logger.debug("Setting up database...")
            engine = await database.setup_database(config)

            total_entries = await database.get_total_entries(engine)
            country_count = await database.get_country_count(engine)

            logger.debug("Data import completed successfully.")
            click.echo("Data import completed successfully.")
            click.echo(f"Total entries in database: {total_entries}")
            click.echo(f"Number of countries: {country_count}")

            await engine.dispose()

        asyncio.run(import_data_async())
    except Exception as e:
        logger.exception("Error during data import")
        click.echo(f"Error during data import: {str(e)}")


@cli.command()
@click.option(
    "--db-file", default="data/geonames.db", help="Path to the SQLite database file"
)
@click.option("--name", help="Name of the location to search for")
@click.option("--postal-code", help="Postal code to search for")
@click.option("--country-code", help="Country code to search for")
@click.option("--lat", type=float, help="Latitude for coordinate search")
@click.option("--lon", type=float, help="Longitude for coordinate search")
@click.option(
    "--radius",
    type=float,
    default=10.0,
    help="Search radius in km for coordinate search",
)
@click.option("--debug", is_flag=True, help="Enable debug logging")
def search(
    db_file: str,
    name: Optional[str],
    postal_code: Optional[str],
    country_code: Optional[str],
    lat: Optional[float],
    lon: Optional[float],
    radius: float,
    debug: bool,
) -> None:
    """Search for locations in the database."""
    if debug:
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")

    db_path = Path(db_file)

    if not db_path.exists():
        click.echo(
            f"Database file not found at {db_path}. Please run the import-data command first."
        )
        return

    click.echo(f"Searching in {db_file}")
    try:

        async def search_wrapper() -> List[Dict[str, Any]]:
            engine = create_async_engine(f"sqlite+aiosqlite:///{db_file}", echo=debug)
            try:
                if not await database.database_exists(engine):
                    click.echo(
                        "Database tables not found. Please run the import-data command first."
                    )
                    return []

                logger.debug("Database exists. Proceeding with search.")

                if name:
                    logger.debug(f"Searching for name: {name}")
                    return await database.search_by_name(engine, name)
                elif postal_code and country_code:
                    logger.debug(
                        f"Searching for postal code {postal_code} in country {country_code}"
                    )
                    result = await database.search_by_postal_code(
                        engine, country_code, postal_code
                    )
                    logger.debug(f"Search result: {result}")
                    return result
                elif country_code:
                    logger.debug(f"Searching for country code: {country_code}")
                    return await database.search_by_country_code(engine, country_code)
                elif lat is not None and lon is not None:
                    logger.debug(
                        f"Searching by coordinates: lat={lat}, lon={lon}, radius={radius}"
                    )
                    return await database.search_by_coordinates(
                        engine, lat, lon, radius
                    )
                else:
                    click.echo(
                        "Please provide a search criteria: --name, --postal-code and --country-code, --country-code, or --lat and --lon"
                    )
                    return []
            finally:
                await engine.dispose()

        results = asyncio.run(search_wrapper())

        if results:
            for result in results:
                click.echo(f"Found: {result}")
        else:
            click.echo("No results found")
    except Exception as e:
        logger.exception("Error during search")
        click.echo(f"Error during search: {str(e)}")
        click.echo(
            "If the problem persists, please ensure the database is properly set up and you have the necessary permissions."
        )


@cli.command()
@click.option(
    "--db-file", default="data/geonames.db", help="Path to the SQLite database file"
)
def stats(db_file: str) -> None:
    """Display statistics about the database."""
    db_path = Path(db_file)

    if not db_path.exists():
        click.echo(
            f"Database file not found at {db_path}. Please run the import-data command first."
        )
        return

    click.echo(f"Displaying statistics for {db_file}")
    try:

        async def stats_wrapper() -> (
            tuple[Optional[int], Optional[int], Optional[list]]
        ):
            engine = create_async_engine(f"sqlite+aiosqlite:///{db_file}", echo=False)
            try:
                if not await database.database_exists(engine):
                    click.echo(
                        "Database tables not found. Please run the import-data command first."
                    )
                    return None, None, None

                total_entries = await database.get_total_entries(engine)
                country_count = await database.get_country_count(engine)
                top_5_countries = await database.get_top_countries(engine, limit=5)
                return total_entries, country_count, top_5_countries
            finally:
                await engine.dispose()

        total_entries, country_count, top_5_countries = sync_wrapper(stats_wrapper)()

        if total_entries is not None:
            click.echo(f"Total entries: {total_entries}")
            click.echo(f"Number of countries: {country_count}")
            click.echo("Top 5 countries by number of entries:")
            for country, count in top_5_countries:
                click.echo(f"  {country}: {count}")
        else:
            click.echo(
                "Unable to retrieve statistics. Please ensure the database is properly set up."
            )
    except Exception as e:
        click.echo(f"Error retrieving statistics: {str(e)}")
        click.echo(
            "If the problem persists, please ensure the database is properly set up and you have the necessary permissions."
        )


if __name__ == "__main__":
    cli()
