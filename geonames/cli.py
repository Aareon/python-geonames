import click
import asyncio
import aiohttp
import zipfile
from pathlib import Path
from geonames import database
from geonames.config import Config
from geonames.utils import download_zip, extract_zip, check_for_updates
from sqlalchemy.ext.asyncio import create_async_engine
from loguru import logger
import os

def sync_wrapper(func):
    def wrapper(*args, **kwargs):
        if asyncio.iscoroutinefunction(func):
            return asyncio.run(func(*args, **kwargs))
        return func(*args, **kwargs)
    return wrapper

@click.group()
def cli():
    """GeoNames CLI for managing and querying geographical data."""
    pass

@cli.command()
@click.option('--input-file', default='data/allCountries.txt', help='Path to the input file')
@click.option('--db-file', default='data/geonames.db', help='Path to the SQLite database file')
def import_data(input_file, db_file):
    """Import data from the input file into the SQLite database."""
    click.echo(f"Importing data from {input_file} into {db_file}")
    try:
        config = Config()
        config.TXT_FILE = Path(input_file).resolve()
        config.DATABASE_FILEPATH = Path(db_file).resolve()
        config.ZIP_FILE = config.TXT_FILE.with_suffix('.zip')

        async def import_data_async():
            if not config.TXT_FILE.exists():
                click.echo("Input file not found. Attempting to download...")
                try:
                    if await check_for_updates(config.URL, config.ZIP_FILE):
                        await download_zip(config.URL, config.ZIP_FILE)
                    
                    click.echo(f"Extracting {config.ZIP_FILE} to {config.TXT_FILE.parent}")
                    extracted_files = await extract_zip(config.ZIP_FILE, config.TXT_FILE.parent)
                    
                    if not extracted_files:
                        raise FileNotFoundError(f"No files were extracted from {config.ZIP_FILE}")
                    
                    # Find the first .txt file in the extracted files
                    txt_file = next((f for f in extracted_files if f.endswith('.txt')), None)
                    if txt_file:
                        config.TXT_FILE = config.TXT_FILE.parent / txt_file
                        click.echo(f"Using extracted file: {config.TXT_FILE}")
                    else:
                        raise FileNotFoundError(f"No .txt file found in extracted files. Extracted files: {extracted_files}")
                
                except aiohttp.ClientError as e:
                    raise Exception(f"Failed to download the file: {str(e)}")
                except zipfile.BadZipFile:
                    raise Exception(f"The downloaded file {config.ZIP_FILE} is not a valid zip file.")
                except Exception as e:
                    raise Exception(f"Error during download or extraction: {str(e)}")

            click.echo("Setting up database...")
            engine = await database.setup_database()
            
            total_entries = await database.get_total_entries(engine)
            country_count = await database.get_country_count(engine)
            
            click.echo("Data import completed successfully.")
            click.echo(f"Total entries in database: {total_entries}")
            click.echo(f"Number of countries: {country_count}")
            
            await engine.dispose()

        asyncio.run(import_data_async())
    except FileNotFoundError as e:
        click.echo(f"Error: {str(e)}")
        click.echo(f"Current working directory: {Path.cwd()}")
        click.echo(f"ZIP file exists: {config.ZIP_FILE.exists()}")
        click.echo(f"ZIP file size: {config.ZIP_FILE.stat().st_size if config.ZIP_FILE.exists() else 'N/A'}")
        click.echo(f"Extract directory contents: {os.listdir(config.TXT_FILE.parent)}")
        click.echo("Please ensure the input file exists at the specified path or check your internet connection.")
    except PermissionError as e:
        click.echo(f"Error: Permission denied. {str(e)}")
    except Exception as e:
        logger.exception("Error during data import")
        click.echo(f"Error during data import: {str(e)}")


@cli.command()
@click.option('--db-file', default='data/geonames.db', help='Path to the SQLite database file')
@click.option('--name', help='Name of the location to search for')
@click.option('--postal-code', help='Postal code to search for')
@click.option('--country-code', help='Country code to search for')
@click.option('--lat', type=float, help='Latitude for coordinate search')
@click.option('--lon', type=float, help='Longitude for coordinate search')
@click.option('--radius', type=float, default=10.0, help='Search radius in km for coordinate search')
def search(db_file, name, postal_code, country_code, lat, lon, radius):
    """Search for locations in the database."""
    db_path = Path(db_file)
    
    if not db_path.exists():
        click.echo(f"Database file not found at {db_path}. Please run the import-data command first.")
        return

    click.echo(f"Searching in {db_file}")
    try:
        async def search_wrapper():
            engine = create_async_engine(f"sqlite+aiosqlite:///{db_file}", echo=False)
            try:
                if not await database.database_exists(engine):
                    click.echo("Database tables not found. Please run the import-data command first.")
                    return []
                
                if name:
                    return await database.search_by_name(engine, name)
                elif postal_code and country_code:
                    return await database.search_by_postal_code(engine, country_code, postal_code)
                elif country_code:
                    return await database.search_by_country_code(engine, country_code)
                elif lat is not None and lon is not None:
                    return await database.search_by_coordinates(engine, lat, lon, radius)
                else:
                    click.echo("Please provide a search criteria: --name, --postal-code and --country-code, --country-code, or --lat and --lon")
                    return []
            finally:
                await engine.dispose()

        results = sync_wrapper(search_wrapper)()

        if results:
            for result in results:
                click.echo(f"Found: {result}")
        else:
            click.echo("No results found")
    except Exception as e:
        click.echo(f"Error during search: {str(e)}")
        click.echo("If the problem persists, please ensure the database is properly set up and you have the necessary permissions.")

@cli.command()
@click.option('--db-file', default='data/geonames.db', help='Path to the SQLite database file')
def stats(db_file):
    """Display statistics about the database."""
    db_path = Path(db_file)
    
    if not db_path.exists():
        click.echo(f"Database file not found at {db_path}. Please run the import-data command first.")
        return

    click.echo(f"Displaying statistics for {db_file}")
    try:
        async def stats_wrapper():
            engine = create_async_engine(f"sqlite+aiosqlite:///{db_file}", echo=False)
            try:
                if not await database.database_exists(engine):
                    click.echo("Database tables not found. Please run the import-data command first.")
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
            click.echo("Unable to retrieve statistics. Please ensure the database is properly set up.")
    except Exception as e:
        click.echo(f"Error retrieving statistics: {str(e)}")
        click.echo("If the problem persists, please ensure the database is properly set up and you have the necessary permissions.")

if __name__ == '__main__':
    cli()