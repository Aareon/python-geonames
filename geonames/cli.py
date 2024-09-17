import click
import asyncio
from geonames import database

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
        sync_wrapper(database.setup_database)()
        click.echo("Data import completed successfully.")
    except Exception as e:
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
    click.echo(f"Searching in {db_file}")
    try:
        results = []
        if name:
            results = sync_wrapper(database.search_by_name)(db_file, name)
        elif postal_code and country_code:
            results = sync_wrapper(database.search_by_postal_code)(db_file, country_code, postal_code)
        elif country_code:
            results = sync_wrapper(database.search_by_country_code)(db_file, country_code)
        elif lat is not None and lon is not None:
            results = sync_wrapper(database.search_by_coordinates)(db_file, lat, lon, radius)
        else:
            click.echo("Please provide a search criteria: --name, --postal-code and --country-code, --country-code, or --lat and --lon")
            return

        if results:
            for result in results:
                click.echo(f"Found: {result}")
        else:
            click.echo("No results found")
    except Exception as e:
        click.echo(f"Error during search: {str(e)}")

@cli.command()
@click.option('--db-file', default='data/geonames.db', help='Path to the SQLite database file')
def stats(db_file):
    """Display statistics about the database."""
    click.echo(f"Displaying statistics for {db_file}")
    try:
        total_entries = sync_wrapper(database.get_total_entries)(db_file)
        click.echo(f"Total entries: {total_entries}")
        
        country_count = sync_wrapper(database.get_country_count)(db_file)
        click.echo(f"Number of countries: {country_count}")
        
        top_5_countries = sync_wrapper(database.get_top_countries)(db_file, limit=5)
        click.echo("Top 5 countries by number of entries:")
        for country, count in top_5_countries:
            click.echo(f"  {country}: {count}")
    except Exception as e:
        click.echo(f"Error retrieving statistics: {str(e)}")


if __name__ == '__main__':
    cli()