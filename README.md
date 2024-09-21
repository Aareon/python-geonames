# python-geonames

[![Python tests](https://github.com/Aareon/python-geonames/actions/workflows/python-tests.yml/badge.svg)](https://github.com/Aareon/python-geonames/actions/workflows/python-tests.yml)
[![codecov](https://codecov.io/gh/Aareon/python-geonames/branch/main/graph/badge.svg)](https://codecov.io/gh/Aareon/python-geonames)
[![Python Version](https://img.shields.io/pypi/pyversions/python-geonames.svg)](https://pypi.org/project/python-geonames/)
[![License](https://img.shields.io/github/license/Aareon/python-geonames.svg)](https://github.com/Aareon/python-geonames/blob/main/LICENSE)

An asynchronous Python library for working with GeoNames data using a database backend.

## Features

- Asynchronous database operations for GeoNames data
- Easy setup and configuration
- Efficient geolocation queries
- Customizable data storage location

## Installation

```bash
pip install python-geonames
```

## Basic Usage

Here's a quick example of how to use the main features of python-geonames:

```python
import asyncio
from pathlib import Path
from loguru import logger

from geonames.database import get_geolocation, setup_database
from geonames.config import Config as GeoNamesConfig

async def main():
    # Configure the library
    config = GeoNamesConfig()
    config.SAVE_DIR = Path(__file__).parent / "geonames_data"
    
    # Set up the database
    engine = await setup_database(config)

    # Perform a geolocation query
    results = await get_geolocation(engine, "US", "39046")
    logger.info(f"Geolocation results: {results}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Advanced Usage

### Custom Configuration

You can customize the configuration by modifying the `GeoNamesConfig` object:

```python
from geonames.config import Config as GeoNamesConfig

config = GeoNamesConfig()
config.SAVE_DIR = Path("/custom/path/to/geonames_data")
config.DATABASE_URL = "sqlite:///custom_database.db"
# Add other configuration options as needed
```

### Working with Multiple Queries

```python
import asyncio
from geonames.database import get_geolocation, setup_database
from geonames.config import Config as GeoNamesConfig

async def perform_multiple_queries(engine):
    queries = [
        ("US", "39046"),
        ("GB", "L1"),
        ("DE", "10115"),
    ]
    
    results = await asyncio.gather(*[get_geolocation(engine, country, postal_code) for country, postal_code in queries])
    
    for query, result in zip(queries, results):
        print(f"Results for {query[0]} {query[1]}: {result}")

async def main():
    config = GeoNamesConfig()
    engine = await setup_database(config)
    await perform_multiple_queries(engine)

if __name__ == "__main__":
    asyncio.run(main())
```

### Error Handling

It's important to handle potential errors when working with the database:

```python
import asyncio
from loguru import logger
from sqlalchemy.exc import SQLAlchemyError
from geonames.database import get_geolocation, setup_database
from geonames.config import Config as GeoNamesConfig

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
```

For more detailed usage instructions, please refer to the [documentation](https://github.com/Aareon/python-geonames/wiki).

## Continuous Integration

This project uses GitHub Actions for continuous integration. The workflow runs tests on Python 3.10 and 3.11, and reports code coverage.

To set up the GitHub Action:

1. Copy the contents of the `.github/workflows/python-tests.yml` file from this repository to your own repository.

2. Ensure your repository has the necessary secrets set up:
   - `GITHUB_TOKEN`: This is automatically provided by GitHub Actions.

3. Make sure your `pyproject.toml` file includes all necessary dependencies, including test and development dependencies.

4. Sign up for a [Codecov](https://codecov.io/) account and add your repository.

5. The GitHub Action will now run automatically on every push to the `main` branch and every pull request targeting the `main` branch.

You can view the status of the tests and coverage in the shields at the top of this README.

## Development

This project uses Poetry for dependency management. To set up the development environment:

1. Install Poetry if you haven't already:
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```

2. Clone the repository:
   ```bash
   git clone https://github.com/Aareon/python-geonames.git
   cd python-geonames
   ```

3. Install the project dependencies:
   ```bash
   poetry install --with test,develop
   ```

4. Activate the virtual environment:
   ```bash
   poetry shell
   ```

## Running Tests

To run the tests:

```bash
pytest
```

To run the tests with coverage:

```bash
pytest --cov=geonames tests/
```

## Type Checking

To run type checking with mypy:

```bash
mypy geonames
```

## Code Formatting

This project uses `black` for code formatting and `isort` for import sorting. To format your code:

```bash
black .
isort .
```

## Linting

To run the linter:

```bash
ruff check .
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the terms of the MIT license. See [LICENSE](LICENSE) for more details.