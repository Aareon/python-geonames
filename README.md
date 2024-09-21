# python-geonames

[![Tests](https://img.shields.io/github/workflow/status/Aareon/python-geonames/tests?label=tests)](https://github.com/Aareon/python-geonames/actions)
[![Mypy](https://img.shields.io/badge/mypy-checked-blue)](https://github.com/Aareon/python-geonames/actions)
[![Coverage](https://img.shields.io/codecov/c/github/Aareon/python-geonames)](https://codecov.io/gh/Aareon/python-geonames)
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

You can customize the configuration by modifying the `Config` object:

```python
from geonames.config import Config

config = Config()
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

## Development

To set up the development environment:

1. Clone the repository:
   ```bash
   git clone https://github.com/Aareon/python-geonames.git
   cd python-geonames
   ```

2. Create a virtual environment and activate it:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows, use \`venv\\Scripts\\activate\`
   ```

3. Install the development dependencies:
   ```bash
   pip install -r requirements-dev.txt
   ```

4. Install the pre-commit hooks:
   ```bash
   pre-commit install
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

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the terms of the MIT license. See [LICENSE](LICENSE) for more details.