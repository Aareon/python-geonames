# Geonames Database

This package provides tools for downloading, processing, and querying geonames data. It uses asyncio for improved performance and SQLite for local data storage.

## Features

- Asynchronous download and processing of geonames data
- Efficient SQLite database storage
- Geolocation querying by country code and postal code
- Automatic update checking and data refresh

## Installation

1. Clone this repository
2. Install dependencies using Poetry:

```
poetry install
```

## Usage

```python
import asyncio
from geonames.database import setup_database, get_geolocation

async def main():
 engine = await setup_database()
 results = await get_geolocation(engine, "US", "90210")
 print(f"Geolocation results: {results}")

if __name__ == "__main__":
 asyncio.run(main())
```

## Development

To run tests:

```
poetry run pytest
```

To format code:

```
poetry run black .
poetry run isort .
```

To run type checking:

```
poetry run mypy .
```

## License

This project is licensed under the MIT License.
```

This completes our comprehensive update of the geonames database package, incorporating best practices, improved performance and modularity, better documentation, testing strategies, update checking, and overall efficiency improvements.