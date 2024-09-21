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