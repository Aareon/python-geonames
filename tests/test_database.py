import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from geonames.database import create_database, bulk_insert_data, get_geolocation, database_exists
from geonames.models import Base

@pytest.fixture
async def test_engine():
    engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()

@pytest.mark.asyncio
async def test_create_database(test_engine):
    await create_database(test_engine)
    assert await database_exists(test_engine)

@pytest.mark.asyncio
async def test_bulk_insert_data(test_engine):
    test_data = [
        {
            'country_code': 'US',
            'postal_code': '90210',
            'place_name': 'Beverly Hills',
            'admin_name1': 'California',
            'admin_code1': 'CA',
            'latitude': 34.0901,
            'longitude': -118.4065,
            'accuracy': 4
        }
    ]
    await bulk_insert_data(test_engine, test_data)
    result = await get_geolocation(test_engine, 'US', '90210')
    assert len(result) == 1
    assert result[0]['city'] == 'Beverly Hills'

@pytest.mark.asyncio
async def test_get_geolocation(test_engine):
    test_data = [
        {
            'country_code': 'US',
            'postal_code': '90210',
            'place_name': 'Beverly Hills',
            'admin_name1': 'California',
            'admin_code1': 'CA',
            'latitude': 34.0901,
            'longitude': -118.4065,
            'accuracy': 4
        }
    ]
    await bulk_insert_data(test_engine, test_data)
    result = await get_geolocation(test_engine, 'US', '90210')
    assert len(result) == 1
    assert result[0]['city'] == 'Beverly Hills'
    assert result[0]['state'] == 'California'
    assert result[0]['latitude'] == 34.0901
    assert result[0]['longitude'] == -118.4065

    # Test non-existent zipcode
    result = await get_geolocation(test_engine, 'US', '00000')
    assert len(result) == 0