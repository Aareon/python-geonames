import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from geonames.models import Base

@pytest.fixture
async def test_engine():
    engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()