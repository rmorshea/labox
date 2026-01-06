from collections.abc import AsyncIterator
from tempfile import NamedTemporaryFile

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from labox.core.database import BaseRecord


@pytest.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    with NamedTemporaryFile() as file:
        engine = create_async_engine(f"sqlite+aiosqlite:///{file.name}")
        await BaseRecord.create_all(engine)
        yield engine
        await engine.dispose()


@pytest.fixture
async def session(engine: AsyncEngine) -> AsyncIterator[AsyncSession]:
    async with AsyncSession(engine, expire_on_commit=False) as session:
        yield session
