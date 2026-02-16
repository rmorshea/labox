from collections.abc import AsyncIterator
from tempfile import NamedTemporaryFile

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from labox.core.database import BaseRecord


@pytest.fixture
async def session() -> AsyncIterator[AsyncSession]:
    with NamedTemporaryFile() as file:
        engine = create_async_engine(f"sqlite+aiosqlite:///{file.name}")
        await BaseRecord.create_all(engine)
        async with AsyncSession(engine, expire_on_commit=False) as session:
            yield session
