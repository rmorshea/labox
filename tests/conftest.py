from collections.abc import AsyncIterator
from tempfile import NamedTemporaryFile

import pytest
from pybooster import provider
from pybooster import solved
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from labrary.core.context import DatabaseSession
from labrary.core.context import registries
from labrary.core.schema import Base
from labrary.core.serializer import SerializerRegistry
from labrary.core.storage import StorageRegistry
from labrary.extra.json import JsonSerializer
from labrary.extra.json import JsonStreamSerializer
from labrary.extra.tempfile import TemporaryDirectoryStorage


@pytest.fixture(autouse=True, scope="session")
def registry_context():
    with registries(
        storages=StorageRegistry([TemporaryDirectoryStorage()]),
        serializers=SerializerRegistry([JsonSerializer(), JsonStreamSerializer()]),
    ):
        yield


@provider.asynciterator(provides=DatabaseSession)
async def database_session(location: str) -> AsyncIterator[DatabaseSession]:
    engine = create_async_engine(f"sqlite+aiosqlite:///{location}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with AsyncSession(engine) as session:
        yield DatabaseSession(session)


@pytest.fixture(autouse=True)
def solution():
    with NamedTemporaryFile() as file:
        with solved(database_session.bind(file.name)):
            yield
