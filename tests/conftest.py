from collections.abc import AsyncIterator
from tempfile import NamedTemporaryFile

import pytest
from pybooster import provider
from pybooster import solved
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from ardex.core.context import DatabaseSession
from ardex.core.context import registries
from ardex.core.schema import Base
from ardex.core.serializer import ScalarSerializerRegistry
from ardex.core.serializer import StreamSerializerRegistry
from ardex.core.storage import StorageRegistry
from ardex.extra.json import JsonSerializer
from ardex.extra.tempfile import TemporaryDirectoryStorage


@pytest.fixture(autouse=True, scope="session")
def registry_context():
    with registries(
        storages=StorageRegistry([TemporaryDirectoryStorage()]),
        stream_serializers=StreamSerializerRegistry([JsonSerializer()]),
        scalar_serializers=ScalarSerializerRegistry([JsonSerializer()]),
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
