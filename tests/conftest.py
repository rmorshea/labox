from collections.abc import AsyncIterator
from collections.abc import Iterator
from tempfile import NamedTemporaryFile

import pytest
from pybooster import provider
from pybooster import solved
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from lakery.core.context import DatabaseSession
from lakery.core.context import Registries
from lakery.core.context import current_registries
from lakery.core.model import ModelRegistry
from lakery.core.schema import Base
from lakery.core.serializer import SerializerRegistry
from lakery.core.storage import StorageRegistry
from lakery.extra.json import JsonSerializer
from lakery.extra.json import JsonStreamSerializer
from lakery.extra.tempfile import TemporaryDirectoryStorage


@pytest.fixture(autouse=True, scope="session")
def basic_registries() -> Iterator[Registries]:
    with current_registries(
        models=ModelRegistry.with_core_models(),
        storages=StorageRegistry([TemporaryDirectoryStorage()]),
        serializers=SerializerRegistry([JsonSerializer(), JsonStreamSerializer()]),
    ) as reg:
        yield reg


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
