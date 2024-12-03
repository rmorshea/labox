import functools
from collections.abc import AsyncIterator
from tempfile import NamedTemporaryFile

import pytest
from pybooster import provider
from pybooster import solved
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from datos.core.schema import Base
from datos.core.serializer import ScalarSerializerRegistry
from datos.core.serializer import StreamSerializerRegistry
from datos.core.storage import StorageRegistry
from datos.extra.json import JsonScalarSerializer
from datos.extra.json import JsonStreamSerializer
from datos.extra.tempfile import TemporaryDirectoryStorage


@provider.asynciterator
async def aiosqlite_session_provider(path: str) -> AsyncIterator[AsyncSession]:
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with AsyncSession(engine) as session:
        yield session


@provider.function
@functools.lru_cache(1)
def scalar_serializer_registry_provider() -> ScalarSerializerRegistry:
    return ScalarSerializerRegistry([JsonScalarSerializer()])


@provider.function
@functools.lru_cache(1)
def stream_serializer_registry_provider() -> StreamSerializerRegistry:
    return StreamSerializerRegistry([JsonStreamSerializer()])


@provider.function
@functools.lru_cache(1)
def storage_registry_provider() -> StorageRegistry:
    return StorageRegistry([TemporaryDirectoryStorage()])


@pytest.fixture(autouse=True)
def providers():
    with NamedTemporaryFile() as file:
        with solved(
            aiosqlite_session_provider.bind(path=file.name),
            scalar_serializer_registry_provider,
            stream_serializer_registry_provider,
            storage_registry_provider,
        ):
            yield
