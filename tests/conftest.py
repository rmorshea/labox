import functools
from collections.abc import AsyncIterator
from tempfile import NamedTemporaryFile

import pytest
from pybooster import provider
from pybooster import solved
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from ardex.core.schema import Base
from ardex.core.serializer import ScalarSerializerRegistry
from ardex.core.serializer import StreamSerializerRegistry
from ardex.core.storage import StorageRegistry
from ardex.extra.json import JsonSerializer
from ardex.extra.tempfile import TemporaryDirectoryStorage


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
    return ScalarSerializerRegistry([JsonSerializer()])


@provider.function
@functools.lru_cache(1)
def stream_serializer_registry_provider() -> StreamSerializerRegistry:
    return StreamSerializerRegistry([JsonSerializer()])


@provider.function
@functools.lru_cache(1)
def storage_registry_provider() -> StorageRegistry:
    return StorageRegistry([TemporaryDirectoryStorage()])


@pytest.fixture(autouse=True, scope="session")
def providers():
    with NamedTemporaryFile() as tempfile:
        with solved(
            aiosqlite_session_provider.bind(path=tempfile.name),
            scalar_serializer_registry_provider,
            stream_serializer_registry_provider,
            storage_registry_provider,
        ):
            yield
