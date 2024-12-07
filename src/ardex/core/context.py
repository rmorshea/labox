from contextlib import contextmanager
from typing import Any
from typing import NewType

from anysync.core import Iterator
from pybooster import injector
from sqlalchemy.ext.asyncio import AsyncSession

from ardex.core.serializer import ScalarSerializerRegistry
from ardex.core.serializer import StreamSerializerRegistry
from ardex.core.storage import StorageRegistry

DatabaseSession = NewType("DatabaseSession", AsyncSession)
"""A type alias for an Ardex database session."""


@contextmanager
def registries(
    *,
    storages: StorageRegistry | None = None,
    stream_serializers: StreamSerializerRegistry | None = None,
    scalar_serializers: ScalarSerializerRegistry | None = None,
) -> Iterator[None]:
    """Declare the set of storage and serializers to use for the duration of the context."""
    regs: list[tuple[type, Any]] = []
    if storages is not None:
        regs.append((StorageRegistry, storages))
    if stream_serializers is not None:
        regs.append((StreamSerializerRegistry, stream_serializers))
    if scalar_serializers is not None:
        regs.append((ScalarSerializerRegistry, scalar_serializers))
    with injector.shared(*regs):
        yield None
