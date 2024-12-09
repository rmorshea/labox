from contextlib import contextmanager
from typing import Any
from typing import NewType

from anysync.core import Iterator
from labrary.core.serializer import SerializerRegistry
from labrary.core.storage import StorageRegistry
from pybooster import injector
from sqlalchemy.ext.asyncio import AsyncSession

DatabaseSession = NewType("DatabaseSession", AsyncSession)
"""A type alias for an labrary database session."""


@contextmanager
def registries(
    *,
    storages: StorageRegistry | None = None,
    serializers: SerializerRegistry | None = None,
) -> Iterator[None]:
    """Declare the set of storage and serializers to use for the duration of the context."""
    regs: list[tuple[type, Any]] = []
    if storages is not None:
        regs.append((StorageRegistry, storages))
    if serializers is not None:
        regs.append((SerializerRegistry, serializers))
    with injector.shared(*regs):
        yield None
