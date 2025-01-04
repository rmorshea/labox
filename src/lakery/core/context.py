from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING
from typing import Any
from typing import NewType

from pybooster import injector
from sqlalchemy.ext.asyncio import AsyncSession

from lakery.core.composer import ComposerRegistry
from lakery.core.serializer import SerializerRegistry
from lakery.core.storage import StorageRegistry

if TYPE_CHECKING:
    from collections.abc import Iterator

DatabaseSession = NewType("DatabaseSession", AsyncSession)
"""A type alias for an lakery database session."""


@contextmanager
def registries(
    *,
    compositors: ComposerRegistry | None = None,
    serializers: SerializerRegistry | None = None,
    storages: StorageRegistry | None = None,
) -> Iterator[None]:
    """Declare the set of storage and serializers to use for the duration of the context."""
    regs: list[tuple[type, Any]] = []

    if serializers is not None:
        regs.append((SerializerRegistry, serializers))
    if compositors is not None:
        regs.append((ComposerRegistry, compositors))
    if storages is not None:
        regs.append((StorageRegistry, storages))

    with injector.shared(*regs):
        yield None
