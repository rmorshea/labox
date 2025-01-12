from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from typing import TYPE_CHECKING
from typing import NewType

from pybooster import injector
from sqlalchemy.ext.asyncio import AsyncSession

from lakery.core.model import ModelRegistry
from lakery.core.serializer import SerializerRegistry
from lakery.core.storage import StorageRegistry

if TYPE_CHECKING:
    from collections.abc import Iterator


DatabaseSession = NewType("DatabaseSession", AsyncSession)
"""A type alias for an lakery database session."""


@contextmanager
def registries(
    *,
    models: ModelRegistry | None = None,
    serializers: SerializerRegistry | None = None,
    storages: StorageRegistry | None = None,
) -> Iterator[Registries]:
    """Declare the set of storage and serializers to use for the duration of the context."""
    current = injector.current_values()

    kwargs = asdict(current[Registries]) if Registries in current else {}
    if models is not None:
        kwargs["models"] = models
    if serializers is not None:
        kwargs["serializers"] = serializers
    if storages is not None:
        kwargs["storages"] = storages

    reg = Registries(**kwargs)
    with injector.shared((Registries, reg)):
        yield reg


@dataclass
class Registries:
    """A collection of registries."""

    models: ModelRegistry = field(default_factory=ModelRegistry)
    serializers: SerializerRegistry = field(default_factory=SerializerRegistry)
    storages: StorageRegistry = field(default_factory=StorageRegistry)
