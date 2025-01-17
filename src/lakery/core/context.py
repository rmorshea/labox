from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
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


EMPTY_MODEL_REGISTRY = ModelRegistry()
"""An empty registry of models."""
EMPTY_SERIALIZER_REGISTRY = SerializerRegistry()
"""An empty registry of serializers."""
EMPTY_STORAGE_REGISTRY = StorageRegistry()
"""An empty registry of storages."""


@contextmanager
def current_registries(
    *,
    models: ModelRegistry = EMPTY_MODEL_REGISTRY,
    serializers: SerializerRegistry = EMPTY_SERIALIZER_REGISTRY,
    storages: StorageRegistry = EMPTY_STORAGE_REGISTRY,
    ignore_conflicts: bool = False,
) -> Iterator[Registries]:
    """Declare the set of storage and serializers to use for the duration of the context."""
    current = injector.current_values()

    to_merge: list[Registries] = []
    if (last_registries := current.get(Registries)) is not None:
        to_merge.append(last_registries)
    to_merge.append(Registries(models=models, serializers=serializers, storages=storages))

    next_registries = Registries.merge(*to_merge, ignore_conflicts=ignore_conflicts)
    with injector.shared((Registries, next_registries)):
        yield next_registries


@dataclass
class Registries:
    """A collection of registries."""

    models: ModelRegistry = EMPTY_MODEL_REGISTRY
    """A registry of models."""
    serializers: SerializerRegistry = EMPTY_SERIALIZER_REGISTRY
    """A registry of serializers."""
    storages: StorageRegistry = EMPTY_STORAGE_REGISTRY
    """A registry of storages."""

    @classmethod
    def merge(cls, *others: Registries, ignore_conflicts: bool = False) -> Registries:
        """Return a new collection of registries that merges this one with the given ones."""
        new_kwargs = {
            "models": ModelRegistry.merge(
                *(r.models for r in others),
                ignore_conflicts=ignore_conflicts,
            ),
            "serializers": SerializerRegistry.merge(
                *(r.serializers for r in others),
                ignore_conflicts=ignore_conflicts,
            ),
            "storages": StorageRegistry.merge(
                *(r.storages for r in others),
                ignore_conflicts=ignore_conflicts,
            ),
        }
        return Registries(**new_kwargs)
