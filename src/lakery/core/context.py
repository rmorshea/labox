from __future__ import annotations

from collections.abc import Callable
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
    from collections.abc import Callable
    from collections.abc import Iterator


DatabaseSession = NewType("DatabaseSession", AsyncSession)
"""A type alias for an lakery database session."""


EMPTY_MODEL_REGISTRY = ModelRegistry()
"""An empty registry of models."""
EMPTY_SERIALIZER_REGISTRY = SerializerRegistry()
"""An empty registry of serializers."""
EMPTY_STORAGE_REGISTRY = StorageRegistry()
"""An empty registry of storages."""


@dataclass(frozen=True)
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
        return Registries(
            models=ModelRegistry.merge(
                *(r.models for r in others),
                ignore_conflicts=ignore_conflicts,
            ),
            serializers=SerializerRegistry.merge(
                *(r.serializers for r in others),
                ignore_conflicts=ignore_conflicts,
            ),
            storages=StorageRegistry.merge(
                *(r.storages for r in others),
                ignore_conflicts=ignore_conflicts,
            ),
        )

    @contextmanager
    def context(self, *, ignore_conflicts: bool = False) -> Iterator[Registries]:
        """Declare the set of storage and serializers to use for the duration of the context."""
        current = injector.current_values()
        current_registries = current.get(Registries, None)
        new_registries = (
            Registries.merge(current_registries, self, ignore_conflicts=ignore_conflicts)
            if current_registries
            else self
        )
        with injector.shared((Registries, new_registries)):
            yield new_registries

    def begin_context(self) -> Callable[[], None]:
        """Begin a global context for the registries and return a function to end it."""
        ctx = self.context()
        ctx.__enter__()

        def end_global_context() -> None:
            ctx.__exit__(None, None, None)

        return end_global_context
