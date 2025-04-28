from __future__ import annotations

from typing import NewType

from sqlalchemy.ext.asyncio import AsyncSession

from lakery.common.utils import frozenclass
from lakery.core.model import ModelRegistry
from lakery.core.serializer import SerializerRegistry
from lakery.core.storage import StorageRegistry

DatabaseSession = NewType("DatabaseSession", AsyncSession)
"""A type alias for an lakery database session."""


EMPTY_MODEL_REGISTRY = ModelRegistry()
"""An empty registry of models."""
EMPTY_SERIALIZER_REGISTRY = SerializerRegistry()
"""An empty registry of serializers."""
EMPTY_STORAGE_REGISTRY = StorageRegistry()
"""An empty registry of storages."""


@frozenclass
class Registries:
    """A collection of registries."""

    models: ModelRegistry = EMPTY_MODEL_REGISTRY
    """A registry of models."""
    serializers: SerializerRegistry = EMPTY_SERIALIZER_REGISTRY
    """A registry of serializers."""
    storages: StorageRegistry = EMPTY_STORAGE_REGISTRY
    """A registry of storages."""

    @classmethod
    def merge(
        cls,
        *others: Registries,
        models: ModelRegistry = EMPTY_MODEL_REGISTRY,
        serializers: SerializerRegistry = EMPTY_SERIALIZER_REGISTRY,
        storages: StorageRegistry = EMPTY_STORAGE_REGISTRY,
        ignore_conflicts: bool = False,
    ) -> Registries:
        """Return a new collection of registries that merges this one with the given ones."""
        return Registries(
            models=ModelRegistry.merge(
                models,
                *(r.models for r in others),
                ignore_conflicts=ignore_conflicts,
            ),
            serializers=SerializerRegistry.merge(
                serializers,
                *(r.serializers for r in others),
                ignore_conflicts=ignore_conflicts,
            ),
            storages=StorageRegistry.merge(
                storages,
                *(r.storages for r in others),
                ignore_conflicts=ignore_conflicts,
            ),
        )
