from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import ParamSpec
from typing import TypeAlias
from typing import TypeVar

from pybooster import injector
from pybooster import required

from lakery.core.schema import DataRelation
from lakery.core.serializer import SerializerRegistry
from lakery.core.serializer import StreamSerializer
from lakery.core.storage import StorageRegistry
from lakery.core.storage import StreamStorage

if TYPE_CHECKING:
    from collections.abc import AsyncIterable


T = TypeVar("T")
R = TypeVar("R", bound=DataRelation)
P = ParamSpec("P")


@injector.function(requires=(StorageRegistry, SerializerRegistry))
def data_loader(
    *,
    session: DatabaseSession = required,
    compositors: CompositorRegistry = required,
    serializers: SerializerRegistry = required,
    storages: StorageRegistry = required,
) -> DataLoader:
    """Create a context manager for saving data."""
    return _DataLoader(storage_registry=storage_registry, serializer_registry=serializer_registry)


class _DataLoader:
    def __init__(
        self,
        *,
        storage_registry: StorageRegistry,
        serializer_registry: SerializerRegistry,
    ) -> None:
        self._storage_registry = storage_registry
        self._serializer_registry = serializer_registry

    async def value(self, relation: DataRelation) -> Any:
        """Load the given data as a value value."""
        serializer = self._serializer_registry.get_by_name(relation.rel_serializer_name)
        storage = self._storage_registry.get_by_name(relation.rel_storage_name)
        return serializer.load_value(
            {
                "content_encoding": relation.rel_content_encoding,
                "content_type": relation.rel_content_type,
                "content_value": await storage.get_value(relation),
                "serializer_name": relation.rel_serializer_name,
                "serializer_version": relation.rel_serializer_version,
            }
        )

    def stream(self, relation: DataRelation) -> AsyncIterable[Any]:
        """Load the given data as a stream of values."""
        serializer = self._serializer_registry.get_by_name(relation.rel_serializer_name)
        if not isinstance(serializer, StreamSerializer):
            msg = f"No stream serializer can load data relation {relation}."
            raise TypeError(msg)
        storage = self._storage_registry.get_by_name(relation.rel_storage_name)
        if not isinstance(storage, StreamStorage):
            msg = f"No stream storage can load data relation {relation}."
            raise TypeError(msg)
        return serializer.load_stream(
            {
                "content_encoding": relation.rel_content_encoding,
                "content_stream": storage.get_stream(relation),
                "content_type": relation.rel_content_type,
                "serializer_name": relation.rel_serializer_name,
                "serializer_version": relation.rel_serializer_version,
            }
        )


DataLoader: TypeAlias = _DataLoader
"""Defines a protocol for saving data."""
