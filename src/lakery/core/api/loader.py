from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import Literal
from typing import ParamSpec
from typing import TypeAlias
from typing import TypeVar

from anyio import create_task_group
from anysync import contextmanager
from pybooster import injector
from pybooster import required

from lakery.core.schema import DataRelation
from lakery.core.serializer import SerializerRegistry
from lakery.core.serializer import StreamSerializer
from lakery.core.storage import StorageRegistry
from lakery.core.storage import StreamStorage
from lakery.utils.anyio import TaskGroupFuture
from lakery.utils.anyio import start_given_future

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator

    from lakery.core.serializer import ValueSerializer
    from lakery.core.storage import ValueStorage

T = TypeVar("T")
R = TypeVar("R", bound=DataRelation)
P = ParamSpec("P")


@contextmanager
@injector.asynciterator(requires=(StorageRegistry, SerializerRegistry))
async def data_loader(
    *,
    storage_registry: StorageRegistry = required,
    serializer_registry: SerializerRegistry = required,
) -> AsyncIterator[DataLoader]:
    """Create a context manager for saving data."""
    items: list[tuple[TaskGroupFuture, DataRelation, Literal["value", "stream"]]] = []

    yield _DataLoader(items)

    await _load_data(items, storage_registry, serializer_registry)


class _DataLoader:
    def __init__(
        self, items: list[tuple[TaskGroupFuture, DataRelation, Literal["value", "stream"]]]
    ) -> None:
        self._items = items

    def value(self, relation: DataRelation) -> TaskGroupFuture[Any]:
        """Load the given data as a value value."""
        fut = TaskGroupFuture()
        self._items.append((fut, relation, "value"))
        return fut

    def stream(self, relation: DataRelation) -> TaskGroupFuture[AsyncIterable[Any]]:
        """Load the given data as a stream of values."""
        fut = TaskGroupFuture()
        self._items.append((fut, relation, "stream"))
        return fut


DataLoader: TypeAlias = _DataLoader
"""Defines a protocol for saving data."""


async def _load_data(
    items: list[tuple[TaskGroupFuture, DataRelation, Literal["value", "stream"]]],
    storage_registry: StorageRegistry,
    serializer_registry: SerializerRegistry,
) -> None:
    async with create_task_group() as tg:
        for fut, rel, typ in items:
            if typ == "value":
                serializer = serializer_registry.get_by_name(rel.rel_serializer_name)
                storage = storage_registry.get_by_name(rel.rel_storage_name)
                start_given_future(tg, fut, _load_value, rel, serializer, storage)
            else:
                stream_serializer = serializer_registry.get_by_name(rel.rel_serializer_name)
                if not isinstance(stream_serializer, StreamSerializer):
                    msg = f"No stream serializer can load data relation {rel}."
                    raise ValueError(msg)
                storage = storage_registry.get_by_name(rel.rel_storage_name)
                if not isinstance(storage, StreamStorage):
                    msg = f"No stream storage can load data relation {rel}."
                    raise ValueError(msg)
                fut._result = _load_stream(rel, stream_serializer, storage)  # noqa: SLF001


async def _load_value(
    relation: DataRelation,
    serializer: ValueSerializer | StreamSerializer,
    storage: ValueStorage,
) -> Any:
    """Load the given value data."""
    return serializer.load_value(
        {
            "value": await storage.get_value(relation),
            "content_type": relation.rel_content_type,
            "content_encoding": relation.rel_content_encoding,
            "serializer_name": relation.rel_serializer_name,
            "serializer_version": relation.rel_serializer_version,
        }
    )


def _load_stream(
    relation: DataRelation,
    serializer: StreamSerializer,
    storage: StreamStorage,
) -> AsyncIterable[Any]:
    """Load the given stream data."""
    return serializer.load_stream(
        {
            "stream": storage.get_stream(relation),
            "content_type": relation.rel_content_type,
            "content_encoding": relation.rel_content_encoding,
            "serializer_name": relation.rel_serializer_name,
            "serializer_version": relation.rel_serializer_version,
        }
    )
