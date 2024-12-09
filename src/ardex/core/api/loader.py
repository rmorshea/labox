from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import Literal
from typing import ParamSpec
from typing import TypeVar

from anyio import create_task_group
from anysync import contextmanager
from pybooster import injector
from pybooster import required

from ardex.core.schema import DataRelation
from ardex.core.serializer import SerializerRegistry
from ardex.core.serializer import StreamSerializer
from ardex.core.storage import StorageRegistry
from ardex.utils.anyio import TaskGroupFuture
from ardex.utils.anyio import start_future

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator

    from ardex.core.serializer import ValueSerializer
    from ardex.core.storage import Storage

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

    yield DataLoader(items)

    await _load_data(items, storage_registry, serializer_registry)


class DataLoader:
    """Defines a protocol for saving data."""

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


async def _load_data(
    items: list[tuple[TaskGroupFuture, DataRelation, Literal["value", "stream"]]],
    storage_registry: StorageRegistry,
    serializer_registry: SerializerRegistry,
) -> None:
    async with create_task_group() as tg:
        for fut, rel, typ in items:
            if typ == "value":
                serializer = serializer_registry.by_name[rel.rel_serializer_name]
                storage = storage_registry.by_name[rel.rel_storage_name]
                start_future(tg, fut, _load_value, rel, serializer, storage)
            else:
                stream_serializer = serializer_registry.by_name[rel.rel_serializer_name]
                if not isinstance(stream_serializer, StreamSerializer):
                    msg = f"Data relation {rel} does not support streaming."
                    raise ValueError(msg)
                storage = storage_registry.by_name[rel.rel_storage_name]
                fut._result = _load_stream(rel, stream_serializer, storage)  # noqa: SLF001


async def _load_value(
    relation: DataRelation,
    serializer: ValueSerializer | StreamSerializer,
    storage: Storage,
) -> Any:
    """Load the given value data."""
    return serializer.load_value(
        {
            "value": await storage.read_value(relation),
            "content_type": relation.rel_content_type,
            "serializer_name": relation.rel_serializer_name,
            "serializer_version": relation.rel_serializer_version,
        }
    )


def _load_stream(
    relation: DataRelation,
    serializer: StreamSerializer,
    storage: Storage,
) -> AsyncIterable[Any]:
    """Load the given stream data."""
    return serializer.load_stream(
        {
            "stream": storage.read_stream(relation),
            "content_type": relation.rel_content_type,
            "serializer_name": relation.rel_serializer_name,
            "serializer_version": relation.rel_serializer_version,
        }
    )
