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
from ardex.core.serializer import SingleSerializerRegistry
from ardex.core.serializer import StreamSerializerRegistry
from ardex.core.storage import StorageRegistry
from ardex.utils.anyio import TaskGroupFuture
from ardex.utils.anyio import start_future

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator

    from ardex.core.serializer import SingleSerializer
    from ardex.core.serializer import StreamSerializer
    from ardex.core.storage import Storage

T = TypeVar("T")
R = TypeVar("R", bound=DataRelation)
P = ParamSpec("P")


@contextmanager
@injector.asynciterator(
    requires=(
        StorageRegistry,
        StreamSerializerRegistry,
        SingleSerializerRegistry,
    )
)
async def data_loader(
    *,
    storage_registry: StorageRegistry = required,
    stream_serializer_registry: StreamSerializerRegistry = required,
    single_serializer_registry: SingleSerializerRegistry = required,
) -> AsyncIterator[_DataLoader]:
    """Create a context manager for saving data."""
    items: list[tuple[TaskGroupFuture, DataRelation, Literal["single", "stream"]]] = []

    yield _DataLoader(items)

    await _load_data(
        items,
        storage_registry,
        stream_serializer_registry,
        single_serializer_registry,
    )


class _DataLoader:
    """Defines a protocol for saving data."""

    def __init__(
        self, items: list[tuple[TaskGroupFuture, DataRelation, Literal["single", "stream"]]]
    ) -> None:
        self._items = items

    def single(self, relation: DataRelation) -> TaskGroupFuture[Any]:
        """Load the given data as a single value."""
        fut = TaskGroupFuture()
        self._items.append((fut, relation, "single"))
        return fut

    def stream(self, relation: DataRelation) -> TaskGroupFuture[AsyncIterable[Any]]:
        fut = TaskGroupFuture()
        self._items.append((fut, relation, "stream"))
        return fut


async def _load_data(
    items: list[tuple[TaskGroupFuture, DataRelation, Literal["single", "stream"]]],
    storage_registry: StorageRegistry,
    stream_serializer_registry: StreamSerializerRegistry,
    single_serializer_registry: SingleSerializerRegistry,
) -> None:
    async with create_task_group() as tg:
        for fut, rel, typ in items:
            if typ == "single":
                single_serializer = single_serializer_registry.by_name[rel.rel_serializer_name]
                storage = storage_registry.by_name[rel.rel_storage_name]
                start_future(tg, fut, _load_single, rel, single_serializer, storage)
            else:
                stream_serializer = stream_serializer_registry.by_name[rel.rel_serializer_name]
                storage = storage_registry.by_name[rel.rel_storage_name]
                fut._result = _load_stream(rel, stream_serializer, storage)  # noqa: SLF001


async def _load_single(
    relation: DataRelation,
    serializer: SingleSerializer,
    storage: Storage,
) -> Any:
    """Load the given single data."""
    return serializer.load_single(
        {
            "content_single": await storage.read_single(relation),
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
            "content_stream": storage.read_stream(relation),
            "content_type": relation.rel_content_type,
            "serializer_name": relation.rel_serializer_name,
            "serializer_version": relation.rel_serializer_version,
        }
    )
