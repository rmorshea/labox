from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import Literal
from typing import ParamSpec
from typing import Protocol
from typing import TypeVar

from anyio import create_task_group
from anysync import contextmanager
from pybooster import injector
from pybooster import required

from datos.core.schema import DataRelation
from datos.utils.anyio import TaskGroupFuture
from datos.utils.anyio import start_future
from datos.utils.misc import frozenclass

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator

    from datos.core.serializer import ScalarSerializer
    from datos.core.serializer import ScalarSerializerRegistry
    from datos.core.serializer import StreamSerializer
    from datos.core.serializer import StreamSerializerRegistry
    from datos.core.storage import Storage
    from datos.core.storage import StorageRegistry

T = TypeVar("T")
R = TypeVar("R", bound=DataRelation)
P = ParamSpec("P")


@contextmanager
@injector.asynciterator
async def data_loader(
    *,
    storage_registry: StorageRegistry = required,
    stream_serializer_registry: StreamSerializerRegistry = required,
    scalar_serializer_registry: ScalarSerializerRegistry = required,
) -> AsyncIterator[DataLoader]:
    """Create a context manager for saving data."""
    items: list[tuple[TaskGroupFuture, DataRelation, Literal["scalar", "stream"]]] = []

    def load_scalar(relation: DataRelation) -> TaskGroupFuture[Any]:
        fut = TaskGroupFuture()
        items.append((fut, relation, "scalar"))
        return fut

    def load_stream(relation: DataRelation) -> TaskGroupFuture[Any]:
        fut = TaskGroupFuture()
        items.append((fut, relation, "stream"))
        return fut

    yield DataLoader(scalar=load_scalar, stream=load_stream)

    await _load_data(
        items,
        storage_registry,
        stream_serializer_registry,
        scalar_serializer_registry,
    )


@frozenclass
class DataLoader:
    """Defines a protocol for saving data."""

    scalar: _ScalarLoader
    stream: _StreamLoader


class _ScalarLoader(Protocol):
    def __call__(self, relation: DataRelation) -> TaskGroupFuture[Any]: ...


class _StreamLoader(Protocol):
    def __call__(self, relation: DataRelation) -> TaskGroupFuture[AsyncIterable[Any]]: ...


async def _load_data(
    items: list[tuple[TaskGroupFuture, DataRelation, Literal["scalar", "stream"]]],
    storage_registry: StorageRegistry,
    stream_serializer_registry: StreamSerializerRegistry,
    scalar_serializer_registry: ScalarSerializerRegistry,
) -> None:
    async with create_task_group() as tg:
        for fut, rel, typ in items:
            if typ == "scalar":
                scalar_serializer = scalar_serializer_registry.by_name[rel.rel_serializer_name]
                storage = storage_registry.by_name[rel.rel_storage_name]
                start_future(tg, fut, _load_scalar, rel, scalar_serializer, storage)
            else:
                stream_serializer = stream_serializer_registry.by_name[rel.rel_serializer_name]
                storage = storage_registry.by_name[rel.rel_storage_name]
                start_future(tg, fut, _load_stream, rel, stream_serializer, storage)


async def _load_scalar(
    relation: DataRelation,
    serializer: ScalarSerializer,
    storage: Storage,
) -> Any:
    """Load the given scalar data."""
    dump = await storage.read_scalar(relation)
    return serializer.load_scalar(dump)


async def _load_stream(
    relation: DataRelation,
    serializer: StreamSerializer,
    storage: Storage,
) -> AsyncIterable[Any]:
    """Load the given stream data."""
    dump = await storage.read_stream(relation)
    return await serializer.load_stream(dump)
