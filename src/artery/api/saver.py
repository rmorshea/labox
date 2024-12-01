from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Generic
from typing import ParamSpec
from typing import Protocol
from typing import Required
from typing import TypedDict
from typing import TypeVar

from anyio import create_task_group
from anysync import contextmanager
from pybooster import injector
from pybooster import required
from sqlalchemy import IntegrityError
from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import update
from tenacity import AsyncRetrying
from tenacity import stop_after_attempt

from artery.core.schema import DataRelation
from artery.utils.anyio import TaskGroupFuture
from artery.utils.anyio import start_future
from artery.utils.misc import frozenclass

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator
    from collections.abc import Callable
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from artery.core.serializer import ScalarSerializer
    from artery.core.serializer import ScalarSerializerRegistry
    from artery.core.serializer import StreamSerializer
    from artery.core.serializer import StreamSerializerRegistry
    from artery.core.storage import Storage
    from artery.core.storage import StorageRegistry


T = TypeVar("T")
R = TypeVar("R", bound=DataRelation)
P = ParamSpec("P")


@contextmanager
@injector.asynciterator
async def data_saver(
    *,
    session: AsyncSession = required,
    storage_registry: StorageRegistry = required,
    stream_serializer_registry: StreamSerializerRegistry = required,
    scalar_serializer_registry: ScalarSerializerRegistry = required,
) -> AsyncIterator[DataSaver]:
    """Create a context manager for saving data."""
    items: list[tuple[TaskGroupFuture[DataRelation], DataRelation, _ScalarData | _StreamData]] = []

    def save_scalar(
        relation: Callable[P, R],
        scalar: R,
        serializer: ScalarSerializer[R] | None = None,
        storage: Storage[R] | None = None,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskGroupFuture[R]:
        fut = TaskGroupFuture[R]()
        rel = relation(*args, **kwargs)
        dat: _ScalarData = {"scalar": scalar}
        if serializer is not None:
            dat["serializer"] = serializer
        if storage is not None:
            dat["storage"] = storage
        items.append((fut, rel, dat))  # type: ignore[reportArgumentType]
        return fut

    def save_stream(
        relation: Callable[P, R],
        stream: AsyncIterable[R],
        serializer: StreamSerializer[R] | type[R],
        storage: Storage[R] | None = None,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskGroupFuture[R]:
        fut = TaskGroupFuture[R]()
        rel = relation(*args, **kwargs)
        dat: _StreamData
        if isinstance(serializer, type):
            dat: _StreamData = {"stream": stream, "type": serializer}
        else:
            dat: _StreamData = {"stream": stream, "serializer": serializer}
        if storage is not None:
            dat["storage"] = storage
        items.append((fut, rel, dat))  # type: ignore[reportArgumentType]
        return fut

    yield DataSaver(scalar=save_scalar, stream=save_stream)

    await _save_data(
        items,
        session,
        storage_registry,
        stream_serializer_registry,
        scalar_serializer_registry,
    )


@frozenclass
class DataSaver:
    """Defines a protocol for saving data."""

    scalar: _ScalarSaver
    stream: _StreamSaver


class _ScalarSaver(Protocol[R]):
    def __call__(
        self,
        relation: Callable[P, R],
        scalar: R,
        serializer: ScalarSerializer[R],
        storage: Storage[R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskGroupFuture[R]: ...


class _StreamSaver(Protocol[R]):
    def __call__(
        self,
        relation: Callable[P, R],
        stream: AsyncIterable[R],
        serializer: StreamSerializer[R],
        storage: Storage[R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskGroupFuture[R]: ...


async def _save_data(
    items: Sequence[tuple[TaskGroupFuture[DataRelation], DataRelation, _ScalarData | _StreamData]],
    session: AsyncSession,
    storage_registry: StorageRegistry,
    stream_serializer_registry: StreamSerializerRegistry,
    scalar_serializer_registry: ScalarSerializerRegistry,
) -> Sequence[DataRelation]:
    """Save the given data to the database."""
    relation_futures: list[TaskGroupFuture[DataRelation]] = []
    try:
        async with create_task_group() as tg:
            for fut, rel, dat in items:
                if "stream" in dat:
                    start_future(
                        tg,
                        fut,
                        _save_stream,
                        rel,
                        dat,
                        storage_registry,
                        stream_serializer_registry,
                    )
                else:
                    start_future(
                        tg,
                        fut,
                        _save_scalar,
                        rel,
                        dat,
                        storage_registry,
                        scalar_serializer_registry,
                    )
        return [f.result() for f in relation_futures]
    finally:
        relations = [r for f in relation_futures if (r := f.result(default=None)) is not None]
        await _save_relations(session, relations, retries=3)


async def _save_scalar(
    relation: DataRelation,
    data: _ScalarData,
    storage_registry: StorageRegistry,
    serializer_registry: ScalarSerializerRegistry,
) -> DataRelation:
    match data:
        case {"scalar": scalar, "serializer": serializer, "storage": storage}:
            pass
        case {"scalar": scalar, "serializer": serializer}:
            storage = storage_registry.get_by_type_inference(type(relation))
        case {"scalar": scalar, "storage": storage}:
            serializer = serializer_registry.get_by_type_inference(type(scalar))
        case {"scalar": scalar}:
            storage = storage_registry.get_by_type_inference(type(relation))
            serializer = serializer_registry.get_by_type_inference(type(scalar))
        case _:
            msg = f"Invalid data dictionary: {data}"
            raise ValueError(msg)

    dump = serializer.dump_scalar(scalar)

    relation.rel_content_type = dump["content_type"]
    relation.rel_serializer_name = serializer.name
    relation.rel_serialier_version = serializer.version
    relation.rel_storage_name = storage.name
    relation.rel_storage_version = storage.version

    return await storage.write_scalar(relation, dump)


async def _save_stream(
    relation: DataRelation,
    data: _StreamData,
    storage_registry: StorageRegistry,
    serializer_registry: StreamSerializerRegistry,
) -> DataRelation:
    match data:
        case {"stream": stream, "serializer": serializer, "storage": storage}:
            pass
        case {"stream": stream, "serializer": serializer}:
            storage = storage_registry.get_by_type_inference(type(relation))
        case {"stream": stream, "storage": storage, "type": type_}:
            serializer = serializer_registry.get_by_type_inference(type_)
        case {"stream": stream, "type": type_}:
            serializer = serializer_registry.get_by_type_inference(type_)
            storage = storage_registry.get_by_type_inference(type(relation))
        case _:
            msg = f"Invalid data dictionary: {data}"
            raise ValueError(msg)

    dump = await serializer.dump_stream(stream)

    relation.rel_storage_name = storage.name
    relation.rel_storage_version = storage.version
    relation.rel_content_type = dump["content_type"]
    relation.rel_serializer_name = serializer.name
    relation.rel_serialier_version = serializer.version

    return await storage.write_stream(relation, dump)


async def _save_relations(
    session: AsyncSession,
    relations: Sequence[DataRelation],
    retries: int,
) -> None:
    stop = stop_after_attempt(retries)
    update_existing_stmt = update(DataRelation).where(
        or_(*(r.rel_select_latest() for r in relations)).values(
            {DataRelation.rel_archived_at: func.now()}
        )
    )
    async for attempt in AsyncRetrying(stop=stop, retry_error_cls=IntegrityError):
        with attempt:
            async with session.begin_nested():
                await session.execute(update_existing_stmt)
                session.add_all(relations)
                await session.flush()


class _ScalarData(Generic[T, R], TypedDict, total=False):
    scalar: Required[T]
    serializer: ScalarSerializer[R]
    storage: Storage[R]


class _BaseStreamData(Generic[T, R], TypedDict, total=False):
    stream: Required[AsyncIterable[T]]
    storage: Storage[R]


class _InferStreamData(_BaseStreamData[T, R]):
    type: Required[type[T]]


class _KnownStreamData(_BaseStreamData[T, R]):
    serializer: Required[StreamSerializer[T]]


_StreamData = _KnownStreamData[T, R] | _InferStreamData[T, R]
