from __future__ import annotations

from contextlib import aclosing
from hashlib import sha256
from typing import TYPE_CHECKING
from typing import Generic
from typing import ParamSpec
from typing import Required
from typing import TypeAlias
from typing import TypedDict
from typing import TypeVar

from anyio import create_task_group
from anysync import contextmanager
from pybooster import injector
from pybooster import required
from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from tenacity import AsyncRetrying
from tenacity import stop_after_attempt

from lakery.common.anyio import TaskGroupFuture
from lakery.common.anyio import start_given_future
from lakery.core.compositor import DEFAULT_COMPOSITOR
from lakery.core.compositor import CompositorRegistry
from lakery.core.context import DatabaseSession
from lakery.core.schema import DataDescriptor
from lakery.core.serializer import SerializerRegistry
from lakery.core.storage import GetStreamDigest
from lakery.core.storage import Storage
from lakery.core.storage import StorageRegistry
from lakery.core.storage import StreamDigest

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator
    from collections.abc import Callable
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from lakery.core.compositor import Compositor
    from lakery.core.serializer import StreamDump
    from lakery.core.serializer import StreamSerializer
    from lakery.core.serializer import ValueDump
    from lakery.core.serializer import ValueSerializer
    from lakery.core.storage import StreamStorage
    from lakery.core.storage import ValueDigest
    from lakery.core.storage import ValueStorage


T = TypeVar("T")
P = ParamSpec("P")
D = TypeVar("D", bound=DataDescriptor)

_SaverItem: TypeAlias = tuple[
    TaskGroupFuture[DataDescriptor],
    DataDescriptor,
    "_ValueData | _StreamData",
]

_COMMIT_RETRIES = 3


@contextmanager
@injector.asynciterator(
    requires=(DatabaseSession, CompositorRegistry, SerializerRegistry, StorageRegistry)
)
async def data_saver(
    *,
    session: DatabaseSession = required,
    compositors: CompositorRegistry = required,
    serializers: SerializerRegistry = required,
    storages: StorageRegistry = required,
) -> AsyncIterator[DataSaver]:
    """Create a context manager for saving data."""
    items: list[_SaverItem] = []
    yield DataSaver(items)
    await _save(
        items,
        database_session,
        storage_registry,
        serializer_registry,
        reducer_registry,
        _COMMIT_RETRIES,
    )


class _DataSaver:
    def __init__(self, items: list[_SaverItem]) -> None:
        self._items = items

    def value(
        self,
        descriptor: Callable[P, D],
        entity: T,
        compositor: Compositor[T] = DEFAULT_COMPOSITOR,
        storage: Storage[D] | None = None,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskGroupFuture[D]:
        """Save the given value data."""
        if serializer is not None and compositor is not None:
            msg = "Cannot specify both a serializer and a reducer."
            raise ValueError(msg)

        fut = TaskGroupFuture[D]()
        des = descriptor(*args, **kwargs)
        dat: _ValueData = {"value": value}

        if serializer is not None:
            dat["serializer"] = serializer
        if compositor is not None:
            dat["compositor"] = compositor
        if storage is not None:
            dat["storage"] = storage

        self._items.append((fut, des, dat))  # type: ignore[reportArgumentType]
        return fut

    def stream(
        self,
        descriptor: Callable[P, D],
        type: type[T],  # noqa: A002
        stream: AsyncIterable[T],
        serializer: StreamSerializer[T] | None = None,
        storage: StreamStorage[D] | None = None,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskGroupFuture[D]:
        """Save the given stream data."""
        fut = TaskGroupFuture[D]()
        des = descriptor(*args, **kwargs)
        dat: _StreamData = {"type": type, "stream": stream}

        if serializer is not None:
            dat["serializer"] = serializer
        if reducer is not None:
            dat["reducer"] = reducer
        if storage is not None:
            dat["storage"] = storage

        self._items.append((fut, des, dat))  # type: ignore[reportArgumentType]
        return fut


DataSaver: TypeAlias = _DataSaver
"""Defines a protocol for saving data."""


async def _save(
    items: Sequence[_SaverItem],
    session: AsyncSession,
    storage_registry: StorageRegistry,
    serializer_registry: SerializerRegistry,
    reducer_registry: ReducerRegistry,
    retries,
) -> Sequence[DataDescriptor]:
    """Save the given data to the database."""
    relation_futures: list[TaskGroupFuture[DataDescriptor]] = []
    try:
        async with create_task_group() as tg:
            for fut, des, dat in items:
                if "stream" in dat:
                    start_given_future(
                        tg,
                        fut,
                        _save_stream,
                        des,
                        dat,
                        storage_registry,
                        serializer_registry,
                        reducer_registry,
                    )
                else:
                    start_given_future(
                        tg,
                        fut,
                        _save_value,
                        des,
                        dat,
                        storage_registry,
                        serializer_registry,
                        reducer_registry,
                    )
        return [f.result() for f in relation_futures]
    finally:
        relations = [r for f in relation_futures if (r := f.result(default=None)) is not None]
        await _save_relations(session, relations, retries=retries)


async def _save_value(
    descriptor: DataDescriptor,
    data: _ValueData,
    storage_registry: StorageRegistry,
    serializer_registry: SerializerRegistry,
    reducer_registry: ReducerRegistry,
) -> DataDescriptor:
    match data:
        case {"value": value, "serializer": serializer, "storage": storage}:
            reduction = {None: serializer.dump_value(value)}
        case {"value": value, "reducer": reducer, "storage": storage}:
            serializer = None
        case {"value": value, "serializer": serializer}:
            storage = storage_registry.infer_from_data_relation_type(type(descriptor))
        case {"value": value, "reducer": reducer}:
            storage = storage_registry.infer_from_data_relation_type(type(descriptor))
        case {"value": value, "storage": storage}:
            value_type = type(value)
            if (reducer := reducer_registry.infer_from_type(type, missing_ok=True)) is None:
                serializer = serializer_registry.infer_from_value_type(type(value))
        case {"value": value}:
            storage = storage_registry.infer_from_data_relation_type(type(descriptor))
            serializer = serializer_registry.infer_from_value_type(type(value))
        case _:
            msg = f"Invalid data dictionary: {data}"
            raise ValueError(msg)

    dump = serializer.dump_value(value)
    digest = _make_value_dump_digest(dump)

    descriptor.rel_content_encoding = dump.get("content_encoding")
    descriptor.rel_content_hash = digest["content_hash"]
    descriptor.rel_content_hash_algorithm = digest["content_hash_algorithm"]
    descriptor.rel_content_size = len(dump["content_value"])
    descriptor.rel_content_type = dump["content_type"]
    descriptor.rel_serializer_name = dump["serializer_name"]
    descriptor.rel_serializer_version = dump["serializer_version"]
    descriptor.rel_storage_name = storage.name
    descriptor.rel_storage_version = storage.version

    return await storage.put_value(descriptor, dump["content_value"], digest)


async def _save_stream(
    descriptor: DataDescriptor,
    data: _StreamData,
    storage_registry: StorageRegistry,
    serializer_registry: SerializerRegistry,
    reducer_registry: ReducerRegistry,
) -> DataDescriptor:
    match data:
        case {"stream": stream, "serializer": serializer, "storage": storage}:
            pass
        case {"stream": stream, "serializer": serializer}:
            storage = storage_registry.infer_from_data_relation_type(type(descriptor), stream=True)
        case {"stream": stream, "storage": storage, "type": type_}:
            serializer = serializer_registry.infer_from_stream_type(type_)
        case {"stream": stream, "type": type_}:
            serializer = serializer_registry.infer_from_stream_type(type_)
            storage = storage_registry.infer_from_data_relation_type(type(descriptor), stream=True)
        case _:
            msg = f"Invalid data dictionary: {data}"
            raise ValueError(msg)

    dump = serializer.dump_stream(stream)
    stream, get_digest = _wrap_stream_dump(descriptor, dump)

    descriptor.rel_content_encoding = dump.get("content_encoding")
    descriptor.rel_content_type = dump["content_type"]
    descriptor.rel_serializer_name = dump["serializer_name"]
    descriptor.rel_serializer_version = dump["serializer_version"]
    descriptor.rel_storage_name = storage.name
    descriptor.rel_storage_version = storage.version

    async with aclosing(stream):
        await storage.put_stream(descriptor, stream, get_digest)

    try:
        get_digest()
    except RuntimeError:
        msg = f"Storage {storage.name} did not fully consume the stream for relation {descriptor}."
        raise RuntimeError(msg) from None

    return descriptor


async def _save_relations(
    session: AsyncSession,
    descriptor: Sequence[DataDescriptor],
    retries: int,
) -> None:
    stop = stop_after_attempt(retries)
    update_existing_stmt = update(DataDescriptor).values(
        {DataDescriptor.rel_archived_at: func.now()}
    )
    if descriptor:
        update_existing_stmt = update_existing_stmt.where(
            or_(*(r.data_where_latest() for r in descriptor))
        )
    async for attempt in AsyncRetrying(stop=stop):
        with attempt:
            try:
                async with session.begin_nested():
                    await session.execute(update_existing_stmt)
                    session.add_all(descriptor)
                    await session.commit()
            except IntegrityError:
                pass


class _ValueData(Generic[T, D], TypedDict, total=False):
    value: Required[T]
    serializer: ValueSerializer[T]
    compositor: Compositor[T]
    storage: ValueStorage[D]


class _StreamData(Generic[T, D], TypedDict, total=False):
    type: Required[type[T]]
    stream: Required[AsyncIterable[T]]
    serializer: StreamSerializer[T]
    storage: StreamStorage[D]


def _wrap_stream_dump(
    descriptor: DataDescriptor,
    dump: StreamDump,
) -> tuple[AsyncGenerator[bytes], GetStreamDigest]:
    stream = dump["content_stream"]

    content_hash = sha256()
    content_size = 0
    is_complete = False

    async def wrapper() -> AsyncGenerator[bytes]:
        nonlocal is_complete, content_size
        try:
            async with aclosing(stream):
                async for chunk in stream:
                    content_hash.update(chunk)
                    content_size += len(chunk)
                    yield chunk
        finally:
            descriptor.rel_content_hash = content_hash.hexdigest()
            descriptor.rel_content_hash_algorithm = content_hash.name
            descriptor.rel_content_size = content_size
        is_complete = True

    def get_digest(*, allow_incomplete: bool = False) -> StreamDigest:
        if not allow_incomplete and not is_complete:
            msg = "The stream has not been fully read."
            raise ValueError(msg)
        return {
            "content_encoding": dump.get("content_encoding"),
            "content_hash": content_hash.hexdigest(),
            "content_hash_algorithm": content_hash.name,
            "content_size": content_size,
            "content_type": dump["content_type"],
            "is_complete": is_complete,
        }

    return wrapper(), get_digest


def _make_value_dump_digest(dump: ValueDump) -> ValueDigest:
    value = dump["content_value"]
    content_hash = sha256(value)
    return {
        "content_encoding": dump.get("content_encoding"),
        "content_hash": content_hash.hexdigest(),
        "content_hash_algorithm": content_hash.name,
        "content_size": len(value),
        "content_type": dump["content_type"],
    }
