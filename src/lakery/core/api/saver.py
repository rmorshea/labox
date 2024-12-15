from __future__ import annotations

from hashlib import sha256
from typing import TYPE_CHECKING
from typing import Generic
from typing import ParamSpec
from typing import Required
from typing import TypeAlias
from typing import TypedDict
from typing import TypeVar
from typing import overload

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

from lakery.core.context import DatabaseSession
from lakery.core.schema import DataRelation
from lakery.core.serializer import SerializerRegistry
from lakery.core.storage import GetStreamDigest
from lakery.core.storage import StorageRegistry
from lakery.core.storage import StreamDigest
from lakery.utils.anyio import TaskGroupFuture
from lakery.utils.anyio import start_given_future

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator
    from collections.abc import Callable
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from lakery.core.serializer import StreamDump
    from lakery.core.serializer import StreamSerializer
    from lakery.core.serializer import ValueDump
    from lakery.core.serializer import ValueSerializer
    from lakery.core.storage import StreamStorage
    from lakery.core.storage import ValueDigest
    from lakery.core.storage import ValueStorage


T = TypeVar("T")
R = TypeVar("R", bound=DataRelation)
P = ParamSpec("P")

_COMMIT_RETRIES = 3


@contextmanager
@injector.asynciterator(requires=(DatabaseSession, StorageRegistry, SerializerRegistry))
async def data_saver(
    *,
    database_session: DatabaseSession | AsyncSession = required,
    storage_registry: StorageRegistry = required,
    serializer_registry: SerializerRegistry = required,
) -> AsyncIterator[DataSaver]:
    """Create a context manager for saving data."""
    items: list[tuple[TaskGroupFuture[DataRelation], DataRelation, _ValueData | _StreamData]] = []

    yield DataSaver(items)

    await _save_data(
        items, database_session, storage_registry, serializer_registry, _COMMIT_RETRIES
    )


class _DataSaver:
    def __init__(
        self,
        items: list[tuple[TaskGroupFuture[DataRelation], DataRelation, _ValueData | _StreamData]],
    ) -> None:
        self._items = items

    def value(
        self,
        relation: Callable[P, R],
        value: T,
        serializer: ValueSerializer[T] | None = None,
        storage: ValueStorage[R] | None = None,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskGroupFuture[R]:
        """Save the given value data."""
        fut = TaskGroupFuture[R]()
        rel = relation(*args, **kwargs)
        dat: _ValueData = {"value": value}
        if serializer is not None:
            dat["serializer"] = serializer
        if storage is not None:
            dat["storage"] = storage
        self._items.append((fut, rel, dat))  # type: ignore[reportArgumentType]
        return fut

    @overload
    def stream(
        self,
        relation: Callable[P, R],
        stream: tuple[type[T], AsyncIterable[T]],
        serializer: StreamSerializer[T] | None = ...,
        storage: StreamStorage[R] | None = ...,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskGroupFuture[R]: ...

    @overload
    def stream(
        self,
        relation: Callable[P, R],
        stream: AsyncIterable[T],
        serializer: StreamSerializer[T],
        storage: StreamStorage[R] | None = None,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskGroupFuture[R]: ...

    def stream(
        self,
        relation: Callable[P, R],
        stream: AsyncIterable[T] | tuple[type[T], AsyncIterable[T]],
        serializer: StreamSerializer[T] | None = None,
        storage: StreamStorage[R] | None = None,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskGroupFuture[R]:
        """Save the given stream data."""
        fut = TaskGroupFuture[R]()
        rel = relation(*args, **kwargs)
        dat: _StreamData
        if serializer is None:
            match stream:
                case (type_, stream):
                    dat = {"stream": stream, "type": type_}
                case stream:
                    msg = "A serializer must be provided when the stream type is not given."
                    raise ValueError(msg)
        else:
            match stream:
                case (type_, stream):
                    msg = "The stream type must not be given when a serializer is provided."
                    raise ValueError(msg)
                case stream:
                    dat = {"stream": stream, "serializer": serializer}
        if storage is not None:
            dat["storage"] = storage
        self._items.append((fut, rel, dat))  # type: ignore[reportArgumentType]
        return fut


DataSaver: TypeAlias = _DataSaver
"""Defines a protocol for saving data."""


async def _save_data(
    items: Sequence[tuple[TaskGroupFuture[DataRelation], DataRelation, _ValueData | _StreamData]],
    session: AsyncSession,
    storage_registry: StorageRegistry,
    serializer_registry: SerializerRegistry,
    retries,
) -> Sequence[DataRelation]:
    """Save the given data to the database."""
    relation_futures: list[TaskGroupFuture[DataRelation]] = []
    try:
        async with create_task_group() as tg:
            for fut, rel, dat in items:
                if "stream" in dat:
                    start_given_future(
                        tg,
                        fut,
                        _save_stream,
                        rel,
                        dat,
                        storage_registry,
                        serializer_registry,
                    )
                else:
                    start_given_future(
                        tg,
                        fut,
                        _save_value,
                        rel,
                        dat,
                        storage_registry,
                        serializer_registry,
                    )
        return [f.result() for f in relation_futures]
    finally:
        relations = [r for f in relation_futures if (r := f.result(default=None)) is not None]
        await _save_relations(session, relations, retries=retries)


async def _save_value(
    relation: DataRelation,
    data: _ValueData,
    storage_registry: StorageRegistry,
    serializer_registry: SerializerRegistry,
) -> DataRelation:
    match data:
        case {"value": value, "serializer": serializer, "storage": storage}:
            pass
        case {"value": value, "serializer": serializer}:
            storage = storage_registry.infer_from_data_relation_type(type(relation))
        case {"value": value, "storage": storage}:
            serializer = serializer_registry.infer_from_value_type(type(value))
        case {"value": value}:
            storage = storage_registry.infer_from_data_relation_type(type(relation))
            serializer = serializer_registry.infer_from_value_type(type(value))
        case _:
            msg = f"Invalid data dictionary: {data}"
            raise ValueError(msg)

    dump = serializer.dump_value(value)
    digest = _make_value_dump_digest(dump)

    relation.rel_serializer_name = dump["serializer_name"]
    relation.rel_serializer_version = dump["serializer_version"]
    relation.rel_storage_name = storage.name
    relation.rel_storage_version = storage.version

    relation = await storage.put_value(relation, dump["value"], digest)

    relation.rel_content_encoding = dump.get("content_encoding")
    relation.rel_content_hash = digest["content_hash"]
    relation.rel_content_hash_algorithm = digest["content_hash_algorithm"]
    relation.rel_content_size = len(dump["value"])
    relation.rel_content_type = dump["content_type"]

    return relation


async def _save_stream(
    relation: DataRelation,
    data: _StreamData,
    storage_registry: StorageRegistry,
    serializer_registry: SerializerRegistry,
) -> DataRelation:
    match data:
        case {"stream": stream, "serializer": serializer, "storage": storage}:
            pass
        case {"stream": stream, "serializer": serializer}:
            storage = storage_registry.infer_from_data_relation_type(type(relation), stream=True)
        case {"stream": stream, "storage": storage, "type": type_}:
            serializer = serializer_registry.infer_from_stream_type(type_)
        case {"stream": stream, "type": type_}:
            serializer = serializer_registry.infer_from_stream_type(type_)
            storage = storage_registry.infer_from_data_relation_type(type(relation), stream=True)
        case _:
            msg = f"Invalid data dictionary: {data}"
            raise ValueError(msg)

    dump = serializer.dump_stream(stream)
    stream, get_digest = _make_stream_dump_digest_getter(dump)

    relation.rel_storage_name = storage.name
    relation.rel_storage_version = storage.version
    relation.rel_serializer_name = dump["serializer_name"]
    relation.rel_serializer_version = dump["serializer_version"]

    await storage.put_stream(relation, stream, get_digest)

    try:
        digest = get_digest()
    except RuntimeError:
        msg = f"Storage {storage.name} did not fully consume the stream for relation {relation}."
        raise RuntimeError(msg) from None

    relation.rel_content_encoding = dump.get("content_encoding")
    relation.rel_content_hash = digest["content_hash"]
    relation.rel_content_hash_algorithm = digest["content_hash_algorithm"]
    relation.rel_content_size = digest["content_size"]
    relation.rel_content_type = dump["content_type"]

    return relation


async def _save_relations(
    session: AsyncSession,
    relations: Sequence[DataRelation],
    retries: int,
) -> None:
    stop = stop_after_attempt(retries)
    update_existing_stmt = update(DataRelation).values({DataRelation.rel_archived_at: func.now()})
    if relations:
        update_existing_stmt = update_existing_stmt.where(
            or_(*(r.rel_select_latest() for r in relations))
        )
    async for attempt in AsyncRetrying(stop=stop):
        with attempt:
            try:
                async with session.begin_nested():
                    await session.execute(update_existing_stmt)
                    session.add_all(relations)
                    await session.commit()
            except IntegrityError:
                pass


class _ValueData(Generic[T, R], TypedDict, total=False):
    value: Required[T]
    serializer: ValueSerializer[R]
    storage: ValueStorage[R]


class _BaseStreamData(Generic[T, R], TypedDict, total=False):
    stream: Required[AsyncIterable[T]]
    storage: StreamStorage[R]


class _InferStreamData(_BaseStreamData[T, R]):
    type: Required[type[T]]


class _KnownStreamData(_BaseStreamData[T, R]):
    serializer: Required[StreamSerializer[T]]


_StreamData = _KnownStreamData[T, R] | _InferStreamData[T, R]


def _make_stream_dump_digest_getter(
    dump: StreamDump,
) -> tuple[AsyncGenerator[bytes], GetStreamDigest]:
    stream = dump["stream"]

    content_hash = sha256()
    size = 0
    is_complete = False

    async def wrapper() -> AsyncGenerator[bytes]:
        nonlocal is_complete, size
        async for chunk in stream:
            content_hash.update(chunk)
            size += len(chunk)
            yield chunk
        is_complete = True

    def get_digest(*, allow_incomplete: bool = False) -> StreamDigest:
        if not allow_incomplete and not is_complete:
            msg = "The stream has not been fully read."
            raise ValueError(msg)
        return {
            "content_encoding": dump.get("content_encoding"),
            "content_hash": content_hash.hexdigest(),
            "content_hash_algorithm": content_hash.name,
            "content_size": size,
            "content_type": dump["content_type"],
            "is_complete": is_complete,
        }

    return wrapper(), get_digest


def _make_value_dump_digest(dump: ValueDump) -> ValueDigest:
    value = dump["value"]
    content_hash = sha256(value)
    return {
        "content_encoding": dump.get("content_encoding"),
        "content_hash": content_hash.hexdigest(),
        "content_hash_algorithm": content_hash.name,
        "content_size": len(value),
        "content_type": dump["content_type"],
    }
