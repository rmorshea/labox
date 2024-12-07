from __future__ import annotations

from hashlib import sha256
from typing import TYPE_CHECKING
from typing import Generic
from typing import ParamSpec
from typing import Required
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

from ardex.core.context import DatabaseSession
from ardex.core.schema import DataRelation
from ardex.core.serializer import ScalarSerializerRegistry
from ardex.core.serializer import StreamSerializerRegistry
from ardex.core.storage import StorageRegistry
from ardex.utils.anyio import TaskGroupFuture
from ardex.utils.anyio import start_future

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator
    from collections.abc import Callable
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from ardex.core.serializer import ScalarDump
    from ardex.core.serializer import ScalarSerializer
    from ardex.core.serializer import StreamDump
    from ardex.core.serializer import StreamSerializer
    from ardex.core.storage import DumpDigest
    from ardex.core.storage import DumpDigestGetter
    from ardex.core.storage import Storage


T = TypeVar("T")
R = TypeVar("R", bound=DataRelation)
P = ParamSpec("P")

_COMMIT_RETRIES = 3


@contextmanager
@injector.asynciterator(
    requires=(
        DatabaseSession,
        StorageRegistry,
        StreamSerializerRegistry,
        ScalarSerializerRegistry,
    )
)
async def data_saver(
    *,
    database_session: DatabaseSession | AsyncSession = required,
    storage_registry: StorageRegistry = required,
    stream_serializer_registry: StreamSerializerRegistry = required,
    scalar_serializer_registry: ScalarSerializerRegistry = required,
) -> AsyncIterator[_DataSaver]:
    """Create a context manager for saving data."""
    items: list[tuple[TaskGroupFuture[DataRelation], DataRelation, _ScalarData | _StreamData]] = []

    yield _DataSaver(items)

    await _save_data(
        items,
        database_session,
        storage_registry,
        stream_serializer_registry,
        scalar_serializer_registry,
        _COMMIT_RETRIES,
    )


class _DataSaver:
    """Defines a protocol for saving data."""

    def __init__(
        self,
        items: list[tuple[TaskGroupFuture[DataRelation], DataRelation, _ScalarData | _StreamData]],
    ) -> None:
        self._items = items

    def scalar(
        self,
        relation: Callable[P, R],
        scalar: T,
        serializer: ScalarSerializer[T] | None = None,
        storage: Storage[R] | None = None,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskGroupFuture[R]:
        """Save the given scalar data."""
        fut = TaskGroupFuture[R]()
        rel = relation(*args, **kwargs)
        dat: _ScalarData = {"scalar": scalar}
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
        storage: Storage[R] | None = ...,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskGroupFuture[R]: ...

    @overload
    def stream(
        self,
        relation: Callable[P, R],
        stream: AsyncIterable[T],
        serializer: StreamSerializer[T],
        storage: Storage[R] | None = None,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskGroupFuture[R]: ...

    def stream(
        self,
        relation: Callable[P, R],
        stream: AsyncIterable[T] | tuple[type[T], AsyncIterable[T]],
        serializer: StreamSerializer[T] | None = None,
        storage: Storage[R] | None = None,
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


async def _save_data(
    items: Sequence[tuple[TaskGroupFuture[DataRelation], DataRelation, _ScalarData | _StreamData]],
    session: AsyncSession,
    storage_registry: StorageRegistry,
    stream_serializer_registry: StreamSerializerRegistry,
    scalar_serializer_registry: ScalarSerializerRegistry,
    retries,
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
        await _save_relations(session, relations, retries=retries)


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
    digest = _make_scalar_dump_digest(dump)

    relation.rel_content_type = dump["content_type"]
    relation.rel_serializer_name = dump["serializer_name"]
    relation.rel_serializer_version = dump["serializer_version"]
    relation.rel_storage_name = storage.name
    relation.rel_storage_version = storage.version

    relation = await storage.write_scalar(relation, dump["content_scalar"], digest)

    relation.rel_content_size = len(dump["content_scalar"])
    relation.rel_content_hash = digest["content_hash"]
    relation.rel_content_hash_algorithm = digest["content_hash_algorithm"]

    return relation


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

    dump = serializer.dump_stream(stream)
    stream, get_digest = _make_stream_dump_digest_getter(dump)

    relation.rel_storage_name = storage.name
    relation.rel_storage_version = storage.version
    relation.rel_content_type = dump["content_type"]
    relation.rel_serializer_name = dump["serializer_name"]
    relation.rel_serializer_version = dump["serializer_version"]

    await storage.write_stream(relation, stream, get_digest)

    try:
        digest = get_digest()
    except RuntimeError:
        msg = f"Storage {storage.name} did not fully consume the stream for relation {relation}."
        raise RuntimeError(msg) from None

    relation.rel_content_size = digest["content_size"]
    relation.rel_content_hash = digest["content_hash"]
    relation.rel_content_hash_algorithm = digest["content_hash_algorithm"]

    return relation


async def _save_relations(
    session: AsyncSession,
    relations: Sequence[DataRelation],
    retries: int,
) -> None:
    stop = stop_after_attempt(retries)
    update_existing_stmt = (
        update(DataRelation)
        .values({DataRelation.rel_archived_at: func.now()})
        .where(or_(*(r.rel_select_latest() for r in relations)))
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


def _make_stream_dump_digest_getter(
    dump: StreamDump,
) -> tuple[AsyncIterator[bytes], DumpDigestGetter]:
    stream = dump["content_stream"]

    content_hash = sha256()
    size = 0
    done = False

    async def wrapper() -> AsyncIterator[bytes]:
        nonlocal done, size
        async for chunk in stream:
            content_hash.update(chunk)
            size += len(chunk)
            yield chunk
        done = True

    def digest() -> DumpDigest:
        if not done:
            msg = "The stream has not been fully consumed."
            raise RuntimeError(msg)

        return {
            "content_hash": content_hash.hexdigest(),
            "content_hash_algorithm": content_hash.name,
            "content_size": size,
            "content_type": dump["content_type"],
        }

    return wrapper(), digest


def _make_scalar_dump_digest(dump: ScalarDump) -> DumpDigest:
    scalar = dump["content_scalar"]
    content_hash = sha256(scalar)
    return {
        "content_hash": content_hash.hexdigest(),
        "content_hash_algorithm": content_hash.name,
        "content_size": len(scalar),
        "content_type": dump["content_type"],
    }
