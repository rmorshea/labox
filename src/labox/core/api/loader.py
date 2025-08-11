from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from contextlib import suppress
from typing import TYPE_CHECKING
from typing import Any
from typing import TypeAlias
from typing import TypeVar
from typing import overload

from anyio import EndOfStream
from anyio import WouldBlock
from anyio import create_memory_object_stream
from anyio import create_task_group
from anysync import contextmanager
from sqlalchemy import inspect as orm_inspect
from sqlalchemy import select

from labox.common.anyio import TaskFuture
from labox.common.anyio import start_future
from labox.common.anyio import start_with_future
from labox.common.exceptions import NotRegistered
from labox.core.database import ContentRecord
from labox.core.database import ManifestRecord
from labox.core.database import SerializerTypeEnum
from labox.core.serializer import StreamSerializer
from labox.core.storable import Storable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from contextlib import AsyncExitStack
    from uuid import UUID

    from anyio.abc import TaskGroup
    from anyio.streams.memory import MemoryObjectReceiveStream
    from anyio.streams.memory import MemoryObjectSendStream
    from sqlalchemy.ext.asyncio import AsyncSession

    from labox.core.registry import Registry
    from labox.core.unpacker import UnpackedValue
    from labox.core.unpacker import UnpackedValueStream

T = TypeVar("T")
S = TypeVar("S", bound=Storable)

_ContentRequest = tuple[TaskFuture, ManifestRecord, type[Any] | None]
_StorableRequest = tuple[TaskFuture, ManifestRecord, type[Any] | None, Sequence[ContentRecord]]


async def load_one(
    manifest: ManifestRecord,
    cls: type[S],
    *,
    registry: Registry,
    session: AsyncSession,
) -> S:
    """Load a single object from the given manifest record."""
    async with new_loader(registry, session) as loader:
        future = loader.load_soon(manifest, cls)
    return future.value


@contextmanager
async def new_loader(
    registry: Registry,
    session: AsyncSession,
    context: AsyncExitStack | None = None,
) -> AsyncIterator[DataLoader]:
    """Create a context manager for saving data."""
    content_req_sender, content_req_receiver = create_memory_object_stream[_ContentRequest]()
    storable_req_sender, storable_req_receiver = create_memory_object_stream[_StorableRequest]()
    async with create_task_group() as handler_tg:
        with content_req_sender:
            handler_tg.start_soon(
                _handle_content_requests,
                session,
                content_req_receiver,
                storable_req_sender,
            )
            handler_tg.start_soon(
                _handle_storable_requests,
                storable_req_receiver,
                registry,
                context,
            )
            async with create_task_group() as loader_tg:
                yield _DataLoader(loader_tg, content_req_sender)


DataLoader: TypeAlias = "_DataLoader"
"""Defines a protocol for saving data."""


async def load_manifest_record(
    manifest: ManifestRecord,
    contents: Sequence[ContentRecord],
    /,
    *,
    registry: Registry,
    context: AsyncExitStack | None = None,
) -> Storable:
    """Load an object from the given manifest record."""
    unpacker = registry.get_unpacker(manifest.unpacker_name)
    cls = registry.get_storable(manifest.class_id)

    content_futures: dict[str, TaskFuture[Any]] = {}
    async with create_task_group() as tg:
        for c in contents:
            content_futures[c.content_key] = start_future(
                tg,
                load_content_record,
                c,
                registry=registry,
                context=context,
            )

    return unpacker.repack_object(
        cls,
        {i: f.value for i, f in content_futures.items()},
        registry,
    )


async def load_content_record(
    record: ContentRecord,
    *,
    registry: Registry,
    context: AsyncExitStack | None = None,
) -> UnpackedValue | UnpackedValueStream:
    """Load the given content from the given record."""
    storage = registry.get_storage(record.storage_name)
    storage_data = storage.deserialize_config(record.storage_config)
    match record.serializer_type:
        case SerializerTypeEnum.Serializer:
            serializer = registry.get_serializer(record.serializer_name)
            value = serializer.deserialize_data(
                {
                    "config": serializer.deserialize_config(record.serializer_config),
                    "content_encoding": record.content_encoding,
                    "content_type": record.content_type,
                    "data": await storage.read_data(storage_data),
                }
            )
            return {
                "value": value,
                "serializer": serializer,
                "storage": storage,
            }
        case SerializerTypeEnum.StreamSerializer:
            if context is None:
                msg = (
                    "Attempted to load stream without a `context` argument - this gives the user "
                    "responsibility and control over when underlying async generators are closed."
                )
                raise ValueError(msg)
            serializer = registry.get_stream_serializer(record.serializer_name)
            if not isinstance(serializer, StreamSerializer):
                msg = f"Content {record.id} expects a stream serializer, got {serializer}."
                raise TypeError(msg)

            data_stream = storage.read_data_stream(storage_data)
            # user needs to ensure the stream is closed when done
            context.push_async_callback(data_stream.aclose)

            stream = serializer.deserialize_data_stream(
                {
                    "config": serializer.deserialize_config(record.serializer_config),
                    "content_encoding": record.content_encoding,
                    "content_type": record.content_type,
                    "data_stream": storage.read_data_stream(storage_data),
                }
            )
            # user needs to ensure the stream is closed when done
            context.push_async_callback(stream.aclose)

            return {
                "value_stream": stream,
                "serializer": serializer,
                "storage": storage,
            }
        case serializer_type:  # nocov
            msg = f"Unknown serializer type: {serializer_type}"
            raise ValueError(msg)


class _DataLoader:
    def __init__(
        self, tg: TaskGroup, request_sender: MemoryObjectSendStream[_ContentRequest]
    ) -> None:
        self._tg = tg
        self._request_sender = request_sender

    @overload
    def load_soon(
        self,
        manifest: ManifestRecord,
        cls: type[S],
        /,
    ) -> TaskFuture[S]: ...

    @overload
    def load_soon(
        self,
        manifest: ManifestRecord,
        cls: None = ...,
        /,
    ) -> TaskFuture[Any]: ...

    def load_soon(
        self,
        manifest: ManifestRecord,
        cls: type[Any] | None = None,
        /,
    ) -> TaskFuture[Any]:
        """Load an object from the given manifest record."""
        future = TaskFuture()
        self._tg.start_soon(self._request_sender.send, (future, manifest, cls))
        return future


async def _handle_content_requests(
    session: AsyncSession,
    receive_stream: MemoryObjectReceiveStream[_ContentRequest],
    send_stream: MemoryObjectSendStream[_StorableRequest],
) -> None:
    with suppress(EndOfStream), send_stream, receive_stream:
        while True:
            requests = await _exhaust_stream(receive_stream)
            futures, manifests, classes = zip(*requests, strict=True)
            record_groups = await _load_content_records(session, manifests)
            for fut, cls, (man, con) in zip(futures, classes, record_groups, strict=True):
                await send_stream.send((fut, man, cls, con))


async def _handle_storable_requests(
    receive_stream: MemoryObjectReceiveStream[_StorableRequest],
    registry: Registry,
    context: AsyncExitStack | None = None,
) -> None:
    with receive_stream:
        async with create_task_group() as tg:
            async for fut, manifest, expected_cls, contents in receive_stream:
                try:
                    actual_cls = registry.get_storable(manifest.class_id)
                except NotRegistered as error:
                    fut.set_exception(error)
                    continue

                if expected_cls and not issubclass(actual_cls, expected_cls):
                    msg = f"Expected {expected_cls}, but {manifest} is {actual_cls}."
                    fut.set_exception(TypeError(msg))
                    continue

                start_with_future(
                    tg,
                    fut,
                    load_manifest_record,
                    manifest,
                    contents,
                    registry=registry,
                    context=context,
                )


async def _load_content_records(
    session: AsyncSession,
    manifests: Sequence[ManifestRecord],
) -> Sequence[tuple[ManifestRecord, Sequence[ContentRecord]]]:
    """Load the content records for the given manifest records."""
    missing: list[ManifestRecord] = []
    present: list[tuple[ManifestRecord, Sequence[ContentRecord]]] = []
    for m in manifests:
        if "contents" in orm_inspect(m).unloaded:
            missing.append(m)
        else:
            present.append((m, m.contents))

    if not missing:
        return present

    stmt = select(ContentRecord).where(ContentRecord.manifest_id.in_([m.id for m in missing]))
    contents_by_manifest_id: defaultdict[UUID, list[ContentRecord]] = defaultdict(list)
    for record in await session.scalars(stmt):
        contents_by_manifest_id[record.manifest_id].append(record)

    return present + [(m, contents_by_manifest_id[m.id]) for m in missing]


async def _exhaust_stream(stream: MemoryObjectReceiveStream[T]) -> Sequence[T]:
    try:
        return [stream.receive_nowait()]
    except WouldBlock:
        first_item = await stream.receive()
    items = [first_item]
    while True:
        try:
            item = stream.receive_nowait()
        except WouldBlock:
            break
        items.append(item)
    return items
