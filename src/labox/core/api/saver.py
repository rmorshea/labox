from __future__ import annotations

from contextlib import AsyncExitStack
from contextlib import aclosing
from hashlib import sha256
from inspect import isasyncgen
from logging import getLogger
from typing import TYPE_CHECKING
from typing import Any
from typing import ParamSpec
from typing import TypeAlias
from typing import TypeVar
from uuid import UUID
from uuid import uuid4

from anyio import create_task_group
from anysync import contextmanager

from labox.common.anyio import TaskFuture
from labox.common.anyio import start_future
from labox.core.database import ContentRecord
from labox.core.database import ManifestRecord
from labox.core.database import SerializerTypeEnum

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator
    from collections.abc import Mapping

    from anyio.abc import TaskGroup
    from sqlalchemy.ext.asyncio import AsyncSession

    from labox.common.types import TagMap
    from labox.core.registry import Registry
    from labox.core.serializer import SerializedData
    from labox.core.serializer import SerializedDataStream
    from labox.core.serializer import Serializer
    from labox.core.serializer import StreamSerializer
    from labox.core.storable import Storable
    from labox.core.storage import Digest
    from labox.core.storage import GetStreamDigest
    from labox.core.storage import Storage
    from labox.core.storage import StreamDigest
    from labox.core.unpacker import AnyUnpackedValue


P = ParamSpec("P")
D = TypeVar("D", bound=ManifestRecord)


_LOG = getLogger(__name__)


async def save_one(
    obj: Storable,
    *,
    tags: Mapping[str, str] | None = None,
    registry: Registry,
    session: AsyncSession,
) -> ManifestRecord:
    """Save a single object to the database."""
    async with new_saver(registry, session) as saver:
        future = saver.save_soon(obj, tags=tags)
    return future.value


@contextmanager
async def new_saver(
    registry: Registry,
    session: AsyncSession,
) -> AsyncIterator[DataSaver]:
    """Create a context manager for saving data."""
    futures: list[TaskFuture[ManifestRecord]] = []
    try:
        async with create_task_group() as tg:
            yield _DataSaver(tg, futures, registry)
    finally:
        errors: list[BaseException] = []
        manifests: list[ManifestRecord] = []
        for f in futures:
            if e := f.exception:
                errors.append(e)
            else:
                m = f.value
                _LOG.debug("Saving manifest %s", m.id.hex)
                manifests.append(m)

        session.add_all(manifests)
        await session.commit()

        if errors:
            msg = f"Failed to save {len(errors)} out of {len(futures)} items."
            raise BaseExceptionGroup(msg, errors)


class _DataSaver:
    def __init__(
        self,
        task_group: TaskGroup,
        futures: list[TaskFuture[ManifestRecord]],
        registry: Registry,
    ):
        self._futures = futures
        self._task_group = task_group
        self._registry = registry

    def save_soon(
        self,
        obj: Storable,
        *,
        tags: Mapping[str, str] | None = None,
    ) -> TaskFuture[ManifestRecord]:
        """Schedule the object to be saved."""
        self._registry.has_storable(type(obj), raise_if_missing=True)

        future = start_future(
            self._task_group,
            _save_object,
            obj,
            tags or {},
            self._registry,
        )
        self._futures.append(future)

        return future


DataSaver: TypeAlias = _DataSaver
"""Defines a protocol for saving data."""


async def _save_object(
    obj: Storable,
    tags: TagMap,
    registry: Registry,
) -> ManifestRecord:
    """Save the given data to the database."""
    cls = obj.__class__
    cfg = cls.storable_config()
    unpacker = cfg.unpacker or registry.infer_unpacker(cls)
    obj_contents: Mapping[str, AnyUnpackedValue] = unpacker.unpack_object(obj, registry)

    manifest_id = uuid4()
    data_record_futures: list[TaskFuture[ContentRecord]] = []
    async with create_task_group() as tg:
        for content_keyw, content in obj_contents.items():
            _LOG.debug("Saving %s in manifest %s", content_keyw, manifest_id.hex)
            match content:
                case {"value": content_value}:
                    data_record_futures.append(
                        start_future(
                            tg,
                            _save_storage_value,
                            tags,
                            manifest_id,
                            content_keyw,
                            content_value,
                            content.get("serializer"),
                            content.get("storage"),
                            registry,
                        )
                    )
                case {"value_stream": content_value_stream}:
                    data_record_futures.append(
                        start_future(
                            tg,
                            _save_storage_stream,
                            tags,
                            manifest_id,
                            content_keyw,
                            content_value_stream,
                            content.get("serializer"),
                            content.get("storage"),
                            registry,
                        )
                    )
                case _:
                    msg = f"Invalid manifest {content_keyw!r} in {obj!r} - {content}"
                    raise AssertionError(msg)

    contents: list[ContentRecord] = []
    for k, f in zip(obj_contents, data_record_futures, strict=False):
        if exc := f.exception:
            _LOG.error(
                "Failed to save %s in manifest %s",
                k,
                manifest_id.hex,
                exc_info=(exc.__class__, exc, exc.__traceback__),
            )
        contents.append(f.value)

    return ManifestRecord(
        id=manifest_id,
        tags=tags,
        class_id=cfg.class_id,
        contents=contents,
        unpacker_name=unpacker.name,
    )


async def _save_storage_value(
    tags: TagMap,
    manifest_id: UUID,
    content_key: str,
    value: Any,
    serializer: Serializer | None,
    storage: Storage | None,
    registry: Registry,
) -> ContentRecord:
    storage = storage or registry.get_default_storage()
    serializer = serializer or registry.get_serializer_by_type(type(value))
    content = serializer.serialize_data(value)
    digest = _make_value_dump_digest(content)
    storage_data = await storage.write_data(content["data"], digest, tags)
    return ContentRecord(
        content_encoding=content["content_encoding"],
        content_hash_algorithm=digest["content_hash_algorithm"],
        content_hash=digest["content_hash"],
        content_key=content_key,
        content_size=digest["content_size"],
        content_type=content["content_type"],
        manifest_id=manifest_id,
        serializer_config=serializer.serialize_config(content.get("config")),
        serializer_name=serializer.name,
        serializer_type=SerializerTypeEnum.Serializer,
        storage_config=storage.serialize_config(storage_data),
        storage_name=storage.name,
    )


async def _save_storage_stream(
    tags: TagMap,
    manifest_id: UUID,
    content_key: str,
    stream: AsyncIterable[Any],
    serializer: StreamSerializer | None,
    storage: Storage | None,
    registry: Registry,
) -> ContentRecord:
    storage = storage or registry.get_default_storage()
    async with AsyncExitStack() as stack:
        if isasyncgen(stream):
            await stack.enter_async_context(aclosing(stream))

        if serializer is None:
            stream_iter = aiter(stream)
            first_value = await anext(stream_iter)
            serializer = registry.get_stream_serializer_by_type(type(first_value))
            stream = _continue_stream(first_value, stream_iter)

        content = serializer.serialize_data_stream(stream)
        byte_stream, get_digest = _wrap_stream_dump(content)
        storage_data = await storage.write_data_stream(byte_stream, get_digest, tags)
        try:
            digest = get_digest()
        except ValueError:
            msg = f"Storage {storage.name!r} did not fully consume the data stream."
            raise RuntimeError(msg) from None
        return ContentRecord(
            content_encoding=content["content_encoding"],
            content_hash_algorithm=digest["content_hash_algorithm"],
            content_hash=digest["content_hash"],
            content_key=content_key,
            content_size=digest["content_size"],
            content_type=content["content_type"],
            manifest_id=manifest_id,
            serializer_config=serializer.serialize_config(content.get("config")),
            serializer_name=serializer.name,
            serializer_type=SerializerTypeEnum.StreamSerializer,
            storage_config=storage.serialize_config(storage_data),
            storage_name=storage.name,
        )
    raise AssertionError  # nocov


def _wrap_stream_dump(
    archive: SerializedDataStream,
) -> tuple[AsyncGenerator[bytes], GetStreamDigest]:
    data_stream = archive["data_stream"]

    content_hash = sha256()
    content_size = 0
    is_complete = False

    async def wrapper() -> AsyncGenerator[bytes]:
        nonlocal is_complete, content_size
        async with aclosing(data_stream):
            async for chunk in data_stream:
                content_hash.update(chunk)
                content_size += len(chunk)
                yield chunk
        is_complete = True

    def get_digest(*, allow_incomplete: bool = False) -> StreamDigest:
        if not allow_incomplete and not is_complete:
            msg = "The stream has not been fully read."
            raise ValueError(msg)
        return {
            "content_encoding": archive.get("content_encoding"),
            "content_hash": content_hash.hexdigest(),
            "content_hash_algorithm": content_hash.name,
            "content_size": content_size,
            "content_type": archive["content_type"],
            "is_complete": is_complete,
        }

    return wrapper(), get_digest


def _make_value_dump_digest(archive: SerializedData) -> Digest:
    data = archive["data"]
    content_hash = sha256(data)
    return {
        "content_encoding": archive.get("content_encoding"),
        "content_hash": content_hash.hexdigest(),
        "content_hash_algorithm": content_hash.name,
        "content_size": len(data),
        "content_type": archive["content_type"],
    }


async def _continue_stream(first_value: Any, stream: AsyncIterable[Any]) -> AsyncGenerator[Any]:
    yield first_value
    async for cont_value in stream:
        yield cont_value
