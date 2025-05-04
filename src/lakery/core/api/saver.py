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

from lakery.common.anyio import FutureResult
from lakery.common.anyio import start_future
from lakery.core.schema import ContentRecord
from lakery.core.schema import ManifestRecord
from lakery.core.schema import SerializerTypeEnum

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator
    from collections.abc import Mapping

    from anyio.abc import TaskGroup
    from sqlalchemy.ext.asyncio import AsyncSession

    from lakery.common.utils import TagMap
    from lakery.core.model import BaseStorageModel
    from lakery.core.registries import Registries
    from lakery.core.serializer import Content
    from lakery.core.serializer import Serializer
    from lakery.core.serializer import SerializerRegistry
    from lakery.core.serializer import StreamContent
    from lakery.core.serializer import StreamSerializer
    from lakery.core.storage import Digest
    from lakery.core.storage import GetStreamDigest
    from lakery.core.storage import Storage
    from lakery.core.storage import StreamDigest


T = TypeVar("T")
P = ParamSpec("P")
D = TypeVar("D", bound=ManifestRecord)


_LOG = getLogger(__name__)


@contextmanager
async def data_saver(
    *,
    registries: Registries,
    session: AsyncSession,
) -> AsyncIterator[DataSaver]:
    """Create a context manager for saving data."""
    futures: list[FutureResult[ManifestRecord]] = []
    try:
        async with create_task_group() as tg:
            yield _DataSaver(tg, futures, registries)
    finally:
        errors: list[BaseException] = []
        manifests: list[ManifestRecord] = []
        for f in futures:
            if e := f.exception():
                errors.append(e)
            else:
                m = f.result()
                _LOG.debug("Saving manifest %s", m.id.hex)
                manifests.append(m)

        async with session.begin():
            session.add_all(manifests)
            for m in manifests:
                session.expunge(m)
                for c in m.contents:
                    session.expunge(c)

        if errors:
            msg = f"Failed to save {len(errors)} out of {len(futures)} items."
            raise BaseExceptionGroup(msg, errors)


class _DataSaver:
    def __init__(
        self,
        task_group: TaskGroup,
        futures: list[FutureResult[ManifestRecord]],
        registries: Registries,
    ):
        self._futures = futures
        self._task_group = task_group
        self._registries = registries

    def save_soon(
        self,
        model: BaseStorageModel,
        *,
        tags: Mapping[str, str] | None = None,
    ) -> FutureResult[ManifestRecord]:
        """Schedule the given model to be saved."""
        self._registries.models.check_registered(type(model))

        future = start_future(
            self._task_group,
            _save_model,
            model,
            tags or {},
            self._registries,
        )
        self._futures.append(future)

        return future


DataSaver: TypeAlias = _DataSaver
"""Defines a protocol for saving data."""


async def _save_model(
    model: BaseStorageModel,
    tags: TagMap,
    registries: Registries,
) -> ManifestRecord:
    """Save the given data to the database."""
    model_uuid = model.storage_model_id()
    model_manifests = model.storage_model_dump(registries)

    manifest_id = uuid4()
    data_record_futures: list[FutureResult[ContentRecord]] = []
    async with create_task_group() as tg:
        for manifest_key, manifest in model_manifests.items():
            _LOG.debug("Saving %s in manifest %s", manifest_key, manifest_id.hex)
            if "value" in manifest:
                data_record_futures.append(
                    start_future(
                        tg,
                        _save_storage_value,
                        tags,
                        manifest_id,
                        manifest_key,
                        manifest["value"],
                        manifest.get("serializer"),
                        manifest.get("storage"),
                        registries,
                    )
                )
            elif "stream" in manifest:
                data_record_futures.append(
                    start_future(
                        tg,
                        _save_storage_stream,
                        tags,
                        manifest_id,
                        manifest_key,
                        manifest["stream"],
                        manifest.get("serializer"),
                        manifest.get("storage"),
                        registries,
                    )
                )
            else:
                msg = f"Invalid manifest {manifest_key!r} in {model!r} - {manifest}"
                raise AssertionError(msg)

    contents: list[ContentRecord] = []
    for k, f in zip(model_manifests, data_record_futures, strict=False):
        if exc := f.exception():
            _LOG.error(
                "Failed to save %s in manifest %s",
                k,
                manifest_id.hex,
                exc_info=(exc.__class__, exc, exc.__traceback__),
            )
        contents.append(f.result())

    return ManifestRecord(
        id=manifest_id,
        tags=tags,
        model_id=model_uuid,
        contents=contents,
    )


async def _save_storage_value(
    tags: TagMap,
    manifest_id: UUID,
    manifest_key: str,
    value: Any,
    serializer: Serializer | None,
    storage: Storage | None,
    registries: Registries,
) -> ContentRecord:
    storage = storage or registries.storages.default
    serializer = serializer or registries.serializers.infer_from_value_type(type(value))
    content = serializer.dump(value)
    digest = _make_value_dump_digest(content)
    storage_data = await storage.put_data(content["data"], digest, tags)
    return ContentRecord(
        content_encoding=content["content_encoding"],
        content_hash=digest["content_hash"],
        content_hash_algorithm=digest["content_hash_algorithm"],
        content_size=digest["content_size"],
        content_type=content["content_type"],
        manifest_id=manifest_id,
        manifest_key=manifest_key,
        serializer_name=serializer.name,
        serializer_type=SerializerTypeEnum.Content,
        storage_data=storage_data,
        storage_name=storage.name,
    )


async def _save_storage_stream(
    tags: TagMap,
    manifest_id: UUID,
    manifest_key: str,
    stream: AsyncIterable[Any],
    serializer: StreamSerializer | None,
    storage: Storage | None,
    registries: Registries,
) -> ContentRecord:
    storage = storage or registries.storages.default
    async with AsyncExitStack() as stack:
        if isasyncgen(stream):
            await stack.enter_async_context(aclosing(stream))

        if serializer is None:
            stream_iter = aiter(stream)
            first_value = await anext(stream_iter)
            serializer = registries.serializers.infer_from_stream_type(type(first_value))
            stream = _continue_stream(first_value, stream_iter)

        content = serializer.dump_stream(stream)
        byte_stream, get_digest = _wrap_stream_dump(content)
        storage_data = await storage.put_data_stream(byte_stream, get_digest, tags)
        try:
            digest = get_digest()
        except ValueError:
            msg = f"Storage {storage.name!r} did not fully consume the data stream."
            raise RuntimeError(msg) from None
        return ContentRecord(
            content_encoding=content["content_encoding"],
            content_hash=digest["content_hash"],
            content_hash_algorithm=digest["content_hash_algorithm"],
            content_size=digest["content_size"],
            content_type=content["content_type"],
            manifest_id=manifest_id,
            manifest_key=manifest_key,
            serializer_name=serializer.name,
            serializer_type=SerializerTypeEnum.ContentStream,
            storage_data=storage_data,
            storage_name=storage.name,
        )
    raise AssertionError  # nocov


def _wrap_stream_dump(
    content: StreamContent,
) -> tuple[AsyncGenerator[bytes], GetStreamDigest]:
    data_stream = content["data_stream"]

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
            "content_encoding": content.get("content_encoding"),
            "content_hash": content_hash.hexdigest(),
            "content_hash_algorithm": content_hash.name,
            "content_size": content_size,
            "content_type": content["content_type"],
            "is_complete": is_complete,
        }

    return wrapper(), get_digest


def _make_value_dump_digest(content: Content) -> Digest:
    data = content["data"]
    content_hash = sha256(data)
    return {
        "content_encoding": content.get("content_encoding"),
        "content_hash": content_hash.hexdigest(),
        "content_hash_algorithm": content_hash.name,
        "content_size": len(data),
        "content_type": content["content_type"],
    }


class _SerializationHelper:
    def __init__(self, serializers: SerializerRegistry) -> None:
        self._serializers = serializers

    def dump(
        self,
        value: Any,
        serializer: Serializer | None,
    ) -> Content:
        if serializer is None:
            serializer = self._serializers.infer_from_value_type(type(value))
        return serializer.dump(value)

    async def dump_stream(
        self,
        stream: AsyncIterable,
        serializer: StreamSerializer | None,
    ) -> StreamContent:
        if serializer is not None:
            return serializer.dump_stream(stream)

        stream_iter = aiter(stream)
        first_value = await anext(stream_iter)
        serializer = self._serializers.infer_from_stream_type(type(first_value))
        return serializer.dump_stream(_continue_stream(first_value, stream_iter))


async def _continue_stream(first_value: Any, stream: AsyncIterable[Any]) -> AsyncGenerator[Any]:
    yield first_value
    async for cont_value in stream:
        yield cont_value
