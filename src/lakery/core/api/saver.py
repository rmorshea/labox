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
from pybooster import injector
from pybooster import required
from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import and_
from tenacity import AsyncRetrying
from tenacity import stop_after_attempt

from lakery.common.anyio import FutureResult
from lakery.common.anyio import start_future
from lakery.core.context import DatabaseSession
from lakery.core.context import Registries
from lakery.core.schema import NEVER
from lakery.core.schema import SerializerTypeEnum
from lakery.core.schema import StorageContentRecord
from lakery.core.schema import StorageModelRecord

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator
    from collections.abc import Mapping
    from collections.abc import Sequence

    from anyio.abc import TaskGroup

    from lakery.common.utils import TagMap
    from lakery.core.model import BaseStorageModel
    from lakery.core.serializer import ContentDump
    from lakery.core.serializer import ContentStreamDump
    from lakery.core.serializer import Serializer
    from lakery.core.serializer import SerializerRegistry
    from lakery.core.serializer import StreamSerializer
    from lakery.core.storage import GetStreamDigest
    from lakery.core.storage import Storage
    from lakery.core.storage import StreamDigest
    from lakery.core.storage import ValueDigest


T = TypeVar("T")
P = ParamSpec("P")
D = TypeVar("D", bound=StorageModelRecord)


_COMMIT_RETRIES = 3
_LOG = getLogger(__name__)


@contextmanager
@injector.asynciterator(requires=(Registries, DatabaseSession))
async def model_saver(
    *,
    registries: Registries = required,
    session: DatabaseSession = required,
) -> AsyncIterator[ModelSaver]:
    """Create a context manager for saving data."""
    futures: list[FutureResult[StorageModelRecord]] = []
    try:
        async with create_task_group() as tg:
            yield _ModelSaver(tg, futures, registries)
    finally:
        records = [r for f in futures if (r := f.result(default=None))]
        await _save_record_groups(records, session, _COMMIT_RETRIES)


class _ModelSaver:
    def __init__(
        self,
        task_group: TaskGroup,
        futures: list[FutureResult[StorageModelRecord]],
        registries: Registries,
    ):
        self._futures = futures
        self._task_group = task_group
        self._registries = registries

    def save_soon(
        self,
        name: str,
        model: BaseStorageModel[Any],
        *,
        tags: Mapping[str, str] | None = None,
    ) -> FutureResult[StorageModelRecord]:
        """Schedule the given model to be saved."""
        self._registries.models.check_registered(type(model))

        future = start_future(
            self._task_group,
            _save_model,
            name,
            model,
            tags or {},
            self._registries,
        )
        self._futures.append(future)

        return future


ModelSaver: TypeAlias = _ModelSaver
"""Defines a protocol for saving data."""


async def _save_model(
    name: str,
    model: BaseStorageModel,
    tags: TagMap,
    registries: Registries,
) -> StorageModelRecord:
    """Save the given data to the database."""
    model_uuid = UUID(type(model).storage_model_id)
    model_spec = model.storage_model_dump(registries)

    record_id = uuid4()
    data_record_futures: list[FutureResult[StorageContentRecord]] = []
    async with create_task_group() as tg:
        for content_key, item_spec in model_spec.items():
            if "value" in item_spec:
                data_record_futures.append(
                    start_future(
                        tg,
                        _save_storage_value_spec,
                        tags,
                        record_id,
                        content_key,
                        item_spec["value"],
                        item_spec.get("serializer"),
                        item_spec.get("storage"),
                        registries,
                    )
                )
            elif "value_stream" in item_spec:
                data_record_futures.append(
                    start_future(
                        tg,
                        _save_storage_stream_spec,
                        tags,
                        record_id,
                        content_key,
                        item_spec["value_stream"],
                        item_spec.get("serializer"),
                        item_spec.get("storage"),
                        registries,
                    )
                )
            else:
                msg = f"Unknown model dump item {item_spec}."
                raise AssertionError(msg)

    contents: list[StorageContentRecord] = []
    for key, f in zip(model_spec, data_record_futures, strict=False):
        if exc := f.exception():
            msg = f"Failed to save {key!r} data for {name!r}"
            _LOG.error(msg, exc_info=(exc.__class__, exc, exc.__traceback__))
        contents.append(f.result())

    return StorageModelRecord(
        id=record_id,
        name=name,
        tags=tags,
        model_type_id=model_uuid,
        contents=contents,
    )


async def _save_storage_value_spec(
    tags: TagMap,
    model_record_id: UUID,
    content_key: str,
    value: Any,
    serializer: Serializer | None,
    storage: Storage | None,
    registries: Registries,
) -> StorageContentRecord:
    storage = storage or registries.storages.default
    serializer = serializer or registries.serializers.infer_from_value_type(type(value))
    dump = serializer.dump(value)
    digest = _make_value_dump_digest(dump)
    storage_data = await storage.put_content(dump["content"], digest, tags)
    return StorageContentRecord(
        content_encoding=dump["content_encoding"],
        content_hash=digest["content_hash"],
        content_hash_algorithm=digest["content_hash_algorithm"],
        content_size=digest["content_size"],
        content_type=dump["content_type"],
        model_id=model_record_id,
        content_key=content_key,
        serializer_name=serializer.name,
        serializer_type=SerializerTypeEnum.CONTENT,
        storage_data=storage_data,
        storage_name=storage.name,
    )


async def _save_storage_stream_spec(
    tags: TagMap,
    model_record_id: UUID,
    content_key: str,
    stream: AsyncIterable[Any],
    serializer: StreamSerializer | None,
    storage: Storage | None,
    registries: Registries,
) -> StorageContentRecord:
    storage = storage or registries.storages.default
    async with AsyncExitStack() as stack:
        if isasyncgen(stream):
            await stack.enter_async_context(aclosing(stream))

        if serializer is None:
            stream_iter = aiter(stream)
            first_value = await anext(stream_iter)
            serializer = registries.serializers.infer_from_stream_type(type(first_value))
            stream = _continue_stream(first_value, stream_iter)

        dump = serializer.dump_stream(stream)
        byte_stream, get_digest = _wrap_stream_dump(dump)
        storage_data = await storage.put_content_stream(byte_stream, get_digest, tags)
        try:
            digest = get_digest()
        except ValueError:
            msg = f"Storage {storage.name!r} did not fully consume the data stream."
            raise RuntimeError(msg) from None
        return StorageContentRecord(
            content_encoding=dump["content_encoding"],
            content_hash=digest["content_hash"],
            content_hash_algorithm=digest["content_hash_algorithm"],
            content_size=digest["content_size"],
            content_type=dump["content_type"],
            model_id=model_record_id,
            content_key=content_key,
            serializer_name=serializer.name,
            serializer_type=SerializerTypeEnum.CONTENT_STREAM,
            storage_data=storage_data,
            storage_name=storage.name,
        )
    raise AssertionError  # nocov


def _wrap_stream_dump(dump: ContentStreamDump) -> tuple[AsyncGenerator[bytes], GetStreamDigest]:
    stream = dump["content_stream"]

    content_hash = sha256()
    content_size = 0
    is_complete = False

    async def wrapper() -> AsyncGenerator[bytes]:
        nonlocal is_complete, content_size
        async with aclosing(stream):
            async for chunk in stream:
                content_hash.update(chunk)
                content_size += len(chunk)
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
            "content_size": content_size,
            "content_type": dump["content_type"],
            "is_complete": is_complete,
        }

    return wrapper(), get_digest


def _make_value_dump_digest(dump: ContentDump) -> ValueDigest:
    content = dump["content"]
    content_hash = sha256(content)
    return {
        "content_encoding": dump.get("content_encoding"),
        "content_hash": content_hash.hexdigest(),
        "content_hash_algorithm": content_hash.name,
        "content_size": len(content),
        "content_type": dump["content_type"],
    }


class _SerializationHelper:
    def __init__(self, serializers: SerializerRegistry) -> None:
        self._serializers = serializers

    def dump(
        self,
        value: Any,
        serializer: Serializer | None,
    ) -> ContentDump:
        if serializer is None:
            serializer = self._serializers.infer_from_value_type(type(value))
        return serializer.dump(value)

    async def dump_stream(
        self,
        stream: AsyncIterable,
        serializer: StreamSerializer | None,
    ) -> ContentStreamDump:
        if serializer is not None:
            return serializer.dump_stream(stream)

        stream_iter = aiter(stream)
        first_value = await anext(stream_iter)
        serializer = self._serializers.infer_from_stream_type(type(first_value))
        return serializer.dump_stream(_continue_stream(first_value, stream_iter))


async def _save_record_groups(
    records: Sequence[StorageModelRecord],
    session: DatabaseSession,
    retries: int,
) -> None:
    if not records:
        return

    archive_conflicting_records = (
        update(StorageModelRecord)
        .where(or_(*(_model_record_conflicts(r) for r in records)))
        .values({StorageModelRecord.archived_at: func.now()})
    )

    async for attempt in AsyncRetrying(stop=stop_after_attempt(retries)):
        with attempt:
            try:
                async with session.begin_nested():
                    await session.execute(archive_conflicting_records)
                    session.add_all(records)
                    await session.commit()
            except IntegrityError:
                pass


def _model_record_conflicts(record: StorageModelRecord):
    """Return a clause that matches records that conflict with the given one."""
    return and_(StorageModelRecord.name == record.name, StorageModelRecord.archived_at == NEVER)


async def _continue_stream(first_value: Any, stream: AsyncIterable[Any]) -> AsyncGenerator[Any]:
    yield first_value
    async for cont_value in stream:
        yield cont_value
