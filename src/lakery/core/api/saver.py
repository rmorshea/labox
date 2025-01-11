from __future__ import annotations

from contextlib import aclosing
from hashlib import sha256
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
from lakery.core.model import ModelRegistry
from lakery.core.schema import NEVER
from lakery.core.schema import SerializerTypeEnum
from lakery.core.schema import StorageContentRecord
from lakery.core.schema import StorageModelRecord
from lakery.core.serializer import SerializerRegistry
from lakery.core.serializer import StreamSerializer
from lakery.core.serializer import ValueSerializer
from lakery.core.storage import GetStreamDigest
from lakery.core.storage import Storage
from lakery.core.storage import StorageRegistry
from lakery.core.storage import StreamDigest

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator
    from collections.abc import Mapping
    from collections.abc import Sequence

    from anyio.abc import TaskGroup

    from lakery.common.utils import TagMap
    from lakery.core.model import StorageModel
    from lakery.core.serializer import StreamDump
    from lakery.core.serializer import ValueDump
    from lakery.core.storage import ValueDigest


T = TypeVar("T")
P = ParamSpec("P")
D = TypeVar("D", bound=StorageModelRecord)


_COMMIT_RETRIES = 3
_LOG = getLogger(__name__)


@contextmanager
@injector.asynciterator(
    requires=(DatabaseSession, ModelRegistry, SerializerRegistry, StorageRegistry)
)
async def model_saver(
    *,
    session: DatabaseSession = required,
    models: ModelRegistry = required,
    serializers: SerializerRegistry = required,
    storages: StorageRegistry = required,
) -> AsyncIterator[ModelSaver]:
    """Create a context manager for saving data."""
    futures: list[FutureResult[StorageModelRecord]] = []
    try:
        async with create_task_group() as tg:
            yield _ModelSaver(tg, futures, models, serializers, storages)
    finally:
        records = [r for f in futures if (r := f.result(default=None))]
        await _save_record_groups(records, session, _COMMIT_RETRIES)


class _ModelSaver:
    def __init__(
        self,
        task_group: TaskGroup,
        futures: list[FutureResult[StorageModelRecord]],
        models: ModelRegistry,
        serializers: SerializerRegistry,
        storages: StorageRegistry,
    ):
        self._futures = futures
        self._task_group = task_group
        self._models = models
        self._serializers = serializers
        self._storages = storages

    def save_soon(
        self,
        name: str,
        model: StorageModel,
        *,
        tags: Mapping[str, str] | None = None,
    ) -> FutureResult[StorageModelRecord]:
        """Schedule the given model to be saved."""
        self._models.check_registered(type(model))

        future = start_future(
            self._task_group,
            _save_model,
            name,
            model,
            tags or {},
            self._serializers,
            self._storages,
        )
        self._futures.append(future)

        return future


ModelSaver: TypeAlias = _ModelSaver
"""Defines a protocol for saving data."""


async def _save_model(
    name: str,
    model: StorageModel,
    tags: TagMap,
    serializers: SerializerRegistry,
    storages: StorageRegistry,
) -> StorageModelRecord:
    """Save the given data to the database."""
    model_uuid = UUID(type(model).storage_model_uuid)
    model_dump = await model.storage_model_dump(serializers)

    record_id = uuid4()
    data_record_futures: list[FutureResult[StorageContentRecord]] = []
    async with create_task_group() as tg:
        for model_key, model_spec in model_dump.items():
            match model_spec:
                case {"value_dump": dump, "storage": storage}:
                    data_record_futures.append(
                        start_future(
                            tg,
                            _save_storage_value_spec,
                            record_id,
                            model_key,
                            dump,
                            storage or storages.default,
                            tags,
                        )
                    )
                case {"stream_dump": dump, "storage": storage}:
                    data_record_futures.append(
                        start_future(
                            tg,
                            _save_storage_stream_spec,
                            record_id,
                            model_key,
                            dump,
                            storage or storages.default,
                            tags,
                        )
                    )
                case _:
                    msg = f"Unknown storage spec {model_spec}."
                    raise AssertionError(msg)

    contents: list[StorageContentRecord] = []
    for model_key, f in zip(model_dump, data_record_futures, strict=False):
        if exc := f.exception():
            msg = f"Failed to save {model_key!r} data for {name!r}"
            _LOG.error(msg, exc_info=(exc.__class__, exc, exc.__traceback__))
        contents.append(f.result())

    return StorageModelRecord(
        id=record_id,
        name=name,
        tags=tags,
        model_uuid=model_uuid,
        contents=contents,
    )


async def _save_storage_value_spec(
    model_record_id: UUID,
    model_key: str,
    dump: ValueDump,
    storage: Storage,
    tags: TagMap,
) -> StorageContentRecord:
    digest = _make_value_dump_digest(dump)
    storage_data = await storage.put_value(dump["content_bytes"], digest, tags)
    return StorageContentRecord(
        content_encoding=dump["content_encoding"],
        content_hash=digest["content_hash"],
        content_hash_algorithm=digest["content_hash_algorithm"],
        content_size=digest["content_size"],
        content_type=dump["content_type"],
        model_id=model_record_id,
        model_key=model_key,
        serializer_name=dump["serializer_name"],
        serializer_type=SerializerTypeEnum.VALUE,
        serializer_version=dump["serializer_version"],
        storage_data=storage_data,
        storage_name=storage.name,
        storage_version=storage.version,
    )


async def _save_storage_stream_spec(
    model_record_id: UUID,
    model_key: str,
    dump: StreamDump,
    storage: Storage,
    tags: TagMap,
) -> StorageContentRecord:
    raw_stream = dump["content_byte_stream"]
    async with aclosing(raw_stream):
        byte_stream, get_digest = _wrap_stream_dump(dump)
        storage_data = await storage.put_stream(byte_stream, get_digest, tags)
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
            model_key=model_key,
            serializer_name=dump["serializer_name"],
            serializer_type=SerializerTypeEnum.STREAM,
            serializer_version=dump["serializer_version"],
            storage_data=storage_data,
            storage_name=storage.name,
            storage_version=storage.version,
        )


def _wrap_stream_dump(dump: StreamDump) -> tuple[AsyncGenerator[bytes], GetStreamDigest]:
    stream = dump["content_byte_stream"]

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


def _make_value_dump_digest(dump: ValueDump) -> ValueDigest:
    value = dump["content_bytes"]
    content_hash = sha256(value)
    return {
        "content_encoding": dump.get("content_encoding"),
        "content_hash": content_hash.hexdigest(),
        "content_hash_algorithm": content_hash.name,
        "content_size": len(value),
        "content_type": dump["content_type"],
    }


class _SerializationHelper:
    def __init__(self, serializers: SerializerRegistry) -> None:
        self._serializers = serializers

    def dump_value(
        self,
        value: Any,
        serializer: ValueSerializer | None,
    ) -> ValueDump:
        if serializer is None:
            serializer = self._serializers.infer_from_value_type(type(value))
        return serializer.dump_value(value)

    async def dump_stream(
        self,
        stream: AsyncIterable,
        serializer: StreamSerializer | None,
    ) -> StreamDump:
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
