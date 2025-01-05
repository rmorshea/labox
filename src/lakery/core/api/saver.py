from __future__ import annotations

from collections.abc import Sequence
from contextlib import aclosing
from hashlib import sha256
from logging import getLogger
from typing import TYPE_CHECKING
from typing import Any
from typing import Generic
from typing import ParamSpec
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
from lakery.common.anyio import start_future
from lakery.core.context import DatabaseSession
from lakery.core.model import ModelDump
from lakery.core.model import ModelRegistry
from lakery.core.model import StorageStreamSpec
from lakery.core.model import StorageValueSpec
from lakery.core.schema import Base
from lakery.core.schema import DataRecord
from lakery.core.schema import InfoRecord
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
    from collections.abc import Callable

    from sqlalchemy.ext.asyncio import AsyncSession

    from lakery.core.model import StorageModel
    from lakery.core.serializer import StreamDump
    from lakery.core.serializer import ValueDump
    from lakery.core.storage import Storage
    from lakery.core.storage import ValueDigest


T = TypeVar("T")
P = ParamSpec("P")
D = TypeVar("D", bound=InfoRecord)

_InfoDump = tuple[InfoRecord, ModelDump]
_RecordGroup = tuple[InfoRecord, Sequence[DataRecord]]

_COMMIT_RETRIES = 3
_LOG = getLogger(__name__)


@contextmanager
@injector.asynciterator(
    requires=(DatabaseSession, ModelRegistry, SerializerRegistry, StorageRegistry)
)
async def saver(
    *,
    session: DatabaseSession = required,
    models: ModelRegistry = required,
    serializers: SerializerRegistry = required,
    storages: StorageRegistry = required,
) -> AsyncIterator[Saver]:
    """Create a context manager for saving data."""
    to_save: list[_InfoDump] = []
    yield _Saver(to_save, models)
    record_group_futures: list[TaskGroupFuture[_RecordGroup]] = []
    try:
        async with create_task_group() as tg:
            for info, dump in to_save:
                f = start_future(tg, _save_info_dump, info, dump, serializers, storages)
                record_group_futures.append(f)
    finally:
        record_groups = [g for f in record_group_futures if (g := f.result(default=None))]
        await _save_record_groups(record_groups, session, _COMMIT_RETRIES)


class _Saver:
    def __init__(self, to_save: list[_InfoDump], models: ModelRegistry):
        self._to_save = to_save
        self._models = models

    def __call__(
        self,
        name: str,
        model: StorageModel,
        record: Callable[P, D] = InfoRecord,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> D:
        """Save the given value data."""
        if args:
            msg = "Positional arguments are not allowed."
            raise TypeError(msg)

        model_type = type(model)
        self._models.check_registered(model_type)
        model_id = self._models.get_key(model_type)

        info = record(*args, **kwargs)
        info.record_name = name
        info.record_storage_model_id = model_id
        info.record_storage_model_version = model_type.storage_model_version
        self._to_save.append((info, model.storage_model_dump()))

        return info


Saver: TypeAlias = _Saver
"""Defines a protocol for saving data."""


async def _save_info_dump(
    info_record: InfoRecord,
    model_dump: ModelDump,
    serializers: SerializerRegistry,
    storages: StorageRegistry,
) -> _RecordGroup:
    """Save the given data to the database."""
    data_record_futures: list[TaskGroupFuture[DataRecord]] = []
    async with create_task_group() as tg:
        for storage_model_key, storage_spec in model_dump.items():
            if "value" in storage_spec:
                data_record_futures.append(
                    start_future(
                        tg,
                        _save_storage_value_spec,
                        info_record,
                        storage_model_key,
                        storage_spec,
                        serializers,
                        storages,
                    )
                )
            elif "stream" in storage_spec:
                data_record_futures.append(
                    start_future(
                        tg,
                        _save_storage_stream_spec,
                        info_record,
                        storage_model_key,
                        storage_spec,
                        serializers,
                        storages,
                    )
                )
            else:  # nocov
                msg = f"Unknown storage spec {storage_spec}."
                raise AssertionError(msg)

    data_records: list[DataRecord] = []
    for f in data_record_futures:
        if exc := f.exception():
            _LOG.error(exc, exc_info=(exc.__class__, exc, exc.__traceback__))
        data_records.append(f.result())

    return info_record, data_records


async def _save_storage_value_spec(
    info_record: InfoRecord,
    storage_model_key: str,
    value_spec: StorageValueSpec,
    serializers: SerializerRegistry,
    storages: StorageRegistry,
) -> DataRecord: ...


async def _save_storage_stream_spec(
    info_record: InfoRecord,
    storage_model_key: str,
    stream_spec: StorageStreamSpec,
    serializers: SerializerRegistry,
    storages: StorageRegistry,
) -> DataRecord: ...


async def _save_record_groups(
    record_groups: Sequence[_RecordGroup],
    session: DatabaseSession,
    retries: int,
) -> None:
    if not record_groups:
        return

    archive_conflicts = (
        update(InfoRecord)
        .where(or_(*(i.record_conflicts() for i, _ in record_groups)))
        .values({InfoRecord.record_archived_at: func.now()})
    )

    records: list[Base] = []
    for info, data in record_groups:
        records.append(info)
        records.extend(data)

    async for attempt in AsyncRetrying(stop=stop_after_attempt(retries)):
        with attempt:
            try:
                async with session.begin_nested():
                    await session.execute(archive_conflicts)
                    session.add_all(records)
                    await session.commit()
            except IntegrityError:
                pass


async def _save_value(
    descriptor: InfoRecord,
    to_save: _ValueToSave,
    model_key: str | None = None,
) -> Sequence[DataRecord]:
    value = to_save["value"]
    serializer = to_save["serializer"]
    storage = to_save["storage"]

    dump = serializer.dump_value(value)
    digest = _make_value_dump_digest(dump)

    await storage.put_value(descriptor, dump["content_value"], digest)

    return []


async def _save_stream(
    descriptor: InfoRecord,
    to_save: _StreamToSave,
    model_key: str | None = None,
) -> Sequence[DataRecord]:
    stream = to_save["stream"]
    serializer = to_save["serializer"]
    storage = to_save["storage"]

    dump = serializer.dump_stream(stream)
    stream, get_digest = _wrap_stream_dump(descriptor, dump)

    async with aclosing(stream):
        await storage.put_stream(descriptor, stream, get_digest)

    try:
        get_digest()
    except RuntimeError:
        msg = f"Storage {storage.name} did not fully consume the stream for relation {descriptor}."
        raise RuntimeError(msg) from None

    return []


async def _save_model(
    descriptor: InfoRecord,
    to_save: _ModelToSave,
    serializers: SerializerRegistry,
) -> Sequence[DataRecord]:
    model = to_save["model"]
    modeler = to_save["modeler"]
    storage = to_save["storage"]

    dump = modeler.dump_model(model, serializers)

    ...

    return []


async def _save_descriptors(
    session: AsyncSession,
    descriptors: Sequence[InfoRecord],
    retries: int,
) -> None:
    stop = stop_after_attempt(retries)
    update_existing_stmt = update(InfoRecord).values({InfoRecord.data_archived_at: func.now()})
    if descriptors:
        update_existing_stmt = update_existing_stmt.where(
            or_(*(d.where_latest() for d in descriptors))
        )
    async for attempt in AsyncRetrying(stop=stop):
        with attempt:
            try:
                async with session.begin_nested():
                    await session.execute(update_existing_stmt)
                    session.add_all(descriptors)
                    await session.commit()
            except IntegrityError:
                pass


class _ValueToSave(Generic[T, D], TypedDict):
    value: T
    serializer: ValueSerializer[T]
    storage: Storage


class _StreamToSave(Generic[T, D], TypedDict):
    stream: AsyncIterable[T]
    serializer: StreamSerializer[T]
    storage: Storage


def _wrap_stream_dump(
    descriptor: InfoRecord,
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


class _AutoSerializer:
    def __init__(self, serializers: SerializerRegistry) -> None:
        self._serializers = serializers

    def dump_value(self, value: Any, serializer: ValueSerializer | None) -> ValueDump:
        if serializer is None:
            serializer = self._serializers.infer_from_value_type(type(value))
        return serializer.dump_value(value)

    async def dump_stream(
        self, stream: AsyncIterable, serializer: StreamSerializer | None
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
