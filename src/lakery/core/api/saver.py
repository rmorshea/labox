from __future__ import annotations

from collections.abc import Sequence
from contextlib import aclosing
from hashlib import sha256
from logging import getLogger
from typing import TYPE_CHECKING
from typing import Any
from typing import ParamSpec
from typing import TypeAlias
from typing import TypeVar

from anyio import create_task_group
from anysync import contextmanager
from pybooster import injector
from pybooster import required
from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from tenacity import AsyncRetrying
from tenacity import stop_after_attempt

from lakery.common.anyio import TaskFuture
from lakery.common.anyio import start_future
from lakery.core.context import DatabaseSession
from lakery.core.model import ModelDump
from lakery.core.model import ModelRegistry
from lakery.core.model import StorageStreamSpec
from lakery.core.model import StorageValueSpec
from lakery.core.schema import Base
from lakery.core.schema import DataInfoAssociation
from lakery.core.schema import DataRecord
from lakery.core.schema import InfoRecord
from lakery.core.serializer import SerializerRegistry
from lakery.core.serializer import StreamSerializer
from lakery.core.serializer import ValueSerializer
from lakery.core.storage import GetStreamDigest
from lakery.core.storage import StorageRegistry
from lakery.core.storage import StreamDigest

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator
    from collections.abc import Callable

    from lakery.core.model import StorageModel
    from lakery.core.serializer import StreamDump
    from lakery.core.serializer import ValueDump
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
    record_group_futures: list[TaskFuture[_RecordGroup]] = []
    serialization_helper = _SerializationHelper(serializers)
    try:
        async with create_task_group() as tg:
            for info, dump in to_save:
                f = start_future(tg, _save_info_dump, info, dump, serialization_helper, storages)
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
    serialization_helper: _SerializationHelper,
    storages: StorageRegistry,
) -> _RecordGroup:
    """Save the given data to the database."""
    data_record_futures: list[TaskFuture[DataRecord]] = []
    async with create_task_group() as tg:
        for storage_model_key, storage_spec in model_dump.items():
            if "value" in storage_spec:
                data_record_futures.append(
                    start_future(
                        tg,
                        _save_storage_value_spec,
                        storage_model_key,
                        storage_spec,
                        serialization_helper,
                        storages,
                    )
                )
            elif "stream" in storage_spec:
                data_record_futures.append(
                    start_future(
                        tg,
                        _save_storage_stream_spec,
                        storage_model_key,
                        storage_spec,
                        serialization_helper,
                        storages,
                    )
                )
            else:  # nocov
                msg = f"Unknown storage spec {storage_spec}."
                raise AssertionError(msg)

    data_records: list[DataRecord] = []
    for model_key, f in zip(model_dump, data_record_futures, strict=False):
        if exc := f.exception():
            msg = f"Failed to save {model_key!r} data for {info_record.record_name!r}"
            _LOG.error(msg, exc_info=(exc.__class__, exc, exc.__traceback__))
        data_records.append(f.result())

    return info_record, data_records


async def _save_storage_value_spec(
    storage_model_key: str,
    spec: StorageValueSpec,
    serialization_helper: _SerializationHelper,
    storages: StorageRegistry,
) -> DataRecord:
    dump = serialization_helper.dump_value(spec["value"], spec.get("serializer"))
    storage = spec.get("storage", storages.default)
    digest = _make_value_dump_digest(dump)

    storage_data = await storage.put_value(dump["content_value"], digest)

    return DataRecord(
        content_type=dump["content_type"],
        content_encoding=dump["content_encoding"],
        content_hash=digest["content_hash"],
        content_hash_algorithm=digest["content_hash_algorithm"],
        content_size=digest["content_size"],
        serializer_name=dump["serializer_name"],
        serializer_version=dump["serializer_version"],
        storage_name=storage.name,
        storage_version=storage.version,
        storage_data=storage_data,
        storage_model_key=storage_model_key,
    )


async def _save_storage_stream_spec(
    storage_model_key: str,
    spec: StorageStreamSpec,
    serialization_helper: _SerializationHelper,
    storages: StorageRegistry,
) -> DataRecord:
    dump = await serialization_helper.dump_stream(spec["stream"], spec.get("serializer"))
    storage = spec.get("storage", storages.default)
    stream, get_digest = _wrap_stream_dump(dump)

    async with aclosing(stream):
        storage_data = await storage.put_stream(stream, get_digest)

    try:
        digest = get_digest()
    except ValueError:
        msg = f"Storage {storage.name!r} did not fully consume the data stream."
        raise RuntimeError(msg) from None

    return DataRecord(
        content_type=dump["content_type"],
        content_encoding=dump["content_encoding"],
        content_hash=digest["content_hash"],
        content_hash_algorithm=digest["content_hash_algorithm"],
        content_size=digest["content_size"],
        serializer_name=dump["serializer_name"],
        serializer_version=dump["serializer_version"],
        storage_name=storage.name,
        storage_version=storage.version,
        storage_data=storage_data,
        storage_model_key=storage_model_key,
    )


def _wrap_stream_dump(dump: StreamDump) -> tuple[AsyncGenerator[bytes], GetStreamDigest]:
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
        records.extend(DataInfoAssociation(data_id=d.id, info_id=info.record_id) for d in data)
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
