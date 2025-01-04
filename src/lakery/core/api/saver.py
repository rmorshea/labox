from __future__ import annotations

from contextlib import aclosing
from hashlib import sha256
from typing import TYPE_CHECKING
from typing import Generic
from typing import ParamSpec
from typing import TypeAlias
from typing import TypedDict
from typing import TypeVar
from uuid import UUID

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
from lakery.core.model import ModelRegistry
from lakery.core.schema import DataDescriptor
from lakery.core.schema import DataRecord
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
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from lakery.core.model import StorageModel
    from lakery.core.serializer import StreamDump
    from lakery.core.serializer import ValueDump
    from lakery.core.storage import Storage
    from lakery.core.storage import ValueDigest


T = TypeVar("T")
P = ParamSpec("P")
D = TypeVar("D", bound=DataDescriptor)

_ItemToSave: TypeAlias = tuple[DataDescriptor, "_ValueToSave | _StreamToSave | _ModelToSave"]

_COMMIT_RETRIES = 3


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
) -> AsyncIterator[_Saver]:
    """Create a context manager for saving data."""
    to_save: list[_ItemToSave] = []
    yield DataSaver(to_save, models, serializers, storages)
    await _save_items(to_save, session, serializers, _COMMIT_RETRIES)


class _Saver:
    def __init__(
        self,
        to_save: list[_ItemToSave],
        models: ModelRegistry,
        serializers: SerializerRegistry,
        storages: StorageRegistry,
    ):
        self._to_save = to_save
        self._models = models
        self._serializers = serializers
        self._storages = storages

    def __call__(
        self,
        name: str,
        model: StorageModel,
        descriptor: Callable[P, D] = DataDescriptor,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> D:
        """Save the given value data."""
        if args:
            msg = "Positional arguments are not allowed."
            raise TypeError(msg)
        des = descriptor(*args, **kwargs)
        des.descriptor_name = name
        des.descriptor_storage_model_id = UUID(model.storage_model_id)
        des.descriptor_storage_model_version = model.storage_model_version
        self._to_save.append(
            (
                des,
                {
                    "value": value,
                    "serializer": serializer
                    or self._serializers.infer_from_value_type(type(value)),
                    "storage": storage or self._storages.infer_from_data_relation_type(type(des)),
                },
            )
        )
        return des

    def stream(
        self,
        descriptor: Callable[P, D],
        stream: AsyncIterable[T],
        serializer: StreamSerializer[T] | type[T],
        storage: Storage[D] | None = None,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> D:
        """Save the given stream data."""
        des = descriptor(*args, **kwargs)
        self._to_save.append(
            (
                des,
                {
                    "stream": stream,
                    "serializer": self._serializers.infer_from_stream_type(serializer)
                    if isinstance(serializer, type)
                    else serializer,
                    "storage": storage or self._storages.infer_from_data_relation_type(type(des)),
                },
            )
        )
        return des

    def model(
        self,
        descriptor: Callable[P, D],
        model: T,
        modeler: Modeler[T] | None = None,
        storage: Storage[D] | None = None,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> D:
        """Save the given model data."""
        des = descriptor(*args, **kwargs)
        self._to_save.append(
            (
                des,
                {
                    "model": model,
                    "modeler": modeler or self._modelers.infer_from_type(type(model)),
                    "storage": storage or self._storages.infer_from_data_relation_type(type(des)),
                },
            )
        )
        return des


DataSaver: TypeAlias = _DataSaver
"""Defines a protocol for saving data."""


async def _save_items(
    items_to_save: Sequence[_ItemToSave],
    session: AsyncSession,
    serializers: SerializerRegistry,
    retries,
) -> None:
    """Save the given data to the database."""
    descriptors = _prepare_descriptors(items_to_save)
    await _save_descriptors(session, descriptors, retries=retries)
    pointers: list[TaskGroupFuture[Sequence[DataRecord]]] = []
    try:
        async with create_task_group() as tg:
            for descriptor, item in items_to_save:
                if "value" in item:
                    pointers.append(start_future(tg, _save_value, descriptor, item))
                if "stream" in item:
                    pointers.append(start_future(tg, _save_stream, descriptor, item))
                if "model" in item:
                    pointers.append(start_future(tg, _save_model, descriptor, item, serializers))
                else:  # noco
                    msg = f"Unknown item {item} to save for relation {descriptor}."
                    raise AssertionError(msg)
    finally:
        await _save_pointers(session, descriptors, [p.result(default=None) for p in pointers])


def _prepare_descriptors(items_to_save: Sequence[_ItemToSave]) -> Sequence[DataDescriptor]:
    descriptors: list[DataDescriptor] = []
    for descriptor, item in items_to_save:
        if "model" in item:
            modeler = item["modeler"]
            descriptor.data_modeler_name = modeler.name
            descriptor.data_modeler_version = modeler.version
        else:
            descriptor.data_modeler_name = None
            descriptor.data_modeler_version = None
        descriptors.append(descriptor)
    return descriptors


async def _save_value(
    descriptor: DataDescriptor,
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
    descriptor: DataDescriptor,
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
    descriptor: DataDescriptor,
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
    descriptors: Sequence[DataDescriptor],
    retries: int,
) -> None:
    stop = stop_after_attempt(retries)
    update_existing_stmt = update(DataDescriptor).values(
        {DataDescriptor.data_archived_at: func.now()}
    )
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


class _ModelToSave(Generic[T, D], TypedDict):
    model: T
    modeler: Modeler[T]
    storage: Storage


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
