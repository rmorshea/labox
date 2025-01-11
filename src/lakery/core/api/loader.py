from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from typing import Any
from typing import TypeAlias
from typing import TypeVar
from typing import overload

from anyio import create_task_group
from anysync import contextmanager
from pybooster import injector
from pybooster import required
from sqlalchemy import and_
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from lakery.common.anyio import FutureResult
from lakery.common.anyio import set_future_exception_forcefully
from lakery.common.anyio import start_future
from lakery.common.anyio import start_given_future
from lakery.common.exceptions import NotRegistered
from lakery.core.context import DatabaseSession
from lakery.core.model import ModelRegistry
from lakery.core.model import StorageDump
from lakery.core.model import StorageModel
from lakery.core.schema import NEVER
from lakery.core.schema import SerializerTypeEnum
from lakery.core.schema import StorageContentRecord
from lakery.core.schema import StorageModelRecord
from lakery.core.serializer import SerializerRegistry
from lakery.core.serializer import StreamSerializer
from lakery.core.storage import StorageRegistry

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from collections.abc import Sequence


M = TypeVar("M", bound=StorageModel)

_Requests = tuple[type[StorageModel] | None, str, datetime | None, FutureResult[StorageModel]]


@contextmanager
@injector.asynciterator(
    requires=(DatabaseSession, ModelRegistry, SerializerRegistry, StorageRegistry)
)
async def data_loader(
    *,
    session: DatabaseSession = required,
    models: ModelRegistry = required,
    serializers: SerializerRegistry = required,
    storages: StorageRegistry = required,
) -> AsyncIterator[DataLoader]:
    """Create a context manager for saving data."""
    requests: list[_Requests] = []
    yield _DataLoader(requests)

    query = [(name, version) for _, name, version, _ in requests]
    records = await _load_model_records(session, query)

    async with create_task_group() as tg:
        for (expected_model_type, _, _, future), rec in zip(requests, records, strict=False):
            if rec is None:
                set_future_exception_forcefully(future, ValueError("No record found."))
                continue
            try:
                actual_model_type = models[rec.model_uuid]
            except NotRegistered as error:
                set_future_exception_forcefully(future, error)
                continue

            if expected_model_type and not issubclass(actual_model_type, expected_model_type):
                msg = f"Expected {expected_model_type}, but {rec} is {actual_model_type}."
                set_future_exception_forcefully(future, TypeError(msg))
                continue

            start_given_future(
                tg,
                future,
                load_model_from_record,
                rec,
                models=models,
                serializers=serializers,
                storages=storages,
            )


class _DataLoader:
    def __init__(self, requests: list[_Requests]) -> None:
        self._requests = requests

    @overload
    def load_soon(
        self,
        model: type[M],
        /,
        *,
        name: str,
        version: datetime | None = ...,
    ) -> FutureResult[M]: ...

    @overload
    def load_soon(
        self,
        model: None = ...,
        /,
        *,
        name: str,
        version: datetime | None = ...,
    ) -> FutureResult[StorageModel]: ...

    def load_soon(
        self,
        model_type: type[StorageModel] | None = None,
        /,
        *,
        name: str,
        version: datetime | None = None,
    ) -> FutureResult[Any]:
        """Load the given model soon."""
        future = FutureResult()
        self._requests.append((model_type, name, version, future))
        return future


DataLoader: TypeAlias = _DataLoader
"""Defines a protocol for saving data."""


async def load_model_from_record(
    record: StorageModelRecord,
    /,
    *,
    models: ModelRegistry,
    serializers: SerializerRegistry,
    storages: StorageRegistry,
) -> StorageModel:
    """Load the given model from the given record."""
    model_type = models[record.model_uuid]

    content_futures: dict[str, FutureResult[StorageDump]] = {}
    async with create_task_group() as tg:
        for content in record.contents:
            content_futures[content.model_key] = start_future(
                tg,
                load_content_from_record,
                content,
                serializers=serializers,
                storages=storages,
            )

    contents = {key: future.result() for key, future in content_futures.items()}

    return await model_type.storage_model_load(contents, serializers)


async def load_content_from_record(
    record: StorageContentRecord,
    *,
    serializers: SerializerRegistry,
    storages: StorageRegistry,
) -> StorageDump:
    """Load the given content from the given record."""
    serializer = serializers[record.serializer_name]
    storage = storages[record.storage_name]
    match record.serializer_type:
        case SerializerTypeEnum.VALUE:
            byte_value = await storage.get_value(record.storage_data)
            return {
                "value_dump": {
                    "content_encoding": record.content_encoding,
                    "content_type": record.content_type,
                    "content_bytes": byte_value,
                    "serializer_name": record.serializer_name,
                    "serializer_version": record.serializer_version,
                },
                "storage": storage,
            }
        case SerializerTypeEnum.STREAM:
            if not isinstance(serializer, StreamSerializer):
                msg = f"Content {record.id} expects a stream serializer, got {serializer}."
                raise TypeError(msg)
            byte_stream = storage.get_stream(record.storage_data)
            return {
                "stream_dump": {
                    "content_encoding": record.content_encoding,
                    "content_type": record.content_type,
                    "content_byte_stream": byte_stream,
                    "serializer_name": record.serializer_name,
                    "serializer_version": record.serializer_version,
                },
                "storage": storage,
            }
        case _:  # nocov
            msg = f"Unknown serializer type: {record.serializer_type}"
            raise ValueError(msg)


async def _load_model_records(
    session: DatabaseSession,
    query: Sequence[tuple[str, datetime | None]],
) -> Sequence[StorageModelRecord | None]:
    """Load the model records for the given query in the order requested."""
    stmt = (
        select(StorageModelRecord)
        .where(
            or_(
                *(
                    and_(
                        StorageModelRecord.name == name,
                        (
                            StorageModelRecord.archived_at == NEVER
                            if version is None
                            else and_(
                                StorageModelRecord.created_at <= version,
                                StorageModelRecord.archived_at > version,
                            )
                        ),
                    )
                    for name, version in query
                )
            )
        )
        .options(joinedload(StorageModelRecord.contents))
    )

    records_by_name: dict[str, StorageModelRecord] = {
        record.name: record for record in (await session.scalars(stmt)).unique()
    }

    return [records_by_name.get(name) for name, _ in query]
