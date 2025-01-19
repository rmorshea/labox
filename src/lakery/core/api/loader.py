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
from lakery.core.context import Registries
from lakery.core.model import AnyManifest
from lakery.core.model import BaseStorageModel
from lakery.core.schema import NEVER
from lakery.core.schema import ContentRecord
from lakery.core.schema import ManifestRecord
from lakery.core.schema import SerializerTypeEnum
from lakery.core.serializer import SerializerRegistry
from lakery.core.serializer import StreamSerializer

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from collections.abc import Sequence

    from lakery.core.storage import StorageRegistry


M = TypeVar("M", bound=BaseStorageModel)

_Requests = tuple[
    type[BaseStorageModel] | None, str, datetime | None, FutureResult[BaseStorageModel]
]


@contextmanager
@injector.asynciterator(requires=(Registries, DatabaseSession))
async def data_loader(
    *,
    registries: Registries = required,
    session: DatabaseSession = required,
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
                actual_model_type = registries.models[rec.model_id]
            except NotRegistered as error:
                set_future_exception_forcefully(future, error)
                continue

            if expected_model_type and not issubclass(actual_model_type, expected_model_type):
                msg = f"Expected {expected_model_type}, but {rec} is {actual_model_type}."
                set_future_exception_forcefully(future, TypeError(msg))
                continue

            start_given_future(tg, future, load_model_from_record, rec, registries=registries)


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
    ) -> FutureResult[BaseStorageModel]: ...

    def load_soon(
        self,
        model_type: type[BaseStorageModel] | None = None,
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
    record: ManifestRecord,
    *,
    registries: Registries,
) -> BaseStorageModel:
    """Load the given model from the given record."""
    model_type = registries.models[record.model_id]

    manifest_futures: dict[str, FutureResult[AnyManifest]] = {}
    async with create_task_group() as tg:
        for content in record.contents:
            manifest_futures[content.manifest_key] = start_future(
                tg,
                load_manifest_from_record,
                content,
                serializers=registries.serializers,
                storages=registries.storages,
            )

    manifests = {i: f.result() for i, f in manifest_futures.items()}
    return model_type.storage_model_load(manifests, registries)


async def load_manifest_from_record(
    record: ContentRecord,
    *,
    serializers: SerializerRegistry,
    storages: StorageRegistry,
) -> AnyManifest:
    """Load the given content from the given record."""
    serializer = serializers[record.serializer_name]
    storage = storages[record.storage_name]
    match record.serializer_type:
        case SerializerTypeEnum.Content:
            value = serializer.load(
                {
                    "content_encoding": record.content_encoding,
                    "content_type": record.content_type,
                    "data": await storage.get_data(record.storage_data),
                }
            )
            return {"value": value, "serializer": serializer, "storage": storage}
        case SerializerTypeEnum.ContentStream:
            if not isinstance(serializer, StreamSerializer):
                msg = f"Content {record.id} expects a stream serializer, got {serializer}."
                raise TypeError(msg)
            stream = serializer.load_stream(
                {
                    "content_encoding": record.content_encoding,
                    "content_type": record.content_type,
                    "data_stream": storage.get_data_stream(record.storage_data),
                }
            )
            return {"stream": stream, "serializer": serializer, "storage": storage}
        case _:  # nocov
            msg = f"Unknown serializer type: {record.serializer_type}"
            raise ValueError(msg)


async def _load_model_records(
    session: DatabaseSession,
    query: Sequence[tuple[str, datetime | None]],
) -> Sequence[ManifestRecord | None]:
    """Load the model records for the given query in the order requested."""
    stmt = (
        select(ManifestRecord)
        .where(
            or_(
                *(
                    and_(
                        ManifestRecord.name == name,
                        (
                            ManifestRecord.archived_at == NEVER
                            if version is None
                            else and_(
                                ManifestRecord.created_at <= version,
                                ManifestRecord.archived_at > version,
                            )
                        ),
                    )
                    for name, version in query
                )
            )
        )
        .options(joinedload(ManifestRecord.contents))
    )

    records_by_name: dict[str, ManifestRecord] = {
        record.name: record for record in (await session.scalars(stmt)).unique()
    }

    return [records_by_name.get(name) for name, _ in query]
