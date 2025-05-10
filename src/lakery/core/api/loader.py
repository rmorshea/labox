from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import TYPE_CHECKING
from typing import Any
from typing import TypeAlias
from typing import TypeVar
from typing import overload

from anyio import create_task_group
from anysync import contextmanager
from sqlalchemy import inspect as orm_inspect
from sqlalchemy import select

from lakery.common.anyio import FutureResult
from lakery.common.anyio import set_future_exception_forcefully
from lakery.common.anyio import start_future
from lakery.common.anyio import start_with_future
from lakery.common.exceptions import NotRegistered
from lakery.core.model import AnyContent
from lakery.core.model import BaseStorageModel
from lakery.core.schema import ContentRecord
from lakery.core.schema import ManifestRecord
from lakery.core.schema import SerializerTypeEnum
from lakery.core.serializer import StreamSerializer

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from lakery.core.registries import RegistryCollection
    from lakery.core.registries import SerializerRegistry
    from lakery.core.registries import StorageRegistry


M = TypeVar("M", bound=BaseStorageModel)

_Requests = tuple[ManifestRecord, type[BaseStorageModel] | None, FutureResult[BaseStorageModel]]
_RecordGroup = tuple[ManifestRecord, Sequence[ContentRecord]]


@contextmanager
async def data_loader(
    *,
    registries: RegistryCollection,
    session: AsyncSession,
) -> AsyncIterator[DataLoader]:
    """Create a context manager for saving data."""
    requests: list[_Requests] = []
    yield _DataLoader(requests)

    record_groups = await _load_manifest_contents(session, [m for m, _, _ in requests])

    async with create_task_group() as tg:
        for (manifest, contents), (_, expected_model_type, future) in zip(
            record_groups, requests, strict=False
        ):
            try:
                actual_model_type = registries.models[manifest.model_id]
            except NotRegistered as error:
                set_future_exception_forcefully(future, error)
                continue

            if expected_model_type and not issubclass(actual_model_type, expected_model_type):
                msg = f"Expected {expected_model_type}, but {manifest} is {actual_model_type}."
                set_future_exception_forcefully(future, TypeError(msg))
                continue

            start_with_future(
                tg,
                future,
                load_model_from_record_group,
                manifest,
                contents,
                registries=registries,
            )


class _DataLoader:
    def __init__(self, requests: list[_Requests]) -> None:
        self._requests = requests

    @overload
    def load_soon(
        self,
        manifest: ManifestRecord,
        model_type: type[M],
        /,
    ) -> FutureResult[M]: ...

    @overload
    def load_soon(
        self,
        manifest: ManifestRecord,
        model_type: None = ...,
        /,
    ) -> FutureResult[BaseStorageModel]: ...

    def load_soon(
        self,
        manifest: ManifestRecord,
        model_type: type[BaseStorageModel] | None = None,
        /,
    ) -> FutureResult[Any]:
        """Load the given model soon."""
        future = FutureResult()
        self._requests.append((manifest, model_type, future))
        return future


DataLoader: TypeAlias = _DataLoader
"""Defines a protocol for saving data."""


async def load_model_from_record_group(
    manifest: ManifestRecord,
    contents: Sequence[ContentRecord],
    /,
    *,
    registries: RegistryCollection,
) -> BaseStorageModel:
    """Load the given model from the given record."""
    model_type = registries.models[manifest.model_id]

    content_futures: dict[str, FutureResult[AnyContent]] = {}
    async with create_task_group() as tg:
        for c in contents:
            content_futures[c.content_key] = start_future(
                tg,
                load_manifest_from_record,
                c,
                serializers=registries.serializers,
                storages=registries.storages,
            )

    return model_type.storage_model_load(
        {i: f.result() for i, f in content_futures.items()},
        manifest.model_version,
        registries,
    )


async def load_manifest_from_record(
    record: ContentRecord,
    *,
    serializers: SerializerRegistry,
    storages: StorageRegistry,
) -> AnyContent:
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
            return {
                "value_stream": stream,
                "serializer": serializer,
                "storage": storage,
            }
        case _:  # nocov
            msg = f"Unknown serializer type: {record.serializer_type}"
            raise ValueError(msg)


async def _load_manifest_contents(
    session: AsyncSession,
    records: Sequence[ManifestRecord],
) -> Sequence[_RecordGroup]:
    """Load the content records for the given model records."""
    missing: list[ManifestRecord] = []
    present: list[_RecordGroup] = []
    for m in records:
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
