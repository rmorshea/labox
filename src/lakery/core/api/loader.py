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

from lakery._internal._anyio import FutureResult
from lakery._internal._anyio import set_future_exception_forcefully
from lakery._internal._anyio import start_future
from lakery._internal._anyio import start_with_future
from lakery.common.exceptions import NotRegistered
from lakery.core.database import ContentRecord
from lakery.core.database import ManifestRecord
from lakery.core.database import SerializerTypeEnum
from lakery.core.serializer import StreamSerializer

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from lakery.core.registry import Registry
    from lakery.core.unpacker import UnpackedValue
    from lakery.core.unpacker import UnpackedValueStream


M = TypeVar("M")

_Requests = tuple[ManifestRecord, type[Any] | None, FutureResult[Any]]
_RecordGroup = tuple[ManifestRecord, Sequence[ContentRecord]]


async def load_one(
    manifest: ManifestRecord,
    cls: type[M],
    *,
    registry: Registry,
    session: AsyncSession,
) -> M:
    """Load a single object from the given manifest record."""
    async with new_loader(registry, session) as loader:
        future = loader.load_soon(manifest, cls)
    return future.result()


@contextmanager
async def new_loader(
    registry: Registry,
    session: AsyncSession,
) -> AsyncIterator[DataLoader]:
    """Create a context manager for saving data."""
    requests: list[_Requests] = []
    yield _DataLoader(requests)

    record_groups = await _load_manifest_contents(session, [m for m, _, _ in requests])

    async with create_task_group() as tg:
        for (manifest, contents), (_, expected_cls, future) in zip(
            record_groups,
            requests,
            strict=False,
        ):
            try:
                actual_cls = registry.get_storable(manifest.class_id)
            except NotRegistered as error:
                set_future_exception_forcefully(future, error)
                continue

            if expected_cls and not issubclass(actual_cls, expected_cls):
                msg = f"Expected {expected_cls}, but {manifest} is {actual_cls}."
                set_future_exception_forcefully(future, TypeError(msg))
                continue

            start_with_future(
                tg,
                future,
                load_from_manifest_record,
                manifest,
                contents,
                registry=registry,
            )


class _DataLoader:
    def __init__(self, requests: list[_Requests]) -> None:
        self._requests = requests

    @overload
    def load_soon(
        self,
        manifest: ManifestRecord,
        cls: type[M],
        /,
    ) -> FutureResult[M]: ...

    @overload
    def load_soon(
        self,
        manifest: ManifestRecord,
        cls: None = ...,
        /,
    ) -> FutureResult[Any]: ...

    def load_soon(
        self,
        manifest: ManifestRecord,
        cls: type[Any] | None = None,
        /,
    ) -> FutureResult[Any]:
        """Load an object from the given manifest record."""
        future = FutureResult()
        self._requests.append((manifest, cls, future))
        return future


DataLoader: TypeAlias = _DataLoader
"""Defines a protocol for saving data."""


async def load_from_manifest_record(
    manifest: ManifestRecord,
    contents: Sequence[ContentRecord],
    /,
    *,
    registry: Registry,
) -> Any:
    """Load an object from the given manifest record."""
    unpacker = registry.get_unpacker(manifest.unpacker_name)
    cls = registry.get_storable(manifest.class_id)

    content_futures: dict[str, FutureResult[Any]] = {}
    async with create_task_group() as tg:
        for c in contents:
            content_futures[c.content_name] = start_future(
                tg,
                load_from_content_record,
                c,
                registry=registry,
            )

    return unpacker.repack_object(
        cls,
        {i: f.result() for i, f in content_futures.items()},
        registry,
    )


async def load_from_content_record(
    record: ContentRecord,
    *,
    registry: Registry,
) -> UnpackedValue | UnpackedValueStream:
    """Load the given content from the given record."""
    storage = registry.get_storage(record.storage_name)
    storage_data = storage.deserialize_storage_data(record.storage_data)
    match record.serializer_type:
        case SerializerTypeEnum.Serializer:
            serializer = registry.get_serializer(record.serializer_name)
            value = serializer.deserialize_data(
                {
                    "content_encoding": record.content_encoding,
                    "content_type": record.content_type,
                    "data": await storage.read_data(storage_data),
                }
            )
            return {
                "value": value,
                "serializer": serializer,
                "storage": storage,
                "tags": record.tags,
            }
        case SerializerTypeEnum.StreamSerializer:
            serializer = registry.get_stream_serializer(record.serializer_name)
            if not isinstance(serializer, StreamSerializer):
                msg = f"Content {record.id} expects a stream serializer, got {serializer}."
                raise TypeError(msg)
            stream = serializer.deserialize_data_stream(
                {
                    "content_encoding": record.content_encoding,
                    "content_type": record.content_type,
                    "data_stream": storage.read_data_stream(storage_data),
                }
            )
            return {
                "value_stream": stream,
                "serializer": serializer,
                "storage": storage,
                "tags": record.tags,
            }
        case _:  # nocov
            msg = f"Unknown serializer type: {record.serializer_type}"
            raise ValueError(msg)


async def _load_manifest_contents(
    session: AsyncSession,
    manifests: Sequence[ManifestRecord],
) -> Sequence[_RecordGroup]:
    """Load the content records for the given manifest records."""
    missing: list[ManifestRecord] = []
    present: list[_RecordGroup] = []
    for m in manifests:
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
