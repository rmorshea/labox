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

from labox.common.anyio import TaskFuture
from labox.common.anyio import start_future
from labox.common.anyio import start_with_future
from labox.common.exceptions import NotRegistered
from labox.core.database import ContentRecord
from labox.core.database import ManifestRecord
from labox.core.database import SerializerTypeEnum
from labox.core.serializer import StreamSerializer

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from labox.core.registry import Registry
    from labox.core.unpacker import UnpackedValue
    from labox.core.unpacker import UnpackedValueStream


M = TypeVar("M")

_Requests = tuple[ManifestRecord, type[Any] | None, TaskFuture[Any]]
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
    return future.value


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
                future.set_exception(error)
                continue

            if expected_cls and not issubclass(actual_cls, expected_cls):
                msg = f"Expected {expected_cls}, but {manifest} is {actual_cls}."
                future.set_exception(TypeError(msg))
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
    ) -> TaskFuture[M]: ...

    @overload
    def load_soon(
        self,
        manifest: ManifestRecord,
        cls: None = ...,
        /,
    ) -> TaskFuture[Any]: ...

    def load_soon(
        self,
        manifest: ManifestRecord,
        cls: type[Any] | None = None,
        /,
    ) -> TaskFuture[Any]:
        """Load an object from the given manifest record."""
        future = TaskFuture()
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

    content_futures: dict[str, TaskFuture[Any]] = {}
    async with create_task_group() as tg:
        for c in contents:
            content_futures[c.content_key] = start_future(
                tg,
                load_from_content_record,
                c,
                registry=registry,
            )

    return unpacker.repack_object(
        cls,
        {i: f.value for i, f in content_futures.items()},
        registry,
    )


async def load_from_content_record(
    record: ContentRecord,
    *,
    registry: Registry,
) -> UnpackedValue | UnpackedValueStream:
    """Load the given content from the given record."""
    storage = registry.get_storage(record.storage_name)
    storage_data = registry.decode_json(record.storage_config)
    match record.serializer_type:
        case SerializerTypeEnum.Serializer:
            serializer = registry.get_serializer(record.serializer_name)
            value = serializer.deserialize_data(
                {
                    "config": registry.decode_json(record.serializer_config),
                    "content_encoding": record.content_encoding,
                    "content_type": record.content_type,
                    "data": await storage.read_data(storage_data),
                }
            )
            return {
                "value": value,
                "serializer": serializer,
                "storage": storage,
            }
        case SerializerTypeEnum.StreamSerializer:
            serializer = registry.get_stream_serializer(record.serializer_name)
            if not isinstance(serializer, StreamSerializer):
                msg = f"Content {record.id} expects a stream serializer, got {serializer}."
                raise TypeError(msg)
            stream = serializer.deserialize_data_stream(
                {
                    "config": registry.decode_json(record.serializer_config),
                    "content_encoding": record.content_encoding,
                    "content_type": record.content_type,
                    "data_stream": storage.read_data_stream(storage_data),
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
