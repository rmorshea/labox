from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Generic
from typing import TypeVar

from anyio import create_task_group
from ninject import inject
from sqlalchemy import func
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from tenacity import AsyncRetrying
from tenacity import stop_after_attempt

from artery.core.schema import NEVER
from artery.core.schema import Record
from artery.utils.anyio import create_future
from artery.utils.misc import frozenclass

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from artery.core.options import Options
    from artery.core.serializer import Serializer
    from artery.core.serializer import SerializerRegistry
    from artery.core.storage import Storage
    from artery.core.storage import StorageRegistry

T = TypeVar("T")


@frozenclass
class Artifact(Generic[T]):
    """An artifact"""

    name: str
    """The name of the artifact."""
    value: T
    """The value of the artifact."""
    description: str | None = None
    """A description of the artifact."""
    serializer: Serializer[T] | None = None
    """The serializer for the artifact."""
    storage: Storage | None = None
    """The storage backend for the artifact."""


@inject
async def save_artifacts(
    artifacts: Sequence[Artifact],
    *,
    session: AsyncSession = inject.ed,
    storage_registry: StorageRegistry = inject.ed,
    serializer_registry: SerializerRegistry = inject.ed,
    options: Options = inject.ed,
) -> Sequence[Record]:
    """Save the given artifacts."""
    pointers, data = list(
        zip(
            *[
                _artifact_to_pointer_data(art, storage_registry, serializer_registry)
                for art in artifacts
            ],
            strict=False,
        )
    )
    await _save_pointers(session, pointers, options.ARTERY_MAX_ARCHIVE_EXISTING_RETRIES)
    await _save_pointer_data(pointers, data)
    return pointers


@inject
async def load_artifacts(
    pointers: Sequence[Record],
    *,
    storage_registry: StorageRegistry = inject.ed,
    serializer_registry: SerializerRegistry = inject.ed,
) -> Sequence[Artifact]:
    """Load the artifacts for the given pointers."""
    return [
        _pointer_data_to_artifact(pointer, data, storage_registry, serializer_registry)
        for pointer, data in zip(
            pointers, await _load_pointer_data(pointers, storage_registry), strict=False
        )
    ]


async def _save_pointer_data(
    pointers: Sequence[Record],
    data: Sequence[bytes],
    storage_registry: StorageRegistry,
) -> None:
    """Save the given data to the storage backend."""
    pointers_by_storage_name: dict[str, list[Record]] = {}
    for pointer, datum in zip(pointers, data, strict=False):
        pointers_by_storage_name.setdefault(pointer.storage_name, []).append((pointer, datum))

    async with create_task_group() as tasks:
        for storage_name, pointer_data in pointers_by_storage_name.items():
            storage = storage_registry.by_name[storage_name]
            pointers, data = zip(*pointer_data, strict=False)
            tasks.start_soon(storage.create_many, pointers, data)


async def _save_pointers(
    session: AsyncSession,
    pointers: Sequence[Record],
    retries: int,
) -> None:
    """Save the given pointers to the database."""
    stop = stop_after_attempt(retries)

    names = {p.pointer_name for p in pointers}
    update_existing_stmt = (
        update(Record)
        .where(Record.pointer_archived_at == NEVER, Record.pointer_name.in_(names))
        .values({Record.pointer_archived_at: func.now()})
    )

    async for attempt in AsyncRetrying(stop=stop, retry_error_cls=IntegrityError):
        async with attempt:
            async with session.begin_nested():
                await session.execute(update_existing_stmt)
                session.add_all(pointers)
                await session.commit()


async def _load_pointer_data(
    pointers: Sequence[Record],
    storage_registry: StorageRegistry,
) -> Sequence[bytes]:
    """Load the data for the given pointers from the storage backend."""
    pointers_by_storage_name: dict[str, list[Record]] = {}
    for pointer in pointers:
        pointers_by_storage_name.setdefault(pointer.storage_name, []).append(pointer)

    async with create_task_group() as tasks:
        data_futures = [
            create_future(tasks, storage_registry.by_name[storage_name].read_many, pointers)
            for storage_name, pointers in pointers_by_storage_name.items()
        ]

    return [data for future in data_futures for data in future.get()]


def _artifact_to_pointer_data(
    artifact: Artifact,
    storage_registry: StorageRegistry,
    serializer_registry: SerializerRegistry,
) -> tuple[Record, bytes]:
    """Serialize the values of the given artifacts."""
    storage = artifact.storage or storage_registry.default
    storage_registry.ensure_registered(storage)

    serializer = artifact.serializer or serializer_registry.get_by_type_inference(
        type(artifact.value)
    )
    if serializer is None:
        msg = f"Could not infer serializer for {artifact.value}"
        raise ValueError(msg)
    serializer_registry.ensure_registered(serializer)

    data = serializer.dump(artifact.value)

    return (
        Record(
            artifact_name=artifact.name,
            artifact_description=artifact.description,
            content_type=data["content_type"],
            content_size=len(data["content_bytes"]),
            content_hash=data["content_hash"],
            serializer_name=serializer.name,
            storage_name=storage.name,
        ),
        data,
    )


@inject
def _pointer_data_to_artifact(
    pointer: Record,
    data: bytes,
    storage_registry: StorageRegistry = inject.ed,
    serializer_registry: SerializerRegistry = inject.ed,
) -> Artifact:
    """Deserialize the values of the given pointers."""
    storage = storage_registry.by_name[pointer.storage_name]
    serializer = serializer_registry.by_name[pointer.serializer_name]

    return Artifact(
        name=pointer.pointer_name,
        value=serializer.load(
            {
                "content_type": pointer.content_type,
                "content_bytes": data,
                "content_hash": pointer.content_hash,
            }
        ),
        description=pointer.pointer_description,
        serializer=serializer,
        storage=storage,
    )
