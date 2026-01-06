from typing import Annotated
from uuid import UUID

from litestar import get
from litestar.dto import DataclassDTO
from litestar.dto import DTOConfig
from litestar.exceptions import NotFoundException
from litestar.params import Parameter
from litestar.response import Stream
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from labox.core.database import ContentRecord
from labox.core.database import ManifestRecord
from labox.core.registry import Registry


class ManifestReadDTO(DataclassDTO[ManifestRecord]):
    """Data transfer object for reading manifest records."""

    config = DTOConfig(exclude={"contents"})


@get("/manifests/{manifest_id:uuid}", return_dto=ManifestReadDTO)
async def get_manifest(
    session: AsyncSession,
    manifest_id: UUID,
) -> ManifestRecord:
    """Get a specific manifest by ID."""
    query = select(ManifestRecord).where(ManifestRecord.id == manifest_id)
    result = await session.execute(query)
    manifest = result.scalar_one_or_none()

    if manifest is None:
        raise NotFoundException(detail=f"Manifest {manifest_id} not found")

    return manifest


@get("/contents")
async def list_contents(
    session: AsyncSession,
    manifest_id: Annotated[UUID | None, Parameter(description="Filter by manifest ID")] = None,
    limit: Annotated[int, Parameter(gt=0, le=1000)] = 100,
    offset: Annotated[int, Parameter(ge=0)] = 0,
) -> list[ContentRecord]:
    """List all content records with pagination and optional filtering."""
    query = select(ContentRecord).offset(offset).limit(limit)

    if manifest_id is not None:
        query = query.where(ContentRecord.manifest_id == manifest_id)

    result = await session.execute(query)
    return list(result.scalars().all())


@get("/contents/{content_id:uuid}")
async def get_content(
    session: AsyncSession,
    content_id: UUID,
) -> ContentRecord:
    """Get a specific content record by ID."""
    query = select(ContentRecord).where(ContentRecord.id == content_id)
    result = await session.execute(query)
    content = result.scalar_one_or_none()

    if content is None:
        raise NotFoundException(detail=f"Content {content_id} not found")

    return content


@get("/contents/{content_id:uuid}/data")
async def get_content_data(
    session: AsyncSession,
    registry: Registry,
    content_id: UUID,
) -> Stream:
    """Stream the actual data for a specific content record."""
    # Get the content record
    query = select(ContentRecord).where(ContentRecord.id == content_id)
    result = await session.execute(query)
    content = result.scalar_one_or_none()

    if content is None:
        raise NotFoundException(detail=f"Content {content_id} not found")

    # Get the storage from the registry
    storage = registry.get_storage(content.storage_name)

    # Deserialize the storage config
    storage_config = storage.deserialize_config(content.storage_config)

    # Return a streaming response
    return Stream(storage.read_data_stream(storage_config))
