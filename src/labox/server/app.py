from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

import litestar as ls
from litestar import get
from litestar.datastructures import State
from litestar.di import Provide
from litestar.dto import DataclassDTO
from litestar.dto import DTOConfig
from litestar.exceptions import ClientException
from litestar.exceptions import NotFoundException
from litestar.params import Parameter
from litestar.status_codes import HTTP_409_CONFLICT
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession

from labox.core.database import ContentRecord
from labox.core.database import ManifestRecord


def create_app(sqlalchemy_engine: AsyncEngine) -> ls.Litestar:
    """Return the ASGI application for Labox.

    Args:
        sqlalchemy_engine: An async SQLAlchemy engine to use for database operations.

    Returns:
        A configured Litestar application.
    """
    app = ls.Litestar(
        route_handlers=[
            get_manifest,
            list_contents,
            get_content,
        ],
        dependencies={"session": Provide(provide_transaction)},
        lifespan=[db_connection],
    )
    app.state.engine = sqlalchemy_engine
    return app


class ManifestReadDTO(DataclassDTO[ManifestRecord]):
    """Data transfer object for reading manifest records."""

    config = DTOConfig(exclude={"contents"})


@asynccontextmanager
async def db_connection(app: ls.Litestar) -> AsyncGenerator[None, None]:
    """Lifespan handler that manages the database engine lifecycle."""
    engine = app.state.engine
    try:
        yield
    finally:
        await engine.dispose()


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
    query = (
        select(ContentRecord).offset(offset).limit(limit).order_by(ContentRecord.created_at.desc())
    )

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


async def provide_transaction(state: State) -> AsyncGenerator[AsyncSession, None]:
    """Provide an async database session with transaction context."""
    async with AsyncSession(bind=state.engine, expire_on_commit=False) as session:
        try:
            async with session.begin():
                yield session
        except IntegrityError as exc:
            raise ClientException(
                status_code=HTTP_409_CONFLICT,
                detail=str(exc),
            ) from exc
