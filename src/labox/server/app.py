from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import litestar as ls
from litestar.datastructures import State
from litestar.di import Provide
from litestar.exceptions import ClientException
from litestar.status_codes import HTTP_409_CONFLICT
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession

from labox.server.routes import get_content
from labox.server.routes import get_manifest
from labox.server.routes import list_contents


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


@asynccontextmanager
async def db_connection(app: ls.Litestar) -> AsyncGenerator[None, None]:
    """Lifespan handler that manages the database engine lifecycle."""
    engine = app.state.engine
    try:
        yield
    finally:
        await engine.dispose()


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
