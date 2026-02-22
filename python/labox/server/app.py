from collections.abc import AsyncGenerator

import litestar as ls
from litestar.datastructures import State
from litestar.di import Provide
from litestar.exceptions import ClientException
from litestar.status_codes import HTTP_409_CONFLICT
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession

from labox.core.registry import Registry
from labox.server.routes import get_content
from labox.server.routes import get_content_data
from labox.server.routes import get_manifest
from labox.server.routes import list_contents


def create_app(sqlalchemy_engine: AsyncEngine, registry: Registry) -> ls.Litestar:
    """Return the ASGI application for Labox.

    Args:
        sqlalchemy_engine: An async SQLAlchemy engine to use for database operations.
        registry: The registry containing storage, serializer, and unpacker configurations.

    Returns:
        A configured Litestar application.
    """
    app = ls.Litestar(
        route_handlers=[
            get_manifest,
            list_contents,
            get_content,
            get_content_data,
        ],
        dependencies={
            "session": Provide(_provide_transaction),
            "registry": Provide(_provide_registry, sync_to_thread=False),
        },
    )
    app.state.engine = sqlalchemy_engine
    app.state.registry = registry
    return app


async def _provide_transaction(state: State) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(bind=state.engine, expire_on_commit=False) as session:
        try:
            async with session.begin():
                yield session
        except IntegrityError as exc:  # nocov
            raise ClientException(status_code=HTTP_409_CONFLICT, detail=str(exc)) from exc


def _provide_registry(state: State) -> Registry:
    return state.registry
