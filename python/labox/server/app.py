from collections.abc import AsyncGenerator
from pathlib import Path

import litestar as ls
from litestar.contrib.jinja import JinjaTemplateEngine
from litestar.datastructures import State
from litestar.di import Provide
from litestar.exceptions import ClientException
from litestar.static_files import create_static_files_router
from litestar.status_codes import HTTP_409_CONFLICT
from litestar.template import TemplateConfig
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession

from labox.core.registry import Registry
from labox.server.routes import get_content
from labox.server.routes import get_content_data
from labox.server.routes import get_content_view
from labox.server.routes import get_manifest
from labox.server.routes import list_contents

_HERE = Path(__file__).parent
_STATIC_DIR = _HERE / "static"
_TEMPLATES_DIR = _HERE / "templates"


def create_app(engine: AsyncEngine, registry: Registry) -> ls.Litestar:
    """Return the ASGI application for Labox.

    Args:
        engine: An async SQLAlchemy engine to use for database operations.
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
            get_content_view,
            create_static_files_router(path="/static", directories=[_STATIC_DIR]),
        ],
        template_config=TemplateConfig(directory=_TEMPLATES_DIR, engine=JinjaTemplateEngine),
        dependencies={
            "session": Provide(_provide_transaction),
            "registry": Provide(_provide_registry, sync_to_thread=False),
        },
    )
    app.state.engine = engine
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
