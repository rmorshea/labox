from collections.abc import Callable
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from enum import IntEnum
from typing import Annotated
from typing import Any
from typing import TypeVar
from uuid import UUID
from uuid import uuid4

from anysync import coroutine
from sqlalchemy import BIGINT
from sqlalchemy import JSON
from sqlalchemy import BindParameter
from sqlalchemy import ColumnElement
from sqlalchemy import DateTime
from sqlalchemy import Dialect
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import MappedColumn
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm.decl_api import MappedAsDataclass
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.expression import FunctionElement
from sqlalchemy.sql.type_api import _BindProcessorType
from sqlalchemy.types import TypeDecorator

from labox.common.types import TagMap

C = TypeVar("C", bound=MappedColumn)

ColumnComparator = Callable[[ColumnElement, Any], ColumnElement[bool]]
"""A function that constrains a column to a specific value."""

DateTimeTZ = Annotated[datetime, mapped_column(DateTime(timezone=True))]
"""A datetime column with timezone information."""

NEVER = datetime.max.replace(tzinfo=UTC)
"""A timestamp representing never.

There is no way to represent an infinite datetime in Python so we resort to using the
largest representable value. Some databases have support for infinite datetimes but this
is not a universal feature. If it's desireable to map datetime.max to infinity you may
be able to register an extension with your driver of choice in order to do so. For
example, `psycopg3` allows you to implement a custom dumper/loader for datetimes:
https://www.psycopg.org/psycopg3/docs/advanced/adapt.html#example-handling-infinity-date
"""

JSON_OR_JSONB = JSON().with_variant(JSONB(), "postgresql")
"""Uses JSONB in PostgreSQL and falls back to JSON in other databases."""


class RawJson(TypeDecorator[str]):
    """A type decorator for JSON where values are given and returned as a string."""

    impl = JSON_OR_JSONB
    cache_ok = True

    def bind_processor(self, dialect: Dialect) -> _BindProcessorType:  # noqa: ARG002
        """Return a function that passes the value to the database as a string."""
        return lambda value: value

    def bind_expression(self, bindparam: BindParameter[str]) -> ColumnElement[str] | None:
        """Return a bind expression that passes the value to the database as a string."""
        return _ValidJson(bindparam)

    def column_expression(self, column: ColumnElement) -> ColumnElement[str]:
        """Return the column expression for the type decorator."""
        return column.cast(String)


class BaseRecord(MappedAsDataclass, DeclarativeBase):
    """The base for labox's core database classes."""

    @classmethod
    @coroutine
    async def create_all(cls, engine: AsyncEngine) -> None:
        """Create all tables for the database."""
        async with engine.begin() as conn:
            await conn.run_sync(cls.metadata.create_all)


class _StrMixin(BaseRecord):
    __abstract__ = True

    id: Any

    def __str__(self) -> str:  # nocov
        return f"{self.__class__.__name__}({self.id})"


class ManifestRecord(_StrMixin, BaseRecord, kw_only=True):
    """A record acting as a manifest for a storable object."""

    __tablename__ = "labox_manifests"

    id: Mapped[UUID] = mapped_column(default_factory=uuid4, primary_key=True)
    """The ID of the manifest record."""
    tags: Mapped[TagMap | None] = mapped_column(JSON_OR_JSONB)
    """User defined tags associated with the object."""
    class_id: Mapped[UUID] = mapped_column()
    """An ID that uniquely identifies the type that was stored."""
    unpacker_name: Mapped[str] = mapped_column()
    """The name of the unpacker used to decompose the object into its constituent parts."""
    created_at: Mapped[DateTimeTZ] = mapped_column(default=func.now())
    """The timestamp when the manifest was created."""
    contents: Mapped[Sequence["ContentRecord"]] = relationship(default=(), collection_class=list)
    """The contents of the object."""


class SerializerTypeEnum(IntEnum):
    """An enumeration of the types of serializers."""

    Serializer = 1
    """A content serializer."""
    StreamSerializer = 2
    """A content stream serializer."""


class ContentRecord(_StrMixin, BaseRecord, kw_only=True):
    """A record describing where and how a piece of content was saved."""

    __tablename__ = "labox_contents"

    id: Mapped[UUID] = mapped_column(default_factory=uuid4, primary_key=True)
    """The ID of the content."""
    manifest_id: Mapped[UUID] = mapped_column(ForeignKey(ManifestRecord.id))
    """The ID of the manifest that the content belongs to."""
    content_key: Mapped[str] = mapped_column()
    """A string that uniquely identifies the content within the manifest."""
    content_type: Mapped[str] = mapped_column()
    """The MIME type of the data."""
    content_encoding: Mapped[str | None] = mapped_column()
    """The encoding of the data."""
    content_hash: Mapped[str] = mapped_column()
    """The hash of the data."""
    content_hash_algorithm: Mapped[str] = mapped_column()
    """The algorithm used to hash the data."""
    content_size: Mapped[int] = mapped_column(BIGINT())
    """The size of the data in bytes"""
    serializer_name: Mapped[str] = mapped_column()
    """The name of the serializer used to serialize the data."""
    serializer_config: Mapped[str] = mapped_column(RawJson)
    """The configuration used to serialize the data."""
    serializer_type: Mapped[SerializerTypeEnum] = mapped_column()
    """The type of the serializer used to serialize the data."""
    storage_name: Mapped[str] = mapped_column()
    """The name of the storage backend used to store the data."""
    storage_config: Mapped[str] = mapped_column(RawJson)
    """The information needed to load data from the storage."""
    created_at: Mapped[DateTimeTZ] = mapped_column(default=func.now())
    """The timestamp when the content was created."""


UniqueConstraint(
    ContentRecord.manifest_id,
    ContentRecord.content_key,
)


class _ValidJson(FunctionElement):
    """Validate that the value is a valid JSON object before inserting it into the database.

    Not all dialiects validate values inserted into JSON columns (e.g. SQLite).
    """

    name = "valid_json"
    inherit_cache = True

    def __init__(self, element: ColumnElement) -> None:
        self.element = element


@compiles(_ValidJson)
def _default_compile_valid_json(element: _ValidJson, compiler: SQLCompiler, **kw: Any) -> str:
    return compiler.process(element.element, **kw)


@compiles(_ValidJson, "sqlite")
def _sqlite_compile_valid_json(element: _ValidJson, compiler: SQLCompiler, **kw: Any) -> str:
    return f"json({compiler.process(element.element, **kw)})"
