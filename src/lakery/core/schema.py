from __future__ import annotations

from collections.abc import Callable
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from enum import IntEnum
from typing import TYPE_CHECKING
from typing import Annotated
from typing import Any
from typing import TypeVar
from uuid import UUID
from uuid import uuid4

from anysync import coroutine
from sqlalchemy import JSON
from sqlalchemy import ColumnElement
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import UniqueConstraint
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import MappedColumn
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm.decl_api import MappedAsDataclass

from lakery.common.utils import TagMap  # noqa: TC001

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio.engine import AsyncEngine

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
"""A JSON type that uses JSONB in PostgreSQL."""


class BaseRecord(MappedAsDataclass, DeclarativeBase):
    """The base for lakery's core schema classes."""

    @classmethod
    @coroutine
    async def create_all(cls, engine: AsyncEngine) -> None:
        """Create all tables for the schema."""
        async with engine.begin() as conn:
            await conn.run_sync(cls.metadata.create_all)


class _StrMixin(BaseRecord):
    __abstract__ = True

    id: Any

    def __str__(self) -> str:  # nocov
        return f"{self.__class__.__name__}({self.id})"


class ManifestRecord(_StrMixin, BaseRecord, kw_only=True):
    """A record acting as a manifest for a stored model."""

    __abstract__ = False
    __tablename__ = "lakery_manifests"

    id: Mapped[UUID] = mapped_column(default_factory=uuid4, primary_key=True)
    """The ID of the stored model."""
    tags: Mapped[TagMap | None] = mapped_column(JSON_OR_JSONB)
    """User defined tags associated with the stored model."""
    model_id: Mapped[UUID] = mapped_column()
    """An ID that uniquely identifies the type of model that was stored."""
    model_version: Mapped[int] = mapped_column()
    """The version of the model that was stored."""
    created_at: Mapped[DateTimeTZ] = mapped_column(default=func.now())
    """The timestamp when the model was created."""
    contents: Mapped[Sequence[ContentRecord]] = relationship(default=(), collection_class=list)
    """The contents of the stored model."""


class SerializerTypeEnum(IntEnum):
    """An enumeration of the types of serializers."""

    Serializer = 1
    """A content serializer."""
    StreamSerializer = 2
    """A content stream serializer."""


class ContentRecord(_StrMixin, BaseRecord, kw_only=True):
    """A record describing where and how a piece of content was saved."""

    __tablename__ = "lakery_contents"

    id: Mapped[UUID] = mapped_column(default_factory=uuid4, primary_key=True)
    """The ID of the content."""
    manifest_id: Mapped[UUID] = mapped_column(ForeignKey(ManifestRecord.id))
    """The ID of the manifest that the content belongs to."""
    content_key: Mapped[str] = mapped_column()
    """A key that uniquely identifies the content within the manifest."""
    content_type: Mapped[str] = mapped_column()
    """The MIME type of the data."""
    content_encoding: Mapped[str | None] = mapped_column()
    """The encoding of the data."""
    content_hash: Mapped[str] = mapped_column()
    """The hash of the data."""
    content_hash_algorithm: Mapped[str] = mapped_column()
    """The algorithm used to hash the data."""
    content_size: Mapped[int] = mapped_column()
    """The size of the data in bytes"""
    serializer_name: Mapped[str] = mapped_column()
    """The name of the serializer used to serialize the data."""
    serializer_type: Mapped[SerializerTypeEnum] = mapped_column()
    """The type of the serializer used to serialize the data."""
    storage_name: Mapped[str] = mapped_column()
    """The name of the storage backend used to store the data."""
    storage_data: Mapped[Any] = mapped_column(JSON_OR_JSONB)
    """The information needed to load data from the storage."""
    created_at: Mapped[DateTimeTZ] = mapped_column(default=func.now())
    """The timestamp when the content was created."""


UniqueConstraint(
    ContentRecord.manifest_id,
    ContentRecord.content_key,
)
