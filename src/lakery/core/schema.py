from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from datetime import UTC
from datetime import datetime
from typing import Annotated
from typing import Any
from typing import NewType
from typing import TypeVar
from uuid import UUID
from uuid import uuid4

from sqlalchemy import JSON
from sqlalchemy import ColumnElement
from sqlalchemy import DateTime
from sqlalchemy import UniqueConstraint
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import MappedColumn
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm.decl_api import MappedAsDataclass
from sqlalchemy.sql import expression as sql

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


class Base(DeclarativeBase):
    """The base for lakery's core schema classes."""


DATA_DESCRIPTOR_NAME_SEP = "."
"""The separator for data descriptor names."""


def is_data_descriptor_name(name: str) -> bool:
    """Check if a string is a valid data descriptor name."""
    return all(map(str.isidentifier, name.split(DATA_DESCRIPTOR_NAME_SEP)))


DataDescriptorName = NewType("DataDescriptorName", str)
"""A unique name for a data descriptor."""


class DataDescriptor(Base):
    """A record with additional information about a one or more data pointers."""

    __tablename__ = "lakery_data_descriptor"
    __mapper_args__: Mapping[str, Any] = {
        "polymorphic_on": "data_type",
        "polymorphic_identity": "default",
    }

    descriptor_id: Mapped[UUID] = mapped_column(default=uuid4, primary_key=True)
    """The ID of the data descriptor."""
    descriptor_type: Mapped[str] = mapped_column()
    """The type of the descriptor."""
    descriptor_name: Mapped[str] = mapped_column()
    """The name of the descriptor."""
    descriptor_created_at: Mapped[DateTimeTZ] = mapped_column(default=func.now())
    """The timestamp when the descriptor was created."""
    descriptor_archived_at: Mapped[DateTimeTZ] = mapped_column(default=NEVER)
    """The timestamp when the descriptor was archived."""
    descriptor_storage_model_id: Mapped[UUID] = mapped_column()
    """The name of the model that the data came from."""
    descriptor_storage_model_version: Mapped[int] = mapped_column()
    """The version of the model that the data came from."""

    def where_latest(self) -> ColumnElement[bool]:
        """Get an expression to select the latest that conflicts with this one."""
        return sql.and_(
            DataDescriptor.descriptor_type == self.descriptor_type,
            sql.or_(
                ((name := DataDescriptor.descriptor_name) == self.descriptor_name),
                DataDescriptor.descriptor_name.startswith(name + DATA_DESCRIPTOR_NAME_SEP),
            ),
            DataDescriptor.descriptor_archived_at == NEVER,
        )


# Ensure that the name and type are unique for the latest record.
UniqueConstraint(
    DataDescriptor.descriptor_type,
    DataDescriptor.descriptor_name,
    DataDescriptor.descriptor_archived_at,
)


class DataRecord(MappedAsDataclass, Base, kw_only=True):
    """A record describing where and how data was saved."""

    __tablename__ = "lakery_data_pointer"

    id: Mapped[UUID] = mapped_column(default_factory=uuid4, primary_key=True)
    """The ID of the pointer."""
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
    serializer_version: Mapped[int] = mapped_column()
    """The version of the serializer used to serialize the data."""
    storage_name: Mapped[str] = mapped_column()
    """The name of the storage backend used to store the data."""
    storage_version: Mapped[int] = mapped_column()
    """The version of the storage backend used to store the data."""
    storage_info: Mapped[Any] = mapped_column(JSON_OR_JSONB)
    """Info returned by the storage backend to locate the data."""
    storage_model_key: Mapped[str | None] = mapped_column()
    """The key of the data within the storage model."""


class DataDescriptorPointer(Base):
    """An association between a descriptor and a pointer."""

    __tablename__ = "lakery_data_descriptor_pointer"

    descriptor_id: Mapped[UUID] = mapped_column(
        primary_key=True, foreign_key=DataDescriptor.descriptor_id
    )
    """The ID of the descriptor."""
    pointer_id: Mapped[UUID] = mapped_column(primary_key=True, foreign_key=DataRecord.id)
    """The ID of the pointer."""
