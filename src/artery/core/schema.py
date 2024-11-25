from datetime import UTC
from datetime import datetime
from typing import Annotated
from uuid import UUID
from uuid import uuid4

from sqlalchemy import DateTime
from sqlalchemy import UniqueConstraint
from sqlalchemy import func
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import MappedAsDataclass
from sqlalchemy.orm import mapped_column

from artery.core.serializer import SerializerRegistry
from artery.core.storage import StorageRegistry

DateTimeTZ = Annotated[datetime, mapped_column(DateTime(timezone=True))]
"""A datetime column with timezone information."""

NEVER = datetime.max.replace(tzinfo=UTC)
"""A timestamp representing never.

There is no way to represent an infinite datetime in Python so we resort to using the
largest representable value. Some databases have support for infinite datetimes but this
is not a universal feature. If it's desireable to map datetime.max to infinity you may
be able to register an extension with your driver of choice in order to do so. For
example, with `psycopg3` allows you to implement a custom dumper/loader for datetimes:
https://www.psycopg.org/psycopg3/docs/advanced/adapt.html#example-handling-infinity-date
"""


class Base(DeclarativeBase):
    """The base for all schema classes."""


class Record(Base, MappedAsDataclass, kw_only=True):
    """A reference to a piece of data stored in a storage backend."""

    id: Mapped[UUID] = mapped_column(default_factory=uuid4)
    """The unique identifier of the pointer."""

    content_type: Mapped[str] = mapped_column()
    """The MIME type of the data."""
    content_size: Mapped[int] = mapped_column()
    """The size of the data in bytes."""
    content_hash: Mapped[str] = mapped_column()
    """The hash of the data."""
    content_hash_algorithm: Mapped[str] = mapped_column()
    """The algorithm used to hash the data."""

    serializer_name: Mapped[str] = mapped_column()
    """The name of the serializer used to serialize the data."""
    serialier_version: Mapped[int] = mapped_column()
    """The version of the serializer used to serialize the data."""
    storage_name: Mapped[str] = mapped_column()
    """The name of the storage backend used to store the data."""
    storage_version: Mapped[int] = mapped_column()
    """The version of the storage backend used to store the data."""

    record_name: Mapped[str] = mapped_column()
    """The name of the artifact."""
    record_created_at: Mapped[DateTimeTZ] = mapped_column(default=func.now())
    """The timestamp when the pointer was created."""
    record_updated_at: Mapped[DateTimeTZ] = mapped_column(default=func.now(), onupdate=func.now())
    """The timestamp when the pointer was last updated."""
    record_archived_at: Mapped[DateTimeTZ] = mapped_column(default=NEVER)
    """The timestamp when the pointer was archived."""

    def __post_init__(
        self,
        *,
        _storage_registry: StorageRegistry,
        _serializer_registry: SerializerRegistry,
    ) -> None:
        if self.storage_name not in _storage_registry.by_name:
            msg = f"Unknown storage: {self.storage_name}"
            raise ValueError(msg)
        if self.serializer_name not in _serializer_registry.by_name:
            msg = f"Unknown serializer: {self.serializer_name}"
            raise ValueError(msg)

    __tablename__ = "artery_record"
    __table_args__ = (
        UniqueConstraint(
            record_archived_at,
            record_name,
            name="uq_artifact_name_archive_at",
        ),
    )
