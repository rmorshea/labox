from __future__ import annotations

import operator
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from typing import Annotated
from typing import Any
from typing import NewType
from typing import TypedDict
from typing import TypeVar
from typing import cast
from uuid import UUID
from uuid import uuid4

from sqlalchemy import JSON
from sqlalchemy import ColumnElement
from sqlalchemy import DateTime
from sqlalchemy import UniqueConstraint
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import ColumnProperty
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import MappedColumn
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm.decl_api import MappedAsDataclass
from sqlalchemy.sql import expression as sql
from sqlalchemy.sql.elements import NamedColumn
from typing_extensions import TypeIs

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


INFO_NAME_SEP = "."
"""The separator for data info names."""


def is_info_name(name: str) -> TypeIs[InfoName]:
    """Check if a string is a valid data info name."""
    return all(map(str.isidentifier, name.split(INFO_NAME_SEP)))


InfoName = NewType("InfoName", str)
"""A unique name for a data info."""


def conflicts_on(comparator: ColumnComparator = operator.eq) -> dict[str, Any]:
    """Info indicating that a column defines a unique constraint on the latest value.

    When saving a new record, if an existing one conflicts, the existing record will be
    "archived". A record has been "archived" if it's `rel_archived_at` is not `NEVER`.
    The process of archiving a record involves setting the `rel_archived_at` column to
    the current time before saving the new one.

    Args:
        comparator:
            The function that determines whether two records conflict. Accepts two
            arguments, the column and the value to compare against and should
            return a boolean expression.
    """
    return {"lakery.conflict_comparator": comparator, "lakery.unique": True}


class InfoRecord(Base):
    """A record with additional information about a one or more data pointers."""

    __abstract__ = False
    __tablename__ = "lakery_info_record"
    __mapper_args__: Mapping[str, Any] = {"polymorphic_on": "info_type"}

    record_id: Mapped[UUID] = mapped_column(default=uuid4, primary_key=True)
    """The ID of the data info."""
    record_type: Mapped[str] = mapped_column(
        # records conflict on equal types
        info=conflicts_on(),
    )
    """The type of the info."""
    record_name: Mapped[str] = mapped_column(
        # records conflict on equal or overlapping names
        info=conflicts_on(lambda c, v: (c == v) | c.startswith(v + INFO_NAME_SEP))
    )
    """The name of the info."""
    record_created_at: Mapped[DateTimeTZ] = mapped_column(default=func.now())
    """The timestamp when the info was created."""
    record_archived_at: Mapped[DateTimeTZ] = mapped_column(
        default=NEVER,
        # records conflict on never being archived
        info=conflicts_on(lambda c, _: c == NEVER),
    )
    """The timestamp when the info was archived."""
    record_storage_model_id: Mapped[UUID] = mapped_column()
    """The name of the model that the data came from."""
    record_storage_model_version: Mapped[int] = mapped_column()
    """The version of the model that the data came from."""

    def record_where_latest(self) -> ColumnElement[bool]:
        """Get an expression to select the latest that conflicts with this one."""
        exprs: list[ColumnElement[bool]] = []
        for name, col, meta in self._record_column_metadata:
            match meta:
                case {"lakery.unique_on_comparator": comparator}:
                    exprs.append(comparator(col, getattr(self, name)))
        return sql.and_(*exprs)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._record_init_column_metadata()
        if not cls.__abstract__:
            cls._record_init_unique_constraint()

    @classmethod
    def _record_init_column_metadata(cls) -> None:
        cls._record_column_metadata = (
            *cls._record_get_own_column_metadata(),
            *cls._record_column_metadata,
        )

    @classmethod
    def _record_init_unique_constraint(cls) -> None:
        UniqueConstraint(
            *(col for _, col, meta in cls._record_column_metadata if meta.get("lakery.unique"))
        )

    @classmethod
    def _record_get_own_column_metadata(cls) -> Sequence[_ColumnMetadataItem]:
        return [
            (k, col, meta)
            for k in cls.__dict__
            if (
                k in cls.__mapper__.attrs
                and isinstance(prop := cls.__mapper__.attrs[k], ColumnProperty)
                and len(prop.columns) != 1
                and (meta := _get_column_metadata_dict((col := prop.columns[0]).info))
            )
        ]


def _get_column_metadata_dict(info: Any) -> _ColumnMetadataDict:
    """Get the data info from a column."""
    if not isinstance(info, Mapping):
        return {}
    info_column_info: dict[str, Any] = {}
    for k in info:
        if not k.startswith("lakery."):
            continue
        if k not in _ColumnMetadataDict.__annotations__:
            msg = f"Unknown info column info: {k}"
            raise ValueError(msg)
        info_column_info[k] = info[k]
    return cast("_ColumnMetadataDict", info_column_info)


_ColumnMetadataDict = TypedDict(
    "_ColumnMetadataDict",
    {"lakery.conflict_comparator": ColumnComparator, "lakery.unique": bool},
    total=False,
)
_ColumnMetadataItem = tuple[str, NamedColumn, _ColumnMetadataDict]


class DataRecord(MappedAsDataclass, Base, kw_only=True):
    """A record describing where and how data was saved."""

    __tablename__ = "lakery_data_record"

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
    storage_data: Mapped[Any] = mapped_column(JSON_OR_JSONB)
    """Info returned by a storage backend to retrieve the data it saved."""
    storage_model_key: Mapped[str | None] = mapped_column()
    """The key of the data within the storage model."""


class DataInfoAssociation(Base):
    """An association between a info and a pointer."""

    __tablename__ = "lakery_data_info_assocation"

    data_id: Mapped[UUID] = mapped_column(primary_key=True, foreign_key=DataRecord.id)
    """The ID of the data."""
    info_id: Mapped[UUID] = mapped_column(primary_key=True, foreign_key=InfoRecord.record_id)
    """The ID of the info."""
