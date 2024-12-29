from __future__ import annotations

import operator
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from typing import TYPE_CHECKING
from typing import Annotated
from typing import Any
from typing import TypedDict
from typing import TypeVar
from typing import cast
from uuid import UUID
from uuid import uuid4

from sqlalchemy import ColumnElement
from sqlalchemy import DateTime
from sqlalchemy import UniqueConstraint
from sqlalchemy import func
from sqlalchemy.orm import ColumnProperty
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import MappedColumn
from sqlalchemy.orm import mapped_column
from sqlalchemy.sql import expression as sql

if TYPE_CHECKING:
    from sqlalchemy.sql.elements import NamedColumn

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


class Base(DeclarativeBase):
    """The base for lakery's schema classes."""


def archived_on(comparator: ColumnComparator = operator.eq) -> dict[str, Any]:
    """Info indicating that a column defines a unique constraint on the latest value.

    When saving a new record, if an existing one conflicts, the existing record will be
    "archived". A record has been "archived" if it's `rel_archived_at` is not `NEVER`.
    The process of archiving a record involves setting the `rel_archived_at` column to
    the current time before saving the new one.

    Args:
        comparator:
            The function that determins whether two records conflict. Accepts two
            arguments, the column and the value to compare against and should
            return a boolean expression.
    """
    return {"lakery.unique_on_comparator": comparator}


class DataRelation(Base):
    """A record describing a value's metadata in addition to where and how that value was saved."""

    __tablename__ = "lakery_data_relations"
    __mapper_args__: Mapping[str, Any] = {"polymorphic_on": "rel_type"}

    rel_id: Mapped[UUID] = mapped_column(default=uuid4, primary_key=True)
    """The ID of the relation."""
    rel_type: Mapped[str] = mapped_column(info=archived_on())
    """The type of relation."""
    rel_created_at: Mapped[DateTimeTZ] = mapped_column(default=func.now())
    """The timestamp when the pointer was created."""
    rel_archived_at: Mapped[DateTimeTZ] = mapped_column(default=NEVER, info=archived_on())
    """The timestamp when the pointer was archived."""
    rel_content_type: Mapped[str] = mapped_column()
    """The MIME type of the data."""
    rel_content_encoding: Mapped[str | None] = mapped_column()
    """The encoding of the data."""
    rel_content_hash: Mapped[str] = mapped_column()
    """The hash of the data."""
    rel_content_hash_algorithm: Mapped[str] = mapped_column()
    """The algorithm used to hash the data."""
    rel_content_size: Mapped[int] = mapped_column()
    """The size of the data in bytes"""
    rel_serializer_name: Mapped[str] = mapped_column()
    """The name of the serializer used to serialize the data."""
    rel_serializer_version: Mapped[int] = mapped_column()
    """The version of the serializer used to serialize the data."""
    rel_storage_name: Mapped[str] = mapped_column()
    """The name of the storage backend used to store the data."""
    rel_storage_version: Mapped[int] = mapped_column()
    """The version of the storage backend used to store the data."""

    def rel_select_latest(self) -> ColumnElement[bool]:
        """Get an expression to select the latest relation that conflicts with this one."""
        exprs: list[ColumnElement[bool]] = []
        for name, col, info in self._rel_column_into_items():
            match info:
                case {"lakery.unique_on_comparator": comparator}:
                    exprs.append(comparator(col, getattr(self, name)))
        return sql.and_(*exprs)

    @classmethod
    def _rel_init_subclass(cls) -> None:
        if cls.__mapper__.polymorphic_identity:
            cls._rel_make_unique_constraint()

    @classmethod
    def _rel_make_unique_constraint(cls) -> UniqueConstraint:
        """Get the unique constraint for the latest relations."""
        return UniqueConstraint(
            *[
                col
                for _, col, info in cls._rel_column_into_items()
                if "lakery.unique_on_comparator" in info
            ],
            **cls._rel_unique_constraint_kwargs(),
        )

    @classmethod
    def _rel_unique_constraint_kwargs(cls) -> dict[str, Any]:
        """Get the keyword arguments for the unique constraint for the latest relations."""
        return {}

    @classmethod
    def _rel_column_into_items(cls) -> Sequence[tuple[str, NamedColumn, DataRelationColumnInfo]]:
        """Get the columns with data relation info."""
        return [
            (k, col, info)
            for k, v in cls.__mapper__.attrs.items()
            if isinstance(v, ColumnProperty)
            and len(v.columns) == 1
            and (info := _get_column_info((col := v.columns[0]).info))
        ]

    if not TYPE_CHECKING:

        def __init_subclass__(cls, **kwargs: Any) -> None:
            super().__init_subclass__(**kwargs)
            cls._rel_init_subclass()


def _get_column_info(info: dict) -> DataRelationColumnInfo:
    """Get the data relation info from a column."""
    return cast(
        "DataRelationColumnInfo",
        {k: info[k] for k in info | DataRelationColumnInfo.__annotations__},
    )


DataRelationColumnInfo = TypedDict(
    "DataRelationColumnInfo",
    {"lakery.unique_on_comparator": ColumnComparator},
)
"""The info for a column that is part of a data relation."""
