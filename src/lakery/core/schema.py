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
from typing import Literal
from typing import TypedDict
from typing import TypeVar
from typing import overload
from uuid import UUID
from uuid import uuid4

from sqlalchemy import ColumnElement
from sqlalchemy import DateTime
from sqlalchemy import UniqueConstraint
from sqlalchemy import func
from sqlalchemy.orm import ColumnProperty
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import MappedAsDataclass
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


class Base(MappedAsDataclass, DeclarativeBase, kw_only=True):
    """The base for lakery's schema classes."""


_INFO_KEY = "__lakery_info__"


def is_unique(where: ColumnComparator = operator.eq) -> dict:
    """Indicate that a column defines a unique constraint on the latest value."""
    return {_INFO_KEY: {"unique_on": {"comparator": where}}}


class DataRelation(Base):
    """A record describing a value's metadata in addition to where and how that value was saved."""

    __tablename__ = "lakery_data_relations"
    __mapper_args__: Mapping[str, Any] = {"polymorphic_on": "rel_type"}

    rel_id: Mapped[UUID] = mapped_column(init=False, default_factory=uuid4, primary_key=True)
    """The ID of the relation."""
    rel_type: Mapped[str] = mapped_column(init=False, info=is_unique())
    """The type of relation."""
    rel_created_at: Mapped[DateTimeTZ] = mapped_column(init=False, default=func.now())
    """The timestamp when the pointer was created."""
    rel_archived_at: Mapped[DateTimeTZ] = mapped_column(init=False, default=NEVER, info=is_unique())
    """The timestamp when the pointer was archived."""
    rel_content_type: Mapped[str] = mapped_column(init=False)
    """The MIME type of the data."""
    rel_content_encoding: Mapped[str | None] = mapped_column(init=False)
    """The encoding of the data."""
    rel_content_hash: Mapped[str] = mapped_column(init=False)
    """The hash of the data."""
    rel_content_hash_algorithm: Mapped[str] = mapped_column(init=False)
    """The algorithm used to hash the data."""
    rel_content_size: Mapped[int] = mapped_column(init=False)
    """The size of the data in bytes"""
    rel_serializer_name: Mapped[str] = mapped_column(init=False)
    """The name of the serializer used to serialize the data."""
    rel_serializer_version: Mapped[int] = mapped_column(init=False)
    """The version of the serializer used to serialize the data."""
    rel_storage_name: Mapped[str] = mapped_column(init=False)
    """The name of the storage backend used to store the data."""
    rel_storage_version: Mapped[int] = mapped_column(init=False)
    """The version of the storage backend used to store the data."""

    def rel_select_latest(self) -> ColumnElement[bool]:
        """Get the expression to select the latest relation that conflicts with this one."""
        exprs: list[ColumnElement[bool]] = []
        for name, col in self._rel_columns_with_data_relation_info().items():
            match _get_column_info(col.info):
                case {"latest": {"comparator": comparator}}:
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
            *cls._rel_unique_constraint_columns(),
            **cls._rel_unique_constraint_kwargs(),
        )

    @classmethod
    def _rel_unique_constraint_columns(cls) -> Sequence[NamedColumn]:
        """Get the columns that make up the unique constraint for the latest relations."""
        return [
            col
            for col in cls._rel_columns_with_data_relation_info().values()
            if "latest" in _get_column_info(col.info)
        ]

    @classmethod
    def _rel_unique_constraint_kwargs(cls) -> dict[str, Any]:
        """Get the keyword arguments for the unique constraint for the latest relations."""
        return {}

    @classmethod
    def _rel_columns_with_data_relation_info(cls) -> dict[str, NamedColumn]:
        """Get the columns with data relation info."""
        return {
            k: v.columns[0]
            for k, v in cls.__mapper__.attrs.items()
            if isinstance(v, ColumnProperty)
            and len(v.columns) == 1
            and _get_column_info(v.info, missing_ok=True)
        }

    if not TYPE_CHECKING:

        def __init_subclass__(cls, **kwargs: Any) -> None:
            super().__init_subclass__(**kwargs)
            cls._rel_init_subclass()


@overload
def _get_column_info(
    info: dict, *, missing_ok: Literal[True]
) -> _DataRelationColumnInfo | None: ...


@overload
def _get_column_info(
    info: dict, *, missing_ok: Literal[False] = ...
) -> _DataRelationColumnInfo: ...


def _get_column_info(info: dict, *, missing_ok: bool = False) -> _DataRelationColumnInfo | None:
    """Get the data relation info from a column."""
    if missing_ok:
        return info.get(_INFO_KEY)
    else:
        try:
            return info[_INFO_KEY]
        except KeyError:
            msg = f"Missing data relation info in {info}"
            raise ValueError(msg) from None


def _set_column_info(info: dict, data_relation_info: _DataRelationColumnInfo) -> None:
    """Set the data relation info on a column."""
    info[_INFO_KEY] = data_relation_info


class _DataRelationColumnInfo(TypedDict, total=False):
    """A dictionary of metadata about a column."""

    unique_on: _UniqueOnInfo


class _UniqueOnInfo(TypedDict):
    """A dictionary of metadata about the latest value of a column."""

    comparator: ColumnComparator
