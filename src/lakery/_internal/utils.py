from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from typing import TYPE_CHECKING
from typing import Any
from typing import TypeVar
from typing import cast
from typing import dataclass_transform
from typing import overload

T = TypeVar("T")

TagMap = Mapping[str, str]
"""A simple mapping from tag names to values."""

UNDEFINED = cast("Any", type("UNDEFINED", (), {"__repr__": lambda _: "UNDEFINED"})())
"""A sentinel value representing an undefined value."""

NON_ALPHANUMERIC = re.compile(r"[^a-z0-9]+")
"""Matches non-alphanumeric characters."""


def slugify(string: str) -> str:
    """Convert a string to a slug."""
    return NON_ALPHANUMERIC.sub("-", string.lower()).strip("-")


if TYPE_CHECKING:
    from collections.abc import Callable

    @overload
    def frozenclass(cls: type[T]) -> type[T]: ...

    @overload
    def frozenclass(
        cls: None = None,
        /,
        *,
        init: bool = True,
        repr: bool = True,
        eq: bool = True,
        order: bool = False,
        unsafe_hash: bool = False,
        frozen: bool = False,
        match_args: bool = True,
        kw_only: bool = False,
        slots: bool = False,
        weakref_slot: bool = False,
    ) -> Callable[[type[T]], type[T]]: ...

    @dataclass_transform(frozen_default=True, kw_only_default=True, field_specifiers=(field,))
    def frozenclass(*args: Any, **kwargs: Any) -> Any:
        """Create a dataclass that's frozen by default."""
        ...

else:

    def frozenclass(*args, **kwargs):
        """Create a dataclass that's frozen by default."""
        return dataclass(*args, **kwargs)


def full_class_name(cls: type) -> str:
    """Return the fully qualified name of a class."""
    return f"{cls.__module__}.{cls.__qualname__}"


_NAME_PATTERN = re.compile(r"^.*\@v\d+.*$")


def validate_versioned_class_name(cls: type) -> None:
    if not hasattr(cls, "name"):
        return
    name: str = cls.name
    if not _NAME_PATTERN.match(name):
        msg = (
            f"Expected a versioned name for {cls.__name__!r}, "
            f"of the form '<name>@v<version>', but got {name!r}."
        )
        raise ValueError(msg)
