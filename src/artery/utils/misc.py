import re
from dataclasses import dataclass
from dataclasses import field
from typing import TYPE_CHECKING
from typing import Any
from typing import TypeVar
from typing import cast
from typing import dataclass_transform
from typing import overload

T = TypeVar("T")

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
