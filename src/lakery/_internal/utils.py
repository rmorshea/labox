from __future__ import annotations

import re
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from functools import wraps
from typing import TYPE_CHECKING
from typing import Any
from typing import ParamSpec
from typing import TypedDict
from typing import TypeVar
from typing import cast
from typing import dataclass_transform
from typing import overload

T = TypeVar("T")
D = TypeVar("D", bound=Mapping[str, Any])
P = ParamSpec("P")
F = TypeVar("F", bound=Callable)

UNDEFINED = cast("Any", type("UNDEFINED", (), {"__repr__": lambda _: "UNDEFINED"})())
"""A sentinel value representing an undefined value."""

NON_ALPHANUMERIC = re.compile(r"[^a-z0-9]+")
"""Matches non-alphanumeric characters."""


def get_typed_dict(typed_dict: type[D], merged_dict: Mapping[str, Any], /) -> D:
    """Partition a merged dictionary into typed dictionaries."""
    extracted: dict[str, Any] = {}

    if TYPE_CHECKING:
        typed_dict_cls = TypedDict
    else:
        typed_dict_cls = typed_dict

    for k in typed_dict_cls.__required_keys__:
        if k in merged_dict:
            extracted[k] = merged_dict[k]
        else:
            msg = f"Missing required key '{k}' in {typed_dict_cls.__name__}."
            raise KeyError(msg)
    for k in typed_dict_cls.__optional_keys__:
        if k in merged_dict:
            extracted[k] = merged_dict[k]

    return cast("D", extracted)


def validate_typed_dict(typed_dict: type[D], raw_dict: Mapping[str, Any]) -> None:
    """Validate a dictionary against a TypedDict type."""
    if TYPE_CHECKING:
        typed_dict_cls = TypedDict
    else:
        typed_dict_cls = typed_dict

    req_keys = set(typed_dict_cls.__required_keys__)
    opt_keys = set(typed_dict_cls.__optional_keys__)

    extra_keys = set(raw_dict) - (req_keys | opt_keys)
    if extra_keys:
        msg = f"Unexpected keys in {typed_dict_cls.__name__}: {', '.join(sorted(extra_keys))}."
        raise KeyError(msg)

    missing_keys = req_keys - set(raw_dict)
    if missing_keys:
        msg = (
            f"Missing required keys in {typed_dict_cls.__name__}: "
            f"{', '.join(sorted(missing_keys))}."
        )
        raise KeyError(msg)


def slugify(string: str) -> str:
    """Convert a string to a slug."""
    return NON_ALPHANUMERIC.sub("-", string.lower()).strip("-")


if TYPE_CHECKING:

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


def not_implemented(f: F) -> F:
    """Return a method wrapper that raises NotImplementedError with a standard message."""

    @wraps(f)
    def wrapper(obj: Any, *args: Any, **kwargs: Any) -> Any:
        msg = f"{obj.__class__.__name__}.{f.__name__}"
        raise NotImplementedError(msg)

    return cast("F", wrapper)
