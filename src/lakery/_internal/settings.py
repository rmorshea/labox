from collections.abc import Callable
from os import environ
from typing import TypeVar

T = TypeVar("T")


def make_setting(
    name: str,
    default: T,
    from_string: Callable[[str], T] = lambda x: x,
) -> Callable[[], T]:
    """Create a setting with a name, default value, and optional conversion function."""
    return lambda: from_string(environ[name]) if name in environ else default


LAKERY_FORWARD_WARNINGS = make_setting(
    "LAKERY_FORWARD_WARNINGS",
    True,
    from_string=lambda x: x.lower() in ("true", "1", "yes"),
)
"""Whether to forward warnings to the caller's stack frame."""
