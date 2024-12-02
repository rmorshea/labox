from collections.abc import Callable
from typing import Any
from typing import ParamSpec
from typing import TypeVar

P = ParamSpec("P")
R = TypeVar("R")


def copy_signature(_copy_from: Callable[P, R]) -> Callable[[Any], Callable[P, R]]:
    """Hint that the given function's signature should be copied to the decorated one."""

    def decorator(copy_to: Any) -> Callable[P, R]:
        return copy_to

    return decorator
