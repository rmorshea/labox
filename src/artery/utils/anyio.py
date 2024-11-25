from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import Generic
from typing import ParamSpec
from typing import TypeVar

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from collections.abc import Callable
    from collections.abc import Coroutine

    from anyio.abc import TaskGroup

P = ParamSpec("P")
R = TypeVar("R")


def create_future(
    task_group: TaskGroup,
    func: Callable[P, Coroutine[Any, Any, R]],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> TaskGroupFuture[R]:
    """Create a future that is completed by a task group."""
    future = TaskGroupFuture[R]()
    task_group.start_soon(_set_future_result, func, args, kwargs, future)
    return future


async def _set_future_result(
    func: Callable[..., Awaitable[R]],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    future: TaskGroupFuture[R],
) -> None:
    future._result = await func(*args, **kwargs)  # noqa: SLF001


class TaskGroupFuture(Generic[R]):
    """A future that is completed by a task group."""

    __slots__ = "_result"

    _result: R

    def get(self) -> R:
        """Get the result of a future."""
        try:
            return self._result
        except AttributeError:
            msg = "Future not completed"
            raise RuntimeError(msg) from None
