from __future__ import annotations

from contextlib import AbstractContextManager
from contextlib import suppress
from typing import TYPE_CHECKING
from typing import Any
from typing import Generic
from typing import ParamSpec
from typing import TypeVar

from anyio import ClosedResourceError
from anyio import create_memory_object_stream
from anyio.from_thread import run_sync as run_sync_from_thread
from anyio.to_thread import run_sync as run_sync_to_thread

from labox._internal._utils import UNDEFINED

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from collections.abc import Callable
    from collections.abc import Coroutine
    from collections.abc import Iterator

    from anyio.abc import TaskGroup
    from anyio.streams.memory import MemoryObjectReceiveStream

P = ParamSpec("P")
R = TypeVar("R")
D = TypeVar("D")


def as_async_iterator(
    task_group: TaskGroup,
    sync_iter: Iterator[R],
    *,
    max_buffer_size: int = 0,
) -> AbstractContextManager[MemoryObjectReceiveStream[R]]:
    """Create an asynchronous iterator from a synchronous iterator.

    Args:
        task_group: The task group to run the asynchronous tasks in.
        sync_iter: The synchronous iterator to convert.
        max_buffer_size: The maximum buffer size for the memory object stream.
    """
    send, recv = create_memory_object_stream[R](max_buffer_size)

    def exhaust_sync_iter():
        try:
            with suppress(ClosedResourceError):
                for value in sync_iter:
                    run_sync_from_thread(send.send_nowait, value)
        finally:
            run_sync_from_thread(send.close)

    task_group.start_soon(run_sync_to_thread, exhaust_sync_iter)
    return recv


def start_future(
    task_group: TaskGroup,
    func: Callable[P, Coroutine[Any, Any, R]],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> TaskFuture[R]:
    """Start a future in a task group."""
    future: TaskFuture[R] = TaskFuture()
    start_with_future(task_group, future, func, *args, **kwargs)
    return future


def start_with_future(
    task_group: TaskGroup,
    future: TaskFuture[R],
    func: Callable[P, Coroutine[Any, Any, R]],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> None:
    """Start the given future in a task group."""
    task_group.start_soon(_evaluate_future, func, args, kwargs, future)


class TaskFuture(Generic[R]):
    """A result that will be completed in the future by a task group."""

    _value: R = UNDEFINED
    _exception: BaseException = UNDEFINED

    @property
    def exception(self) -> BaseException | None:
        """Get the exception that caused the future to fail (if any)."""
        return None if (exc := self._exception) is UNDEFINED else exc

    @property
    def value(self) -> R:
        """Get the result of a future."""
        if self._exception is not UNDEFINED:
            raise self._exception
        if self._value is UNDEFINED:
            msg = "Future has not been set yet"
            raise RuntimeError(msg)
        return self._value

    def set_value(self, value: R) -> None:
        """Set the value of the future."""
        if self._value is not UNDEFINED or self._exception is not UNDEFINED:
            msg = "Future has already been set"
            raise RuntimeError(msg)
        self._value = value

    def set_exception(self, exception: BaseException) -> None:
        """Set the exception of the future."""
        if self._value is not UNDEFINED or self._exception is not UNDEFINED:
            msg = "Future has already been set"
            raise RuntimeError(msg)
        self._exception = exception


async def _evaluate_future(
    func: Callable[..., Awaitable[R]],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    future: TaskFuture[R],
) -> None:
    try:
        returned = await func(*args, **kwargs)
    except BaseException as exc:
        future.set_exception(exc)
        raise
    else:
        future.set_value(returned)
