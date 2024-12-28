from __future__ import annotations

from contextlib import AbstractContextManager
from contextlib import suppress
from typing import TYPE_CHECKING
from typing import Any
from typing import Generic
from typing import ParamSpec
from typing import TypeVar
from typing import overload

from anyio import ClosedResourceError
from anyio import create_memory_object_stream
from anyio.from_thread import run_sync as run_sync_from_thread
from anyio.to_thread import run_sync as run_sync_to_thread

from lakery.common.utils import UNDEFINED

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


def start_async_iterator(
    task_group: TaskGroup, sync_iter: Iterator[R]
) -> AbstractContextManager[MemoryObjectReceiveStream[R]]:
    """Create an asynchronous iterator from a synchronous iterator."""
    send, recv = create_memory_object_stream[R]()

    def exhause_sync_iter():
        try:
            with suppress(ClosedResourceError):
                for value in sync_iter:
                    run_sync_from_thread(send.send_nowait, value)
        finally:
            run_sync_from_thread(send.close)

    task_group.start_soon(run_sync_to_thread, exhause_sync_iter)
    return recv


def start_given_future(
    task_group: TaskGroup,
    future: TaskGroupFuture[R],
    func: Callable[P, Coroutine[Any, Any, R]],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> None:
    """Start the given future in a task group."""
    task_group.start_soon(_set_future_result, func, args, kwargs, future)


async def _set_future_result(
    func: Callable[..., Awaitable[R]],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    future: TaskGroupFuture[R],
) -> None:
    try:
        future._result = await func(*args, **kwargs)  # noqa: SLF001
    except BaseException as exc:
        future._exception = exc  # noqa: SLF001
        raise


class TaskGroupFuture(Generic[R]):
    """A future that is completed by a task group."""

    _result: R
    _exception: BaseException

    @overload
    def result(self) -> R: ...

    @overload
    def result(self, default: D) -> R | D: ...

    def result(self, default: D = UNDEFINED) -> R | D:
        """Get the result of a future."""
        try:
            return self._result
        except AttributeError:
            try:
                raise self._exception from None
            except AttributeError:
                if default is not UNDEFINED:
                    return default
                msg = "Future not completed"
                raise RuntimeError(msg) from None
