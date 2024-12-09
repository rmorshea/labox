import random
from collections.abc import AsyncIterator
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Sequence
from functools import partial
from inspect import isawaitable
from typing import Any
from typing import TypeVar

import pytest

from ardex.core.serializer import ScalarDump
from ardex.core.serializer import ScalarSerializer
from ardex.core.serializer import StreamDump
from ardex.core.serializer import StreamSerializer

T = TypeVar("T")


def make_scalar_serializer_test(
    serializer: ScalarSerializer[T],
    *cases: T,
) -> Callable:
    async def tester(checker, case):
        if isawaitable(result := checker(case)):
            await result

    matrix = [(partial(_check_dump_scalar_load_scalar, serializer, None), case) for case in cases]

    def get_id(x: Any) -> Any:
        wrapped = getattr(x, "func", x)
        return getattr(wrapped, "__name__", x)

    arg_names = ("checker", "case")
    return pytest.mark.parametrize(arg_names, matrix, ids=get_id)(tester)


def make_stream_serializer_test(
    serializer: StreamSerializer[T],
    *cases: Sequence[T],
) -> Callable:
    async def tester(checker, restream, case):
        if isawaitable(result := checker(restream, case)):
            await result

    matrix = []

    restreamers = (_stream_random_chunks, _stream_one_chunk, _stream_one_byte_chunks)

    for case in cases:
        matrix.append(
            (
                partial(_check_dump_scalar_load_scalar, serializer),
                None,
                case,
            )
        )
        matrix.extend(
            (
                partial(_check_dump_scalar_load_stream, serializer),
                restreamer,
                case,
            )
            for restreamer in restreamers
        )
        matrix.append(
            (
                partial(_check_dump_stream_load_scalar, serializer),
                None,
                case,
            )
        )
        matrix.extend(
            (
                partial(_check_dump_stream_load_stream, serializer),
                restreamer,
                case,
            )
            for restreamer in restreamers
        )

    def get_id(x: Any) -> Any:
        if hasattr(wrapped := getattr(x, "func", x), "__name__"):
            return wrapped.__name__.lstrip("_")
        return x

    arg_names = ("checker", "restream", "case")
    return pytest.mark.parametrize(arg_names, matrix, ids=get_id)(tester)


async def _check_dump_scalar_load_stream(
    serializer: StreamSerializer[Any],
    restream: Callable[[bytes], AsyncIterator[bytes]],
    value: Any,
) -> None:
    scalar_dump = serializer.dump_scalar(value)
    content_stream = restream(scalar_dump["content_scalar"])
    stream_dump: StreamDump = {
        "content_stream": content_stream,
        "content_type": scalar_dump["content_type"],
        "serializer_name": scalar_dump["serializer_name"],
        "serializer_version": scalar_dump["serializer_version"],
    }
    loaded_stream = serializer.load_stream(stream_dump)

    loaded_values = [value async for value in loaded_stream]

    if isinstance(value, Iterable) and not isinstance(value, str):
        assert loaded_values == [value] or loaded_values == list(value)
    else:
        assert loaded_values == [value]


async def _check_dump_stream_load_scalar(
    serializer: StreamSerializer[Any],
    restream: Any,
    values: Sequence[Any],
) -> None:
    content_stream = _to_async_iterable(values)
    stream_dump = serializer.dump_stream(content_stream)
    content_scalar = b"".join([chunk async for chunk in stream_dump["content_stream"]])
    scalar_dump: ScalarDump = {
        "content_scalar": content_scalar,
        "content_type": stream_dump["content_type"],
        "serializer_name": stream_dump["serializer_name"],
        "serializer_version": stream_dump["serializer_version"],
    }
    assert list(serializer.load_scalar(scalar_dump)) == list(values)  # type: ignore[reportArgumentType]


async def _check_dump_stream_load_stream(
    serializer: StreamSerializer[Any],
    restream: Callable[[bytes], AsyncIterator[bytes]],
    values: Sequence[Any],
) -> None:
    content_stream = _to_async_iterable(values)
    stream_dump = serializer.dump_stream(content_stream)
    stream = restream(b"".join([chunk async for chunk in stream_dump["content_stream"]]))
    loaded_stream = serializer.load_stream({**stream_dump, "content_stream": stream})
    assert [value async for value in loaded_stream] == list(values)


def _check_dump_scalar_load_scalar(
    serializer: ScalarSerializer[Any] | StreamSerializer[Any],
    restream: None,
    value: Any,
) -> None:
    assert serializer.load_scalar(serializer.dump_scalar(value)) == value


async def _to_async_iterable(iterable: Iterable[Any]) -> AsyncIterator[Any]:
    for value in iterable:
        yield value


async def _stream_random_chunks(content: bytes) -> AsyncIterator[bytes]:
    random.seed(0)  # make the test deterministic
    while content:
        chunk_size = random.randint(1, len(content))
        yield content[:chunk_size]
        content = content[chunk_size:]


async def _stream_one_chunk(content: bytes) -> AsyncIterator[bytes]:
    yield content


async def _stream_one_byte_chunks(content) -> AsyncIterator[bytes]:
    for i in range(len(content)):
        yield content[i : i + 1]
