import random
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Sequence
from functools import partial
from typing import Any
from typing import TypeVar

import pytest

from lakery.core.serializer import Serializer
from lakery.core.serializer import StreamSerializer

T = TypeVar("T")

AssertionFunc = Callable[[T, T], None]


def _assert_equal(a: Any, b: Any) -> None:
    assert a == b


def make_value_serializer_test(
    serializer: Serializer[T],
    *cases: T,
    assertion: AssertionFunc[T] = _assert_equal,
) -> Callable:
    async def tester(checker, case):
        await checker(case)

    matrix = [(partial(_check_dump_value_load_value, assertion, serializer), c) for c in cases]

    def get_id(x: Any) -> Any:
        if hasattr(wrapped := getattr(x, "func", x), "__name__"):
            return wrapped.__name__.lstrip("_")
        return x

    arg_names = ("checker", "case")
    return pytest.mark.parametrize(arg_names, matrix, ids=get_id)(tester)


def make_stream_serializer_test(
    serializer: StreamSerializer[T],
    *cases: Sequence[T],
    assertion: AssertionFunc[T] = _assert_equal,
) -> Callable:
    async def tester(checker, restream, case):
        await checker(restream, case)

    matrix = []

    restreamers = (_stream_random_chunks, _stream_one_chunk, _stream_one_byte_chunks)

    for case in cases:
        matrix.extend(
            (
                partial(_check_dump_stream_load_stream, assertion, serializer),
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


async def _check_dump_stream_load_stream(
    assertion: AssertionFunc[T],
    serializer: StreamSerializer[Any],
    restream: Callable[[bytes], AsyncGenerator[bytes]],
    values: Sequence[Any],
) -> None:
    content = serializer.serialize_data_stream(_to_async_iterable(values))
    data_stream = restream(b"".join([chunk async for chunk in content["data_stream"]]))
    loaded_stream = serializer.deserialize_data_stream({**content, "data_stream": data_stream})
    assertion([value async for value in loaded_stream], list(values))  # type: ignore[reportArgumentType]


async def _check_dump_value_load_value(
    assertion: AssertionFunc[T],
    serializer: Serializer[Any],
    value: Any,
    conv: Callable[[Any], Any] = lambda x: x,
) -> None:
    assertion(conv(serializer.deserialize_data(serializer.serialize_data(value))), conv(value))


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
