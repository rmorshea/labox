from codecs import IncrementalDecoder
from collections.abc import AsyncIterable
from collections.abc import AsyncIterator
from typing import IO
from typing import TypeVar

T = TypeVar("T")


async def decode_async_byte_stream(
    decoder: IncrementalDecoder,
    stream: AsyncIterable[bytes],
) -> AsyncIterator[str]:
    """Convert a stream of bytes to a stream of UTF-8 strings - yields empty string on EOF."""
    async for byte_chunk in stream:
        if str_chunk := decoder.decode(byte_chunk, final=False):
            yield str_chunk
    yield (last := decoder.decode(b"", final=True))
    if last:  # nocov (not worth trying to test this)
        yield ""


async def write_async_byte_stream_into(
    stream: AsyncIterable[bytes],
    buffer: IO[bytes],
    *,
    min_size: int,
) -> int | None:
    """Write at least min_size bytes into the buffer and return the number of bytes written."""
    wrote = 0
    start = buffer.tell()
    async for byte_chunk in stream:
        buffer.write(byte_chunk)
        if (wrote := buffer.tell() - start) >= min_size:
            break
    return wrote
