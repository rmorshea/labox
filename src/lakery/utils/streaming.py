from codecs import IncrementalDecoder
from collections.abc import AsyncIterable
from collections.abc import AsyncIterator
from collections.abc import Iterator
from io import RawIOBase
from threading import Lock as ThreadLock
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
    if last:
        yield ""


class ByteStreamReader(RawIOBase, IO[bytes]):
    """A file-like wrapper around a stream of bytes."""

    def __init__(self, stream: Iterator[bytes]):
        super().__init__()
        self._stream = stream
        self._buffer = b""
        self._lock = ThreadLock()
        self._closed = False

    def readable(self):
        """Whether the stream is readable - True."""
        return True

    def seekable(self) -> bool:
        """Whether the stream is seekable - False."""
        return False

    def writable(self) -> bool:
        """Whether the stream is writable - False."""
        return False

    def read(self, size=-1):
        """Read at most size bytes from the stream - unused bytes are buffered."""
        if self._closed:
            msg = "Cannot read from a closed stream."
            raise ValueError(msg)
        with self._lock:
            if size == -1:  # Read all
                chunks = [self._buffer, *self._stream]
                self._buffer = b""
                return b"".join(chunks)
            while len(self._buffer) < size:
                try:
                    self._buffer += next(self._stream)
                except StopIteration:
                    break
            result, self._buffer = self._buffer[:size], self._buffer[size:]
            return result

    def close(self):
        """Close the stream."""
        self._closed = True
        super().close()

    @property
    def closed(self):
        """Whether the stream is closed."""
        return self._closed
