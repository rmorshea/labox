from codecs import IncrementalDecoder
from collections.abc import AsyncIterable
from collections.abc import AsyncIterator


async def decode_byte_stream(
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
