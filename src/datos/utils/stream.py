from codecs import getincrementaldecoder
from collections.abc import AsyncIterable
from collections.abc import AsyncIterator


async def decode_utf8_byte_stream(stream: AsyncIterable[bytes]) -> AsyncIterator[str]:
    """Convert a stream of bytes to a stream of UTF-8 strings."""
    decoder = _Utf8Decoder()
    async for byte_chunk in stream:
        if str_chunk := decoder.decode(byte_chunk, final=False):
            yield str_chunk
    yield decoder.decode(b"", final=True)


_Utf8Decoder = getincrementaldecoder("utf-8")
