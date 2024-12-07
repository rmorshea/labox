import json
from codecs import getincrementaldecoder
from collections.abc import AsyncIterable
from typing import TypeAlias

from anysync.core import AsyncIterator

from ardex.core.serializer import ScalarDump
from ardex.core.serializer import ScalarSerializer
from ardex.core.serializer import StreamDump
from ardex.core.serializer import StreamSerializer
from ardex.utils.stream import decode_byte_stream

JsonType: TypeAlias = int | str | float | bool | dict[str, "JsonType"] | list["JsonType"] | None
"""A type alias for JSON data."""

JSON_TYPES = (int, str, float, bool, type(None), dict, list)
"""The types that can be serialized to JSON."""


class JsonSerializer(ScalarSerializer[JsonType], StreamSerializer[JsonType]):
    """A serializer for JSON data."""

    name = "ardex.json"
    version = 1
    types = JSON_TYPES
    content_type = "application/json"

    def dump_scalar(self, value: JsonType) -> ScalarDump:
        """Serialize the given value to JSON."""
        return {
            "content_scalar": json.dumps(value).encode("utf-8"),
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_scalar(self, dump: ScalarDump) -> JsonType:
        """Deserialize the given JSON data."""
        return json.loads(dump["content_scalar"].decode("utf-8"))

    def dump_stream(self, stream: AsyncIterable[JsonType]) -> StreamDump:
        """Serialize the given stream of JSON data."""
        return {
            "content_stream": _dump_json_stream(stream),
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_stream(self, dump: StreamDump) -> AsyncIterator[JsonType]:
        """Deserialize the given stream of JSON data."""
        return _load_json_stream(dump["content_stream"])


async def _dump_json_stream(stream: AsyncIterable[JsonType]) -> AsyncIterator[bytes]:
    yield b"["
    first = True
    async for chunk in stream:
        content_body = json.dumps(chunk).encode("utf-8")
        if first:
            yield content_body
            first = False
        else:
            yield b"," + content_body
    yield b"]"


async def _load_json_stream(stream: AsyncIterable[bytes]) -> AsyncIterator[JsonType]:
    buffer = ""
    started = False
    decoder = json.JSONDecoder()
    async for chunk in decode_byte_stream(_GET_UTF_8_DECODER(), stream):
        if not started:
            buffer += chunk.lstrip()
            if not buffer.startswith("["):
                msg = f"Expected '[' at start of JSON stream, got {buffer!r}"
                raise ValueError(msg)
            buffer = buffer[1:]
            started = True
        else:
            buffer += chunk
        if not chunk:
            buffer = buffer.removesuffix("]")
        while buffer:
            try:
                buffer = buffer.lstrip()
                obj, index = decoder.raw_decode(buffer)
                yield obj
                buffer = buffer[index:].removeprefix(",")
            except json.JSONDecodeError:
                break
    if not started:
        msg = "Expected '[' at start of JSON stream, got EOF"
        raise ValueError(msg)
    if buffer:
        msg = f"Expected ']' at end of JSON stream, got {buffer!r}"
        raise ValueError(msg)


_GET_UTF_8_DECODER = getincrementaldecoder("utf-8")
