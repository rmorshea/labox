from __future__ import annotations

import json
from codecs import getincrementaldecoder
from io import StringIO
from typing import TYPE_CHECKING

from lakery.common.streaming import decode_async_byte_stream
from lakery.core.serializer import SerializedData
from lakery.core.serializer import SerializedDataStream
from lakery.core.serializer import Serializer
from lakery.core.serializer import StreamSerializer

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable

from lakery.common.types import JsonStreamType
from lakery.common.types import JsonType

__all__ = (
    "JsonSerializer",
    "JsonStreamSerializer",
    "json_serializer",
    "json_stream_serializer",
)

JSON_SCALAR_TYPES = (int, str, float, bool, type(None), dict, list, tuple)
"""The types that can be serialized to JSON."""
JSON_STREAM_TYPES = (list, dict, tuple)
"""The types that can be serialized JSON in a stream."""
DEFAULT_JSON_ENCODER = json.JSONEncoder(separators=(",", ":"), allow_nan=False)
"""The default JSON encoder used for serialization."""
DEFAULT_JSON_DECODER = json.JSONDecoder()
"""The default JSON decoder used for deserialization."""


class _JsonSerializerBase:
    def __init__(
        self,
        encoder: json.JSONEncoder = DEFAULT_JSON_ENCODER,
        decoder: json.JSONDecoder = DEFAULT_JSON_DECODER,
    ) -> None:
        self.encoder = encoder
        self.decoder = decoder


class JsonSerializer(Serializer[JsonType], _JsonSerializerBase):
    """A serializer for JSON data."""

    name = "lakery.json.value@v1"
    types = JSON_SCALAR_TYPES
    content_type = "application/json"

    def serialize_data(self, value: JsonType) -> SerializedData:
        """Serialize the given value to JSON."""
        return {
            "content_encoding": "utf-8",
            "content_type": self.content_type,
            "data": self.encoder.encode(value).encode("utf-8"),
        }

    def deserialize_data(self, content: SerializedData) -> JsonType:
        """Deserialize the given JSON data."""
        return self.decoder.decode(content["data"].decode("utf-8"))


class JsonStreamSerializer(StreamSerializer[JsonStreamType], _JsonSerializerBase):
    """A serializer for JSON data."""

    name = "lakery.json.stream@v1"
    types = JSON_STREAM_TYPES
    content_type = "application/json"

    def serialize_data_stream(self, stream: AsyncIterable[JsonStreamType]) -> SerializedDataStream:
        """Serialize the given stream of JSON data."""
        return {
            "content_encoding": "utf-8",
            "content_type": self.content_type,
            "data_stream": _dump_json_stream(self.encoder, stream),
        }

    def deserialize_data_stream(
        self, content: SerializedDataStream
    ) -> AsyncGenerator[JsonStreamType]:
        """Deserialize the given stream of JSON data."""
        return _load_json_stream(self.decoder, content["data_stream"])


json_serializer = JsonSerializer()
"""JsonSerializer with default settings."""
json_stream_serializer = JsonStreamSerializer()
"""JsonStreamSerializer with default settings."""


async def _dump_json_stream(
    encoder: json.JSONEncoder,
    stream: AsyncIterable[JsonStreamType],
) -> AsyncGenerator[bytes]:
    yield b"["
    buffer = StringIO()
    async for chunk in stream:
        if not isinstance(chunk, list | dict):
            msg = f"Expected list or dict of JSON data, got {chunk!r}"
            raise TypeError(msg)
        data = encoder.encode(chunk)
        buffer.write(data)
        yield buffer.getvalue().encode("utf-8")
        buffer.seek(0)
        buffer.write(",")
        buffer.truncate()
    yield b"]"


async def _load_json_stream(
    decoder: json.JSONDecoder,
    stream: AsyncIterable[bytes],
) -> AsyncGenerator[JsonStreamType]:
    buffer = StringIO()
    started = False
    async for chunk in decode_async_byte_stream(_GET_UTF_8_DECODER(), stream):
        if not started:
            if not chunk.startswith("["):
                msg = f"Expected '[' at start of JSON stream, got {chunk!r}"
                raise ValueError(msg)
            buffer.write(chunk[1:])
            started = True
        elif not chunk:
            continue
        else:
            buffer.write(chunk)

        pos = 0
        buffer.seek(pos)
        while value := buffer.read():
            try:
                offset = 1 if value.startswith(",") else 0
                obj, index = decoder.raw_decode(value[offset:])
                yield obj
                pos += index + offset
                buffer.seek(pos)
            except json.JSONDecodeError:
                buffer.seek(pos)
                break

        remainder = buffer.read()
        buffer.seek(0)
        buffer.write(remainder)
        buffer.truncate()

    if (remainder := buffer.getvalue()) and remainder != "]":
        msg = f"Expected end of JSON stream, got {remainder!r}"
        raise ValueError(msg)


_GET_UTF_8_DECODER = getincrementaldecoder("utf-8")
