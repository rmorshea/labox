from __future__ import annotations

import json
from codecs import getincrementaldecoder
from io import StringIO
from typing import TYPE_CHECKING

from lakery._internal._json import DEFAULT_JSON_DECODER
from lakery._internal._json import DEFAULT_JSON_ENCODER
from lakery._internal._json import JSON_TYPES
from lakery.common.streaming import decode_async_byte_stream
from lakery.core.serializer import SerializedData
from lakery.core.serializer import SerializedDataStream
from lakery.core.serializer import Serializer
from lakery.core.serializer import StreamSerializer

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable

from lakery.common.types import JsonType

__all__ = (
    "DEFAULT_JSON_DECODER",
    "DEFAULT_JSON_ENCODER",
    "JSON_TYPES",
    "JsonSerializer",
    "JsonStreamSerializer",
    "json_serializer",
    "json_stream_serializer",
)


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
    types = JSON_TYPES
    content_types = ("application/json",)

    def serialize_data(self, value: JsonType) -> SerializedData:
        """Serialize the given value to JSON."""
        return {
            "content_encoding": "utf-8",
            "content_type": "application/json",
            "data": self.encoder.encode(value).encode("utf-8"),
        }

    def deserialize_data(self, content: SerializedData) -> JsonType:
        """Deserialize the given JSON data."""
        return self.decoder.decode(content["data"].decode("utf-8"))


class JsonStreamSerializer(StreamSerializer[JsonType], _JsonSerializerBase):
    """A serializer for JSON data."""

    name = "lakery.json.stream@v1"
    types = JSON_TYPES
    content_types = ("application/json",)

    def serialize_data_stream(self, stream: AsyncIterable[JsonType]) -> SerializedDataStream:
        """Serialize the given stream of JSON data."""
        return {
            "content_encoding": "utf-8",
            "content_type": "application/json",
            "data_stream": _dump_json_stream(self.encoder, stream),
        }

    def deserialize_data_stream(self, content: SerializedDataStream) -> AsyncGenerator[JsonType]:
        """Deserialize the given stream of JSON data."""
        return _load_json_stream(self.decoder, content["data_stream"])


json_serializer = JsonSerializer()
"""JsonSerializer with default settings."""
json_stream_serializer = JsonStreamSerializer()
"""JsonStreamSerializer with default settings."""


async def _dump_json_stream(
    encoder: json.JSONEncoder,
    stream: AsyncIterable[JsonType],
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
) -> AsyncGenerator[JsonType]:
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
