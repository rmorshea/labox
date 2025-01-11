from __future__ import annotations

import json
from codecs import getincrementaldecoder
from io import StringIO
from typing import TYPE_CHECKING

from lakery.common.streaming import decode_async_byte_stream
from lakery.core.serializer import StreamDump
from lakery.core.serializer import StreamSerializer
from lakery.core.serializer import ValueDump
from lakery.core.serializer import ValueSerializer

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable
    from collections.abc import Iterable

JsonType = int | str | float | bool | dict[str, "JsonType"] | list["JsonType"] | None
"""A type alias for JSON data."""
JsonStreamType = dict[str, JsonType] | list[JsonType]
"""A type alias for a a value in a stream of JSON data."""

JSON_SCALAR_TYPES = (int, str, float, bool, type(None), dict, list)
"""The types that can be serialized to JSON."""
JSON_STREAM_TYPES = (list, dict)
"""The types that can be serialized JSON in a stream."""


class JsonSerializer(ValueSerializer[JsonType]):
    """A serializer for JSON data."""

    name = "lakery.json.value"
    version = 1
    types = JSON_SCALAR_TYPES
    content_type = "application/json"

    def dump_value(self, value: JsonType) -> ValueDump:
        """Serialize the given value to JSON."""
        return {
            "content_encoding": "utf-8",
            "content_type": self.content_type,
            "content_bytes": json.dumps(value, separators=(",", ":")).encode("utf-8"),
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_value(self, dump: ValueDump) -> JsonType:
        """Deserialize the given JSON data."""
        return json.loads(dump["content_bytes"].decode("utf-8"))


class JsonStreamSerializer(StreamSerializer[JsonStreamType]):
    """A serializer for JSON data."""

    name = "lakery.json.stream"
    version = 1
    types = JSON_STREAM_TYPES
    content_type = "application/json"

    def dump_value(self, value: Iterable[JsonStreamType]) -> ValueDump:
        """Serialize the given value to JSON."""
        return {
            "content_encoding": "utf-8",
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
            "content_bytes": json.dumps(list(value), separators=(",", ":")).encode("utf-8"),
        }

    def load_value(self, dump: ValueDump) -> list[JsonStreamType]:
        """Deserialize the given JSON data."""
        return json.loads(dump["content_bytes"].decode("utf-8"))

    def dump_stream(self, stream: AsyncIterable[JsonStreamType]) -> StreamDump:
        """Serialize the given stream of JSON data."""
        return {
            "content_encoding": "utf-8",
            "content_byte_stream": _dump_json_stream(stream),
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_stream(self, dump: StreamDump) -> AsyncGenerator[JsonStreamType]:
        """Deserialize the given stream of JSON data."""
        return _load_json_stream(dump["content_byte_stream"])


async def _dump_json_stream(stream: AsyncIterable[JsonStreamType]) -> AsyncGenerator[bytes]:
    yield b"["
    buffer = StringIO()
    async for chunk in stream:
        if not isinstance(chunk, list | dict):
            msg = f"Expected list or dict of JSON data, got {chunk!r}"
            raise TypeError(msg)
        json.dump(chunk, buffer, separators=(",", ":"))
        yield buffer.getvalue().encode("utf-8")
        buffer.seek(0)
        buffer.write(",")
        buffer.truncate()
    yield b"]"


async def _load_json_stream(stream: AsyncIterable[bytes]) -> AsyncGenerator[JsonStreamType]:
    buffer = StringIO()
    started = False
    decoder = json.JSONDecoder()
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
