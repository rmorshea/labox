from collections.abc import AsyncGenerator
from collections.abc import AsyncIterable
from collections.abc import Callable
from collections.abc import Iterable
from typing import Any

from msgpack import Packer
from msgpack import Unpacker
from msgpack.fallback import BytesIO

from lakery.core.serializer import StreamDump
from lakery.core.serializer import StreamSerializer
from lakery.core.serializer import ValueDump
from lakery.core.serializer import ValueSerializer

MsgPackType = (
    Any  # Include any to account for msgpack extension types
    | int
    | str
    | float
    | bool
    | dict[int | str | float | bool | None, "MsgPackType"]
    | list["MsgPackType"]
    | None
)
"""A type alias for MessagePack data."""

MSG_PACK_TYPES = (int, str, float, bool, type(None), dict, list)
"""The types that can be serialized to the MessagePack format."""


class _MsgPackBase:
    types = MSG_PACK_TYPES
    content_type = "application/msgpack"

    def __init__(
        self,
        *,
        packer: Callable[[], Packer] = Packer,
        unpacker: Callable[[], Unpacker] = Unpacker,
    ) -> None:
        self._packer = packer
        self._unpacker = unpacker

    def _pack(self, value: Any) -> bytes:
        return self._packer().pack(value)

    def _unpack(self, data: bytes) -> Any:
        unpacker = self._unpacker()
        unpacker.feed(data)
        return unpacker.unpack()


class MsgPackSerializer(_MsgPackBase, ValueSerializer[MsgPackType]):
    """A serializer for MessagePack data."""

    name = "lakery.msgpack.value"
    version = 1

    def dump_value(self, value: MsgPackType) -> ValueDump:
        """Serialize the given value to MessagePack."""
        return {
            "content_encoding": None,
            "content_type": self.content_type,
            "content_value": self._pack(value),
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_value(self, dump: ValueDump) -> MsgPackType:
        """Deserialize the given MessagePack data."""
        return self._unpack(dump["content_value"])


class MsgPackStreamSerializer(_MsgPackBase, StreamSerializer[MsgPackType]):
    """A serializer for MessagePack data."""

    name = "lakery.msgpack.stream"
    version = 1

    def dump_value(self, value: Iterable[MsgPackType]) -> ValueDump:
        """Serialize the given value to MessagePack."""
        packer = self._packer()
        buffer = BytesIO()
        for v in value:
            buffer.write(packer.pack(v))
        return {
            "content_encoding": None,
            "content_type": self.content_type,
            "content_value": buffer.getvalue(),
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_value(self, dump: ValueDump) -> list[MsgPackType]:
        """Deserialize the given MessagePack data."""
        unpacker = self._unpacker()
        unpacker.feed(dump["content_value"])
        return list(unpacker)

    def dump_stream(self, stream: AsyncIterable[MsgPackType]) -> StreamDump:
        """Serialize the given stream of MessagePack data."""
        return {
            "content_encoding": None,
            "content_stream": _stream_dump(self._packer(), stream),
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_stream(self, dump: StreamDump, /) -> AsyncGenerator[MsgPackType]:
        """Deserialize the given stream of MessagePack data."""
        return _stream_load(self._unpacker(), dump["content_stream"])


async def _stream_dump(packer: Packer, value_stream: AsyncIterable[Any]) -> AsyncGenerator[bytes]:
    async for value in value_stream:
        yield packer.pack(value)


async def _stream_load(unpacker: Unpacker, stream: AsyncIterable[bytes]) -> AsyncGenerator[Any]:
    async for chunk in stream:
        unpacker.feed(chunk)
        for value in unpacker:
            yield value
