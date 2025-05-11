from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any

from msgpack import Packer
from msgpack import Unpacker
from msgpack.fallback import BytesIO

from lakery.core.serializer import Archive
from lakery.core.serializer import Serializer
from lakery.core.serializer import StreamArchive
from lakery.core.serializer import StreamSerializer

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable
    from collections.abc import Callable
    from collections.abc import Iterable

__all__ = (
    "MSG_PACK_TYPES",
    "MsgPackSerializer",
    "MsgPackStreamSerializer",
    "MsgPackType",
    "msgpack_serializer",
    "msgpack_stream_serializer",
)

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


class MsgPackSerializer(_MsgPackBase, Serializer[MsgPackType]):
    """A serializer for MessagePack data."""

    name = "lakery.msgpack.value"
    version = 1

    def dump(self, value: MsgPackType) -> Archive:
        """Serialize the given value to MessagePack."""
        return {
            "content_encoding": None,
            "content_type": self.content_type,
            "data": self._pack(value),
        }

    def load(self, content: Archive) -> MsgPackType:
        """Deserialize the given MessagePack data."""
        return self._unpack(content["data"])


class MsgPackStreamSerializer(_MsgPackBase, StreamSerializer[MsgPackType]):
    """A serializer for MessagePack data."""

    name = "lakery.msgpack.stream"
    version = 1

    def dump(self, value: Iterable[MsgPackType]) -> Archive:
        """Serialize the given value to MessagePack."""
        packer = self._packer()
        buffer = BytesIO()
        for v in value:
            buffer.write(packer.pack(v))
        return {
            "content_encoding": None,
            "content_type": self.content_type,
            "data": buffer.getvalue(),
        }

    def load(self, content: Archive) -> list[MsgPackType]:
        """Deserialize the given MessagePack data."""
        unpacker = self._unpacker()
        unpacker.feed(content["data"])
        return list(unpacker)

    def dump_stream(self, stream: AsyncIterable[MsgPackType]) -> StreamArchive:
        """Serialize the given stream of MessagePack data."""
        return {
            "content_encoding": None,
            "data_stream": _dump_stream(self._packer(), stream),
            "content_type": self.content_type,
        }

    def load_stream(self, content: StreamArchive, /) -> AsyncGenerator[MsgPackType]:
        """Deserialize the given stream of MessagePack data."""
        return _load_stream(self._unpacker(), content["data_stream"])


msgpack_serializer = MsgPackSerializer()
"""MsgPackSerializer with default settings."""

msgpack_stream_serializer = MsgPackStreamSerializer()
"""MsgPackStreamSerializer with default settings."""


async def _dump_stream(packer: Packer, value_stream: AsyncIterable[Any]) -> AsyncGenerator[bytes]:
    async for value in value_stream:
        yield packer.pack(value)


async def _load_stream(unpacker: Unpacker, stream: AsyncIterable[bytes]) -> AsyncGenerator[Any]:
    async for chunk in stream:
        unpacker.feed(chunk)
        for value in unpacker:
            yield value
