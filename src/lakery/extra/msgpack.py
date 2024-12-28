import abc
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterable
from collections.abc import Iterable
from typing import Any
from typing import Generic
from typing import TypeVar
from typing import cast

from msgpack import ExtType
from msgpack import Packer
from msgpack import Unpacker
from msgpack import packb
from msgpack import unpackb
from msgpack.fallback import BytesIO

from lakery.core.serializer import StreamDump
from lakery.core.serializer import StreamSerializer
from lakery.core.serializer import ValueDump
from lakery.core.serializer import ValueSerializer

MsgPackType = (
    int
    | str
    | float
    | bool
    | dict[int | str | float | bool | None, "MsgPackType"]
    | list["MsgPackType"]
    | None
)

H = TypeVar("H")
T = TypeVar("T", default=None)


MSG_PACK_TYPES = (int, str, float, bool, type(None), dict, list)
"""The types that can be serialized to the MessagePack format."""


class ExtensionHook(Generic[H], abc.ABC):
    """A hook for serializing and deserializing custom types with MessagePack."""

    @abc.abstractmethod
    def dump(self, value: Any) -> ExtType:
        """Serialize the given value to a MessagePack extension type."""
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def load(cls, code: int, data: MsgPackType) -> H | ExtType:
        """Deserialize the given MessagePack extension type."""
        raise NotImplementedError


class MsgPackSerializer(ValueSerializer[T | MsgPackType]):
    """A serializer for MessagePack data."""

    name = "lakery.msgpack.value"
    version = 1
    types = MSG_PACK_TYPES
    content_type = "application/msgpack"

    def __init__(self, *, extension_hook: ExtensionHook[T] | None = None) -> None:
        if extension_hook is None:
            self._ext_dump_hook = None
            self._ext_load_hook = None
        else:
            self._ext_dump_hook = extension_hook.dump
            self._ext_load_hook = extension_hook.load

    def dump_value(self, value: T | MsgPackType) -> ValueDump:
        """Serialize the given value to MessagePack."""
        return {
            "content_encoding": "binary",
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
            "value": cast("bytes", packb(value, default=self._ext_dump_hook)),
        }

    def load_value(self, dump: ValueDump) -> T | MsgPackType:
        """Deserialize the given MessagePack data."""
        return unpackb(dump["value"], ext_hook=self._ext_load_hook)


class MsgPackStreamSerializer(StreamSerializer[T | MsgPackType]):
    """A serializer for MessagePack data."""

    name = "lakery.msgpack.stream"
    version = 1
    types = (list, dict)
    content_type = "application/msgpack"

    def __init__(self, *, extension_hook: ExtensionHook[T] | None = None) -> None:
        if extension_hook is None:
            self._ext_dump_hook = None
            self._ext_load_hook = None
        else:
            self._ext_dump_hook = extension_hook.dump
            self._ext_load_hook = extension_hook.load

    def dump_value(self, value: Iterable[T | MsgPackType]) -> ValueDump:
        """Serialize the given value to MessagePack."""
        packer = Packer(default=self._ext_dump_hook)
        buffer = BytesIO()
        for v in value:
            buffer.write(packer.pack(v))
        return {
            "content_encoding": "binary",
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
            "value": buffer.getvalue(),
        }

    def load_value(self, dump: ValueDump) -> list[T | MsgPackType]:
        """Deserialize the given MessagePack data."""
        unpacker = Unpacker(BytesIO(dump["value"]), ext_hook=self._ext_load_hook)
        return list(unpacker)

    def dump_stream(self, stream: AsyncIterable[T | MsgPackType], /) -> StreamDump:
        """Serialize the given stream of MessagePack data."""
        return {
            "content_encoding": "binary",
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
            "stream": _stream_dump(Packer(default=self._ext_dump_hook), stream),
        }

    def load_stream(self, dump: StreamDump, /) -> AsyncGenerator[T | MsgPackType]:
        """Deserialize the given stream of MessagePack data."""
        return _stream_load(Unpacker(ext_hook=self._ext_load_hook), dump["stream"])


async def _stream_dump(packer: Packer, value_stream: AsyncIterable[Any]) -> AsyncGenerator[bytes]:
    async for value in value_stream:
        yield packer.pack(value)


async def _stream_load(unpacker: Unpacker, stream: AsyncIterable[bytes], /) -> AsyncGenerator[Any]:
    async for chunk in stream:
        unpacker.feed(chunk)
        for value in unpacker:
            yield value
