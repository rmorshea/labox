from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any

from msgpack import Packer
from msgpack import Unpacker

from labox._internal._utils import frozenclass
from labox.core.serializer import SerializedData
from labox.core.serializer import SerializedDataStream
from labox.core.serializer import Serializer
from labox.core.serializer import StreamSerializer

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable
    from collections.abc import Callable

__all__ = (
    "MSG_PACK_TYPES",
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


@frozenclass
class MsgPackOptions:
    """Options for MsgPack serializers."""

    packer_type: Callable[[], Packer] = Packer
    unpacker_type: Callable[[], Unpacker] = Unpacker


def pack(value: MsgPackType, options: MsgPackOptions) -> SerializedData:
    """Pack the given value using the specified options."""
    return {
        "content_encoding": None,
        "content_type": "application/msgpack",
        "data": options.packer_type().pack(value),
    }


def unpack(data: SerializedData, options: MsgPackOptions) -> MsgPackType:
    """Unpack the given data using the specified options."""
    return options.unpacker_type().unpack(data["data"])


def pack_stream(
    stream: AsyncIterable[MsgPackType],
    options: MsgPackOptions,
) -> SerializedDataStream:
    """Pack the given stream of values using the specified options."""
    return {
        "content_encoding": None,
        "data_stream": _pack_stream(options.packer_type(), stream),
        "content_type": "application/msgpack",
    }


def unpack_stream(
    content: SerializedDataStream,
    options: MsgPackOptions,
) -> AsyncGenerator[MsgPackType]:
    """Unpack the given stream of data using the specified options."""
    return _unpack_stream(options.unpacker_type(), content["data_stream"])


async def _pack_stream(packer: Packer, value_stream: AsyncIterable[Any]) -> AsyncGenerator[bytes]:
    async for value in value_stream:
        yield packer.pack(value)


async def _unpack_stream(unpacker: Unpacker, stream: AsyncIterable[bytes]) -> AsyncGenerator[Any]:
    async for chunk in stream:
        unpacker.feed(chunk)
        for value in unpacker:
            yield value


msgpack_serializer = Serializer(
    name="labox.msgpack.value@v1",
    serialize_func=pack,
    deserialize_func=unpack,
    options=MsgPackOptions(),
    types=MSG_PACK_TYPES,
    content_types=("application/msgpack",),
)

msgpack_stream_serializer = StreamSerializer(
    name="labox.msgpack.stream@v1",
    serialize_func=pack_stream,
    deserialize_func=unpack_stream,
    options=MsgPackOptions(),
    types=MSG_PACK_TYPES,
    content_types=("application/msgpack",),
)
