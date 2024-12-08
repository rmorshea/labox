from __future__ import annotations

from typing import TYPE_CHECKING
from typing import LiteralString
from typing import Protocol
from typing import TypedDict
from typing import TypeVar

from ardex.core._registry import Registry

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator


T = TypeVar("T")


class _BaseSerializer(Protocol[T]):
    name: LiteralString
    """The name of the serializer."""
    types: tuple[type[T], ...]
    """The types that the serializer can handle."""
    version: int


class SingleSerializer(_BaseSerializer[T]):
    """A protocol for serializing and deserializing objects from single values."""

    def dump_single(self, value: T, /) -> SingleDump:
        """Serialize the given value."""
        ...

    def load_single(self, dump: SingleDump, /) -> T:
        """Deserialize the given value."""
        ...


class StreamSerializer(_BaseSerializer[T]):
    """A protocol for serializing and deserializing objects from streams of values."""

    def dump_stream(self, stream: AsyncIterable[T], /) -> StreamDump:
        """Serialize the given stream."""
        ...

    def load_stream(self, dump: StreamDump, /) -> AsyncIterator[T]:
        """Deserialize the given stream."""
        ...


class _BaseDump(TypedDict):
    content_type: str
    """The MIME type of the data."""
    serializer_name: str
    """The name of the serializer used to serialize the data."""
    serializer_version: int
    """The version of the serializer used to serialize the data."""


class SingleDump(_BaseDump):
    """The serialized representation of a single value."""

    content_single: bytes
    """The serialized data."""


class StreamDump(_BaseDump):
    """The serialized representation of a stream of values."""

    content_stream: AsyncIterable[bytes]
    """The serialized data stream."""


class StreamSerializerRegistry(Registry[StreamSerializer]):
    """A registry of stream serializers."""

    item_description = "Stream serializer"

    if TYPE_CHECKING:

        def get_by_type_inference(self, cls: type[T]) -> StreamSerializer[T]: ...  # noqa: D102


class SingleSerializerRegistry(Registry[SingleSerializer]):
    """A registry of single serializers."""

    item_description = "Single serializer"

    if TYPE_CHECKING:

        def get_by_type_inference(self, cls: type[T]) -> SingleSerializer[T]: ...  # noqa: D102
