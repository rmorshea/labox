from __future__ import annotations

from typing import TYPE_CHECKING
from typing import LiteralString
from typing import Protocol
from typing import TypedDict
from typing import TypeVar

from artery.core.registry import Registry

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


class ScalarSerializer(_BaseSerializer[T]):
    """A protocol for serializing and deserializing objects from scalar values."""

    def dump_scalar(self, value: T) -> ScalarDump:
        """Serialize the given value."""
        ...

    def load_scalar(self, dump: ScalarDump) -> T:
        """Deserialize the given value."""
        ...


class StreamSerializer(_BaseSerializer[T]):
    """A protocol for serializing and deserializing objects from streams of values."""

    async def dump_stream(self, stream: AsyncIterable[T]) -> StreamDump:
        """Serialize the given stream."""
        ...

    async def load_stream(self, dump: StreamDump) -> AsyncIterator[T]:
        """Deserialize the given stream."""
        ...


class ScalarDump(TypedDict):
    """The serialized representation of a single value."""

    scalar: bytes
    """The serialized data."""
    content_type: str
    """The MIME type of the data."""
    serializer_name: str
    """The name of the serializer used to serialize the data."""
    serializer_version: int
    """The version of the serializer used to serialize the data."""


class StreamDump(TypedDict):
    """The serialized representation of a stream of values."""

    stream: AsyncIterable[bytes]
    """The serialized data stream."""
    content_type: str
    """The MIME type of the data stream as a whole."""
    serializer_name: str
    """The name of the serializer used to serialize the data."""
    serializer_version: int
    """The version of the serializer used to serialize the data."""


class StreamSerializerRegistry(Registry[StreamSerializer]):
    """A registry of stream serializers."""

    item_description = "Stream serializer"

    if TYPE_CHECKING:

        def get_by_type_inference(self, cls: type[T]) -> StreamSerializer[T]: ...  # noqa: D102


class ScalarSerializerRegistry(Registry[ScalarSerializer]):
    """A registry of scalar serializers."""

    item_description = "Scalar serializer"

    if TYPE_CHECKING:

        def get_by_type_inference(self, cls: type[T]) -> ScalarSerializer[T]: ...  # noqa: D102
