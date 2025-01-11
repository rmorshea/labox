from __future__ import annotations

import abc
from collections.abc import Iterable
from typing import TYPE_CHECKING
from typing import Generic
from typing import LiteralString
from typing import TypedDict
from typing import TypeVar

from typing_extensions import AsyncGenerator

from lakery.core._registry import Registry

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import Sequence


T = TypeVar("T")


class ValueSerializer(Generic[T]):
    """A protocol for serializing/deserializing values."""

    name: LiteralString
    """The name of the serializer."""
    types: tuple[type[T], ...]
    """The types that the serializer can handle."""
    version: int

    @abc.abstractmethod
    def dump_value(self, value: T, /) -> ValueDump:
        """Serialize the given value."""
        raise NotImplementedError

    @abc.abstractmethod
    def load_value(self, dump: ValueDump, /) -> T:
        """Deserialize the given value."""
        raise NotImplementedError


class StreamSerializer(ValueSerializer[Iterable[T]]):
    """A protocol for serializing/deserializing streams of values."""

    @abc.abstractmethod
    def dump_stream(self, stream: AsyncIterable[T], /) -> StreamDump:
        """Serialize the given stream."""
        raise NotImplementedError

    @abc.abstractmethod
    def load_stream(self, dump: StreamDump, /) -> AsyncGenerator[T]:
        """Deserialize the given stream."""
        raise NotImplementedError


class _BaseDump(TypedDict):
    content_encoding: str | None
    """The encoding of the data."""
    content_type: str
    """The MIME type of the data."""
    serializer_name: str
    """The name of the serializer used to serialize the data."""
    serializer_version: int
    """The version of the serializer used to serialize the data."""


class ValueDump(_BaseDump):
    """The serialized representation of a value value."""

    content_bytes: bytes
    """The serialized data."""


class StreamDump(_BaseDump):
    """The serialized representation of a stream of values."""

    content_byte_stream: AsyncGenerator[bytes]
    """The serialized data stream."""


class SerializerRegistry(Registry[str, ValueSerializer | StreamSerializer]):
    """A registry of stream serializers."""

    value_description = "Serializer"

    def __init__(self, serializers: Sequence[ValueSerializer | StreamSerializer]) -> None:
        super().__init__(serializers)
        self.by_value_type = {
            type_: serializer
            for serializer in self.values()
            if isinstance(serializer, ValueSerializer | StreamSerializer)
            for type_ in serializer.types
        }
        self.by_stream_type = {
            type_: serializer
            for serializer in self.values()
            if isinstance(serializer, StreamSerializer)
            for type_ in serializer.types
        }

    def get_key(self, serializer: ValueSerializer | StreamSerializer) -> str:
        """Get the key for the given serializer."""
        return serializer.name

    def infer_from_value_type(self, cls: type[T]) -> ValueSerializer[T] | StreamSerializer[T]:
        """Get the first serializer that can handle the given type or its parent classes."""
        for base in cls.mro():
            if item := self.by_value_type.get(base):
                return item
        msg = f"No value {self.value_description.lower()} found for {cls}."
        raise ValueError(msg)

    def infer_from_stream_type(self, cls: type[T]) -> StreamSerializer[T]:
        """Get the first serializer that can handle the given type or its base classes."""
        for base in cls.mro():
            if item := self.by_stream_type.get(base):
                return item
        msg = f"No stream {self.value_description.lower()} found for {cls}."
        raise ValueError(msg)
