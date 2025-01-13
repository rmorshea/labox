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


class Serializer(Generic[T]):
    """A protocol for serializing/deserializing values."""

    name: LiteralString
    """The name of the serializer."""
    types: tuple[type[T], ...]
    """The types that the serializer can handle."""
    version: int

    @abc.abstractmethod
    def dump(self, value: T, /) -> ContentDump:
        """Serialize the given value."""
        raise NotImplementedError

    @abc.abstractmethod
    def load(self, dump: ContentDump, /) -> T:
        """Deserialize the given value."""
        raise NotImplementedError


class StreamSerializer(Serializer[Iterable[T]]):
    """A protocol for serializing/deserializing streams of values."""

    @abc.abstractmethod
    def dump_stream(self, stream: AsyncIterable[T], /) -> ContentStreamDump:
        """Serialize the given stream."""
        raise NotImplementedError

    @abc.abstractmethod
    def load_stream(self, dump: ContentStreamDump, /) -> AsyncGenerator[T]:
        """Deserialize the given stream."""
        raise NotImplementedError


class ContentDump(TypedDict):
    """The serialized representation of a value value."""

    content: bytes
    """The serialized data."""
    content_encoding: str | None
    """The encoding of the data."""
    content_type: str
    """The MIME type of the data."""


class ContentStreamDump(TypedDict):
    """The serialized representation of a stream of values."""

    content_stream: AsyncGenerator[bytes]
    """The serialized data stream."""
    content_encoding: str | None
    """The encoding of the data."""
    content_type: str
    """The MIME type of the data."""


class SerializerRegistry(Registry[str, Serializer | StreamSerializer]):
    """A registry of stream serializers."""

    value_description = "Serializer"

    def __init__(self, serializers: Sequence[Serializer | StreamSerializer] = ()) -> None:
        super().__init__(serializers)
        self._by_value_type = {
            type_: serializer
            for serializer in self.values()
            if isinstance(serializer, Serializer | StreamSerializer)
            for type_ in serializer.types
        }
        self._by_stream_type = {
            type_: serializer
            for serializer in self.values()
            if isinstance(serializer, StreamSerializer)
            for type_ in serializer.types
        }

    def get_key(self, serializer: Serializer | StreamSerializer) -> str:
        """Get the key for the given serializer."""
        return serializer.name

    def infer_from_value_type(self, cls: type[T]) -> Serializer[T]:
        """Get the first serializer that can handle the given type or its parent classes."""
        for base in cls.mro():
            if item := self._by_value_type.get(base):
                return item
        msg = f"No value {self.value_description.lower()} found for {cls}."
        raise ValueError(msg)

    def infer_from_stream_type(self, cls: type[T]) -> StreamSerializer[T]:
        """Get the first serializer that can handle the given type or its base classes."""
        for base in cls.mro():
            if item := self._by_stream_type.get(base):
                return item
        msg = f"No stream {self.value_description.lower()} found for {cls}."
        raise ValueError(msg)
