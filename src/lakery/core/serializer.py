from __future__ import annotations

import abc
from typing import TYPE_CHECKING
from typing import Generic
from typing import LiteralString
from typing import TypedDict
from typing import TypeVar

from lakery.core._registry import Registry

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator
    from collections.abc import Iterable
    from collections.abc import Sequence


T = TypeVar("T")


class _BaseSerializer(abc.ABC, Generic[T]):
    name: LiteralString
    """The name of the serializer."""
    types: tuple[type[T], ...]
    """The types that the serializer can handle."""
    version: int


class ValueSerializer(_BaseSerializer[T]):
    """A protocol for serializing/deserializing objects from value values."""

    @abc.abstractmethod
    def dump_value(self, value: T, /) -> ValueDump:
        """Serialize the given value."""
        raise NotImplementedError

    @abc.abstractmethod
    def load_value(self, dump: ValueDump, /) -> T:
        """Deserialize the given value."""
        raise NotImplementedError


class StreamSerializer(_BaseSerializer[T]):
    """A protocol for serializing/deserializing objects from streams of values."""

    @abc.abstractmethod
    def dump_value(self, value: Iterable[T], /) -> ValueDump:
        """Serialize the given value."""
        raise NotImplementedError

    @abc.abstractmethod
    def load_value(self, dump: ValueDump, /) -> Iterable[T]:
        """Deserialize the given value."""
        raise NotImplementedError

    @abc.abstractmethod
    def dump_stream(self, stream: AsyncIterable[T], /) -> StreamDump:
        """Serialize the given stream."""
        raise NotImplementedError

    @abc.abstractmethod
    def load_stream(self, dump: StreamDump, /) -> AsyncIterator[T]:
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

    value: bytes
    """The serialized data."""


class StreamDump(_BaseDump):
    """The serialized representation of a stream of values."""

    stream: AsyncIterable[bytes]
    """The serialized data stream."""


class SerializerRegistry(Registry[ValueSerializer | StreamSerializer]):
    """A registry of stream serializers."""

    item_description = "Stream serializer"

    def __init__(self, items: Sequence[ValueSerializer | StreamSerializer]) -> None:
        super().__init__(items)
        self.by_value_type = {
            type_: serializer
            for serializer in self.items
            if isinstance(serializer, ValueSerializer | StreamSerializer)
            for type_ in serializer.types
        }
        self.by_stream_type = {
            type_: serializer
            for serializer in self.items
            if isinstance(serializer, StreamSerializer)
            for type_ in serializer.types
        }

    def infer_from_value_type(self, cls: type[T]) -> ValueSerializer[T] | StreamSerializer[T]:
        """Get the first serializer that can handle the given type or its parent classes."""
        for base in cls.mro():
            if item := self.by_value_type.get(base):
                return item
        msg = f"No serializer found for {cls}."
        raise ValueError(msg)

    def infer_from_stream_type(self, cls: type[T]) -> StreamSerializer[T]:
        """Get the first serializer that can handle the given type or its parent classes."""
        for base in cls.mro():
            if item := self.by_stream_type.get(base):
                return item
        msg = f"No serializer found for {cls}."
        raise ValueError(msg)
