from __future__ import annotations

import abc
from typing import TYPE_CHECKING
from typing import Generic
from typing import LiteralString
from typing import TypedDict
from typing import TypeVar

from ardex.core._registry import Registry

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator
    from collections.abc import Sequence


T = TypeVar("T")


class _BaseSerializer(abc.ABC, Generic[T]):
    name: LiteralString
    """The name of the serializer."""
    types: tuple[type[T], ...]
    """The types that the serializer can handle."""
    version: int


class ScalarSerializer(_BaseSerializer[T]):
    """A protocol for serializing/deserializing objects from scalar values."""

    @abc.abstractmethod
    def dump_scalar(self, scalar: T, /) -> ScalarDump:
        """Serialize the given value."""
        ...

    @abc.abstractmethod
    def load_scalar(self, dump: ScalarDump, /) -> T:
        """Deserialize the given value."""
        ...


class StreamSerializer(_BaseSerializer[T]):
    """A protocol for serializing/deserializing objects from streams of values."""

    @abc.abstractmethod
    def dump_scalar(self, scalar: Sequence[T], /) -> ScalarDump:
        """Serialize the given value."""
        ...

    @abc.abstractmethod
    def load_scalar(self, dump: ScalarDump, /) -> Sequence[T]:
        """Deserialize the given value."""
        ...

    @abc.abstractmethod
    def dump_stream(self, stream: AsyncIterable[T], /) -> StreamDump:
        """Serialize the given stream."""
        ...

    @abc.abstractmethod
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


class ScalarDump(_BaseDump):
    """The serialized representation of a scalar value."""

    content_scalar: bytes
    """The serialized data."""


class StreamDump(_BaseDump):
    """The serialized representation of a stream of values."""

    content_stream: AsyncIterable[bytes]
    """The serialized data stream."""


class SerializerRegistry(Registry[ScalarSerializer | StreamSerializer]):
    """A registry of stream serializers."""

    item_description = "Stream serializer"

    def __init__(self, items: Sequence[ScalarSerializer | StreamSerializer]) -> None:
        super().__init__(items)
        self.by_scalar_type = {
            type_: serializer
            for serializer in self.items
            if isinstance(serializer, ScalarSerializer | StreamSerializer)
            for type_ in serializer.types
        }
        self.by_stream_type = {
            type_: serializer
            for serializer in self.items
            if isinstance(serializer, StreamSerializer)
            for type_ in serializer.types
        }

    def infer_from_scalar_type(self, cls: type[T]) -> ScalarSerializer[T] | StreamSerializer[T]:
        """Get the first serializer that can handle the given type or its parent classes."""
        for base in cls.mro():
            if item := self.by_scalar_type.get(base):
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
