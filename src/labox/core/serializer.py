from __future__ import annotations

from dataclasses import dataclass
from dataclasses import replace
from typing import TYPE_CHECKING
from typing import Any
from typing import Generic
from typing import Protocol
from typing import TypedDict
from typing import TypeVar

from typing_extensions import AsyncGenerator

from labox._internal._component import Component

if TYPE_CHECKING:
    from collections.abc import AsyncIterable


T = TypeVar("T", default=Any)
O = TypeVar("O", default=Any)  # noqa: E741
T_co = TypeVar("T_co", covariant=True, default=Any)
T_con = TypeVar("T_con", contravariant=True, default=Any)
O_con = TypeVar("O_con", contravariant=True, default=Any)


@dataclass(frozen=True)
class Serializer(Generic[T, O], Component):
    """An object that can serialize and deserialize data."""

    serialize_func: SerializeFunc[T, O]
    """A function that serializes the given value."""
    deserialize_func: DeserializeFunc[T, O]
    """A function that deserializes the given value."""
    options: O
    """Options for serialization and deserialization."""
    types: tuple[type[T], ...] = ()
    """The types that the serializer can handle."""
    content_types: tuple[str, ...] = ()
    """The content types that the serializer uses."""

    def serialize(self, value: T) -> SerializedData:
        """Serialize the given value."""
        return self.serialize_func(value, self.options)

    def deserialize(self, data: SerializedData) -> T:
        """Deserialize the given data."""
        return self.deserialize_func(data, self.options)

    def configure(self, options: O) -> Serializer[T, O]:
        """Create a new serializer with the given options."""
        return replace(self, options=options)


@dataclass(frozen=True)
class StreamSerializer(Generic[T, O], Component):
    """An object that can serialize and deserialize streams of data."""

    serialize_func: SerializeStreamFunc[T, O]
    """A function that serializes the given stream of values."""
    deserialize_func: DeserializeStreamFunc[T, O]
    """A function that deserializes the given stream of values."""
    options: O
    """Options for serialization and deserialization."""
    types: tuple[type[T], ...] = ()
    """The types that the serializer can handle."""
    content_types: tuple[str, ...] = ()
    """The content types that the serializer uses."""

    def serialize(self, value: AsyncIterable[T]) -> SerializedDataStream:
        """Serialize the given stream of values."""
        return self.serialize_func(value, self.options)

    def deserialize(self, data: SerializedDataStream) -> AsyncGenerator[T]:
        """Deserialize the given stream of values."""
        return self.deserialize_func(data, self.options)

    def configure(self, options: O) -> StreamSerializer[T, O]:
        """Create a new stream serializer with the given options."""
        return replace(self, options=options)


class SerializeFunc(Protocol[T_con, O_con]):
    """A function that serializes the given value."""

    def __call__(self, value: T_con, options: O_con, /) -> SerializedData:
        """Serialize the given value."""
        ...


class DeserializeFunc(Protocol[T_co, O_con]):
    """A function that deserializes the given value."""

    def __call__(self, data: SerializedData, options: O_con, /) -> T_co:
        """Deserialize the given value."""
        ...


class SerializeStreamFunc(Protocol[T_con, O_con]):
    """A function that serializes the given stream of values."""

    def __call__(self, value: AsyncIterable[T_con], options: O_con, /) -> SerializedDataStream:
        """Serialize the given stream of values."""
        ...


class DeserializeStreamFunc(Protocol[T_co, O_con]):
    """A function that deserializes the given stream of values."""

    def __call__(self, data: SerializedDataStream, options: O_con, /) -> AsyncGenerator[T_co]:
        """Deserialize the given stream of values."""
        ...


class SerializedData(TypedDict):
    """The serialized representation of a value."""

    data: bytes
    """The serialized data."""
    content_encoding: str | None
    """The encoding of the data."""
    content_type: str
    """The MIME type of the data."""


class SerializedDataStream(TypedDict):
    """The serialized representation of a stream of values."""

    data_stream: AsyncGenerator[bytes]
    """The serialized data stream."""
    content_encoding: str | None
    """The encoding of the data."""
    content_type: str
    """The MIME type of the data."""
