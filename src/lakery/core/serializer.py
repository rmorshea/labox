from __future__ import annotations

import abc
from typing import TYPE_CHECKING
from typing import ClassVar
from typing import Generic
from typing import LiteralString
from typing import TypedDict
from typing import TypeVar

from typing_extensions import AsyncGenerator

from lakery._internal.utils import validate_versioned_class_name

if TYPE_CHECKING:
    from collections.abc import AsyncIterable


T = TypeVar("T")


class Serializer(Generic[T]):
    """A protocol for serializing/deserializing values."""

    name: ClassVar[LiteralString]
    """The name of the serializer."""
    types: tuple[type[T], ...] = ()
    """The types that the serializer can handle."""

    def __init_subclass__(cls) -> None:
        validate_versioned_class_name(cls)

    @abc.abstractmethod
    def serialize_data(self, value: T, /) -> SerializedData:
        """Serialize the given value."""
        raise NotImplementedError

    @abc.abstractmethod
    def deserialize_data(self, content: SerializedData, /) -> T:
        """Deserialize the given value."""
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name!r})"


class StreamSerializer(Generic[T]):
    """A protocol for serializing/deserializing streams of values."""

    name: LiteralString
    """The name of the serializer."""
    types: tuple[type[T], ...] = ()
    """The types that the serializer can handle."""

    @abc.abstractmethod
    def serialize_data_stream(self, stream: AsyncIterable[T], /) -> SerializedDataStream:
        """Serialize the given stream."""
        raise NotImplementedError

    @abc.abstractmethod
    def deserialize_data_stream(self, content: SerializedDataStream, /) -> AsyncGenerator[T]:
        """Deserialize the given stream."""
        raise NotImplementedError


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
