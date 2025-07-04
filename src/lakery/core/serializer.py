from __future__ import annotations

import abc
from typing import TYPE_CHECKING
from typing import Any
from typing import Generic
from typing import TypedDict
from typing import TypeVar

from typing_extensions import AsyncGenerator

from lakery._internal._component import Component
from lakery._internal._utils import not_implemented
from lakery._internal._utils import validate_versioned_class_name

if TYPE_CHECKING:
    from collections.abc import AsyncIterable


T = TypeVar("T", default=Any)


class Serializer(Generic[T], Component):
    """A protocol for serializing/deserializing values."""

    types: tuple[type[T], ...] = ()
    """The types that the serializer can handle

    Used for type inference in the [registry][lakery.core.registry.Registry].
    """
    content_types: tuple[str, ...] = ()
    """The content types that the serializer uses.

    Used to get serializers by content type in the [registry][lakery.core.registry.Registry].
    """

    def __init_subclass__(cls) -> None:
        validate_versioned_class_name(cls)

    @abc.abstractmethod
    @not_implemented
    def serialize_data(self, value: T, /) -> SerializedData:
        """Serialize the given value."""
        ...

    @abc.abstractmethod
    @not_implemented
    def deserialize_data(self, content: SerializedData, /) -> T:
        """Deserialize the given value."""
        ...


class StreamSerializer(Generic[T], Component):
    """A protocol for serializing/deserializing streams of values."""

    types: tuple[type[T], ...] = ()
    """The types that the serializer can handle.

    Used for type inference in the [registry][lakery.core.registry.Registry].
    """
    content_types: tuple[str, ...] = ()
    """The content types that the serializer uses.

    Used to get serializers by content type in the [registry][lakery.core.registry.Registry].
    """

    @abc.abstractmethod
    @not_implemented
    def serialize_data_stream(self, stream: AsyncIterable[T], /) -> SerializedDataStream:
        """Serialize the given stream."""
        ...

    @abc.abstractmethod
    @not_implemented
    def deserialize_data_stream(self, content: SerializedDataStream, /) -> AsyncGenerator[T]:
        """Deserialize the given stream."""
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
