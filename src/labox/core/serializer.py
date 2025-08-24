from __future__ import annotations

import abc
from typing import TYPE_CHECKING
from typing import Any
from typing import Generic
from typing import NotRequired
from typing import TypedDict

from typing_extensions import AsyncGenerator
from typing_extensions import TypeVar

from labox._internal._component import Component
from labox._internal._utils import not_implemented
from labox._internal._utils import validate_versioned_class_name
from labox.common.json import DEFAULT_JSON_DECODER
from labox.common.json import DEFAULT_JSON_ENCODER

if TYPE_CHECKING:
    from collections.abc import AsyncIterable


T = TypeVar("T", default=Any)
C = TypeVar("C", default=Any)


class _BaseSerializer(Generic[T, C]):
    """A base class for serializers that provides common functionality."""

    types: tuple[type[T], ...] = ()
    """The types that the serializer can handle

    Used for type inference in the [registry][labox.core.registry.Registry].
    """
    content_types: tuple[str, ...] = ()
    """The content types that the serializer uses.

    Used to get serializers by content type in the [registry][labox.core.registry.Registry].
    """

    def serialize_config(self, config: C) -> str:
        """Serialize the configuration to a JSON string."""
        return DEFAULT_JSON_ENCODER.encode(config)

    def deserialize_config(self, config: str) -> C:
        """Deserialize the configuration from a JSON string."""
        return DEFAULT_JSON_DECODER.decode(config)


class Serializer(_BaseSerializer[T, C], Component):
    """A protocol for serializing/deserializing values."""

    def __init_subclass__(cls) -> None:
        validate_versioned_class_name(cls)

    @abc.abstractmethod
    @not_implemented
    def serialize_data(self, value: T, /) -> SerializedData[C]:
        """Serialize the given value."""
        ...

    @abc.abstractmethod
    @not_implemented
    def deserialize_data(self, content: SerializedData[C], /) -> T:
        """Deserialize the given value."""
        ...


class StreamSerializer(_BaseSerializer[T, C], Component):
    """A protocol for serializing/deserializing streams of values."""

    @abc.abstractmethod
    @not_implemented
    def serialize_data_stream(self, stream: AsyncIterable[T], /) -> SerializedDataStream[C]:
        """Serialize the given stream."""
        ...

    @abc.abstractmethod
    @not_implemented
    def deserialize_data_stream(self, content: SerializedDataStream[C], /) -> AsyncGenerator[T]:
        """Deserialize the given stream."""
        ...


class SerializedData(Generic[C], TypedDict):
    """The serialized representation of a value."""

    data: bytes
    """The serialized data."""
    content_encoding: str | None
    """The encoding of the data."""
    content_type: str
    """The MIME type of the data."""
    config: NotRequired[C]
    """Additional configuration for the serializer, if any."""


class SerializedDataStream(Generic[C], TypedDict):
    """The serialized representation of a stream of values."""

    data_stream: AsyncGenerator[bytes]
    """The serialized data stream."""
    content_encoding: str | None
    """The encoding of the data."""
    content_type: str
    """The MIME type of the data."""
    config: NotRequired[C]
    """Additional configuration for the serializer, if any."""
