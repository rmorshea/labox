from __future__ import annotations

from collections.abc import Mapping
from dataclasses import KW_ONLY
from dataclasses import dataclass
from dataclasses import field
from typing import TYPE_CHECKING
from typing import Any
from typing import Generic

from typing_extensions import TypeVar

from labox.core.registry import Registry
from labox.core.storable import Storable
from labox.core.unpacker import UnpackedValue
from labox.core.unpacker import UnpackedValueStream
from labox.core.unpacker import Unpacker

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import Mapping

    from labox.core.registry import Registry
    from labox.core.serializer import Serializer
    from labox.core.serializer import StreamSerializer
    from labox.core.storage import Storage

T = TypeVar("T", default=Any)

__all__ = (
    "StorableStream",
    "StorableValue",
)


class StorableValueUnpacker(Unpacker["StorableValue"]):
    name = "labox.builtin.storable_value_unpacker@v1"

    def unpack_object(
        self, obj: StorableValue[Any], registry: Registry
    ) -> Mapping[str, UnpackedValue]:
        ser = registry.get_serializer(obj.serializer.name) if obj.serializer else None
        store = registry.get_storage(obj.storage.name) if obj.storage else None
        return {"item": {"value": obj.value, "serializer": ser, "storage": store}}

    def repack_object(
        self,
        cls: type[StorableValue[Any]],
        contents: Mapping[str, UnpackedValue],
        _registry: Registry,
    ) -> StorableValue[Any]:
        try:
            value = contents["item"]["value"]
        except KeyError as e:
            msg = "Unpacked contents must contain an 'item' key"
            raise ValueError(msg) from e
        return cls(value=value)


@dataclass(frozen=True)
class StorableValue(Storable, Generic[T], class_id="5a044e9f", unpacker=StorableValueUnpacker()):
    """A storable object that contains a single value."""

    value: T
    """The value to store."""

    _: KW_ONLY

    serializer: type[Serializer[T]] | None = field(default=None, compare=False)
    """The serializer to use for the value."""
    storage: type[Storage] | None = field(default=None, compare=False)
    """The storage to use for the value."""


class StorableStreamUnpacker(Unpacker["StorableStream"]):
    name = "labox.builtin.storable_stream_unpacker@v1"

    def unpack_object(
        self, obj: StorableStream[Any], registry: Registry
    ) -> Mapping[str, UnpackedValueStream]:
        ser = registry.get_stream_serializer(obj.serializer.name) if obj.serializer else None
        store = registry.get_storage(obj.storage.name) if obj.storage else None
        return {"item": {"value_stream": obj.value_stream, "serializer": ser, "storage": store}}

    def repack_object(
        self,
        cls: type[StorableStream[Any]],
        contents: Mapping[str, UnpackedValueStream],
        _registry: Registry,
    ) -> StorableStream[Any]:
        try:
            value_stream = contents["item"]["value_stream"]
        except KeyError as e:
            msg = "Unpacked contents must contain a 'value_stream' key"
            raise ValueError(msg) from e
        return cls(value_stream=value_stream)


@dataclass(frozen=True)
class StorableStream(Storable, Generic[T], class_id="31198cfb", unpacker=StorableStreamUnpacker()):
    """A storable object that contains a stream of values."""

    value_stream: AsyncIterable[T]
    """The stream of values to store."""

    _: KW_ONLY

    serializer: type[StreamSerializer[T]] | None = field(default=None, compare=False)
    """The serializer to use for the stream."""
    storage: type[Storage] | None = field(default=None, compare=False)
    """The storage to use for the stream."""
