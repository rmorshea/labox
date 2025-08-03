from __future__ import annotations

import abc
from collections.abc import Mapping
from typing import TYPE_CHECKING
from typing import Any
from typing import Generic
from typing import Required

from sqlalchemy.util.typing import TypedDict
from typing_extensions import TypeVar

from labox._internal._component import Component
from labox._internal._utils import not_implemented
from labox.core.storable import Storable

if TYPE_CHECKING:
    from collections.abc import AsyncIterable

    from labox.core.registry import Registry
    from labox.core.serializer import Serializer
    from labox.core.serializer import StreamSerializer
    from labox.core.storage import Storage


S = TypeVar("S", bound=Storable, default=Any)
T = TypeVar("T", bound=Any, default=Any)
D = TypeVar("D", bound=Mapping[str, Any], default=Mapping[str, Any])


class Unpacker(Generic[S, D], Component):
    """A base for classes that decompose storable objects into their constituent parts."""

    @abc.abstractmethod
    @not_implemented
    def unpack_object(self, obj: S, registry: Registry, /) -> D:
        """Return a mapping of that describes where and how to store the object's contents."""
        ...

    @abc.abstractmethod
    @not_implemented
    def repack_object(self, cls: type[S], contents: D, registry: Registry, /) -> S:
        """Reconstitute the object from a mapping of its unpacked contents."""
        ...


class UnpackedValue(Generic[T], TypedDict, total=False):
    """Describes where and how to store a value."""

    value: Required[T]
    """The value to store."""
    serializer: Serializer[T] | None
    """The serializer to apply to the value."""
    storage: Storage | None
    """The storage to send the serialized value to."""


class UnpackedValueStream(Generic[T], TypedDict, total=False):
    """Describes where and how to store a stream."""

    value_stream: Required[AsyncIterable[T]]
    """The stream of data to store."""
    serializer: StreamSerializer[T] | None
    """The serializer to apply to the stream."""
    storage: Storage | None
    """The storage to send the serialized stream to."""


AnyUnpackedValue = UnpackedValue[Any] | UnpackedValueStream[Any]
"""A type that can be either a value or a stream of values."""
