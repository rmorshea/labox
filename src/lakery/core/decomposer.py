from __future__ import annotations

import abc
from collections.abc import AsyncIterable
from collections.abc import Mapping
from typing import TYPE_CHECKING
from typing import Any
from typing import Generic
from typing import LiteralString

from sqlalchemy.util.typing import TypedDict
from typing_extensions import TypeVar

if TYPE_CHECKING:
    from lakery.core.registry import Registry
    from lakery.core.serializer import Serializer
    from lakery.core.serializer import StreamSerializer
    from lakery.core.storage import Storage


T = TypeVar("T", default=Any)
D = TypeVar("D", bound=Mapping[str, Any], default=Mapping[str, Any])


class Decomposer(abc.ABC, Generic[T, D]):
    """A base for classes that decompose models into their constituent parts."""

    name: LiteralString
    """The name of the packer."""

    @abc.abstractmethod
    def recompose_model(self, obj: T, registry: Registry, /) -> D:
        """Return a mapping of that describes where and how to store the object's contents."""
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def decompose_model(cls, unpacked: D, registry: Registry, /) -> T:
        """Reconstitute the object from a mapping of its unpacked contents."""
        raise NotImplementedError


class DecomposedValue(Generic[T], TypedDict):
    """Describes where and how to store a value."""

    value: T
    """The value to store."""
    serializer: Serializer[T] | None
    """The serializer to apply to the value."""
    storage: Storage | None
    """The storage to send the serialized value to."""


class UnpackedValueStream(Generic[T], TypedDict):
    """Describes where and how to store a stream."""

    value_stream: AsyncIterable[T]
    """The stream of data to store."""
    serializer: StreamSerializer[T] | None
    """The serializer to apply to the stream."""
    storage: Storage | None
    """The storage to send the serialized stream to."""
