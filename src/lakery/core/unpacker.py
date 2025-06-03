from __future__ import annotations

import abc
from typing import TYPE_CHECKING
from typing import Any
from typing import ClassVar
from typing import Generic
from typing import LiteralString
from typing import Required

from sqlalchemy.util.typing import TypedDict
from typing_extensions import TypeVar

from lakery._internal.utils import validate_versioned_class_name
from lakery.core.registry import Registry

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import Mapping

    from lakery.core.registry import Registry
    from lakery.core.serializer import Serializer
    from lakery.core.serializer import StreamSerializer
    from lakery.core.storage import Storage


T = TypeVar("T", bound=Any, default=Any)


class Unpacker(abc.ABC, Generic[T]):
    """A base for classes that decompose models into their constituent parts."""

    name: ClassVar[LiteralString]
    """The name of the packer."""
    types: tuple[type[T], ...]
    """The types of objects that this packer can handle."""

    def __init_subclass__(cls) -> None:
        validate_versioned_class_name(cls)

    @abc.abstractmethod
    def unpack_object(self, obj: T, registry: Registry, /) -> Mapping[str, Any]:
        """Return a mapping of that describes where and how to store the object's contents."""
        raise NotImplementedError

    @abc.abstractmethod
    def repack_object(self, unpacked: Mapping[str, Any], registry: Registry, /) -> T:
        """Reconstitute the object from a mapping of its unpacked contents."""
        raise NotImplementedError


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
