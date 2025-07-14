from __future__ import annotations

from dataclasses import KW_ONLY
from dataclasses import dataclass
from dataclasses import field
from dataclasses import fields
from typing import TYPE_CHECKING
from typing import Any
from typing import Generic
from typing import TypeAlias
from typing import TypeVar

from lakery.core.storable import Storable
from lakery.core.unpacker import Unpacker

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import Mapping

    from lakery.core.registry import Registry
    from lakery.core.serializer import Serializer
    from lakery.core.serializer import StreamSerializer
    from lakery.core.storage import Storage

T = TypeVar("T", default=Any)

__all__ = (
    "StorableStream",
    "StorableValue",
)

_SimpleStorable: TypeAlias = "StorableValue | StorableStream"


class _SimpleUnpacker(Unpacker[_SimpleStorable]):
    name = "lakery.builtin.simple_unpacker@v1"

    def unpack_object(
        self,
        obj: _SimpleStorable,
        _registry: Registry,
    ) -> Mapping[str, Any]:
        return {"item": {f.name: getattr(obj, f.name) for f in fields(obj)}}

    def repack_object(
        self,
        cls: type[_SimpleStorable],
        contents: Mapping[str, Any],
        _registry: Registry,
    ) -> _SimpleStorable:
        return cls(**contents["item"])


@dataclass(frozen=True)
class StorableValue(Storable, Generic[T], class_id="5a044e9f", unpacker=_SimpleUnpacker()):
    """A storable object that contains a single value."""

    value: T
    """The value to store."""

    _: KW_ONLY

    serializer: Serializer[T] | None = field(default=None, compare=False)
    """The serializer to use for the value."""
    storage: Storage | None = field(default=None, compare=False)
    """The storage to use for the value."""


@dataclass(frozen=True)
class StorableStream(Storable, Generic[T], class_id="31198cfb", unpacker=_SimpleUnpacker()):
    """A storable object that contains a stream of values."""

    value_stream: AsyncIterable[T]
    """The stream of values to store."""

    _: KW_ONLY

    serializer: StreamSerializer[T] | None = field(default=None, compare=False)
    """The serializer to use for the stream."""
    storage: Storage | None = field(default=None, compare=False)
    """The storage to use for the stream."""
