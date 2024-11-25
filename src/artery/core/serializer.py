from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import LiteralString
from typing import Protocol
from typing import TypedDict
from typing import TypeVar

from artery.core.registry import Registry

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator
    from collections.abc import Sequence

T = TypeVar("T")


class Serializer(Protocol[T]):
    """A protocol for serializing and deserializing objects."""

    name: LiteralString
    """The name of the serializer."""
    types: tuple[type[T], ...]
    """The types that the serializer can handle."""

    async def dump_scalar(self, scalar: T) -> ScalarData:
        """Serialize the given scalar."""
        ...

    async def dump_stream(self, stream: AsyncIterator[T]) -> StreamData:
        """Serialize the given stream."""
        ...

    async def load_scalar(self, dump: ScalarData) -> T:
        """Deserialize the given scalar."""
        ...

    async def load_stream(self, dump: StreamData) -> AsyncIterator[T]:
        """Deserialize the given stream."""
        ...


class ScalarData(TypedDict):
    """The serialized representation of a scalar value."""

    content_bytes: bytes
    """The serialized data."""
    content_type: str
    """The MIME type of the data."""


class StreamData(TypedDict):
    """The serialized representation of a stream of values."""

    content_bytes: AsyncIterable[bytes]
    """The serialized data stream."""
    content_type: str
    """The MIME type of the data stream as a whole."""


class SerializerRegistry(Registry[Serializer]):
    def __init__(self, items: Sequence[Serializer[Any]]) -> None:
        super().__init__(items)
        # ensure first declared item has highest priority for type inference
        self.by_type = {t: s for s in reversed(self.items) for t in s.types}
        """A mapping of types to serializers."""

    def get_by_type_inference(self, cls: type[T]) -> Serializer[T] | None:
        """Get the first serializer that can handle the given type or its parent classes."""
        for base in cls.mro():
            if base in self.by_type:
                return self.by_type[base]
        return None
