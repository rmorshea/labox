from __future__ import annotations

import abc
from typing import TYPE_CHECKING
from typing import Generic
from typing import LiteralString
from typing import Protocol
from typing import TypedDict
from typing import TypeVar

from lakery.core._registry import Registry
from lakery.core.schema import DataDescriptor

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable
    from collections.abc import Sequence


D = TypeVar("D", bound=DataDescriptor)


class Storage(Generic[D], abc.ABC):
    """A protocol for storing and retrieving data."""

    name: LiteralString
    """The name of the storage backend."""
    types: tuple[type[D], ...] = ()
    """The types that the serializer can handle."""
    version: int

    @abc.abstractmethod
    async def put_value(
        self,
        relation: D,
        value: bytes,
        digest: ValueDigest,
        /,
    ) -> D:
        """Save the given value dump."""
        raise NotImplementedError

    @abc.abstractmethod
    async def get_value(self, relation: D, /) -> bytes:
        """Load the value dump for the given relation."""
        raise NotImplementedError

    @abc.abstractmethod
    async def put_stream(
        self,
        relation: D,
        stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
        /,
    ) -> D:
        """Save the given stream dump."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_stream(self, relation: D, /) -> AsyncGenerator[bytes]:
        """Load the stream dump for the given relation."""
        raise NotImplementedError


class ValueDigest(TypedDict):
    """The digest of a dump."""

    content_encoding: str | None
    """The encoding of the dumped content."""
    content_type: str
    """The MIME type of the data."""
    content_hash_algorithm: str
    """The algorithm used to hash the dumped content."""
    content_hash: str
    """The hash of the dumped content."""
    content_size: int
    """The size of the dumped content in bytes."""


class StreamDigest(ValueDigest):
    """The digest of a stream dump."""

    is_complete: bool
    """A flag indicating whether the stream has been read in full."""


class GetStreamDigest(Protocol):
    """A protocol for getting the digest of a stream dump."""

    def __call__(self, *, allow_incomplete: bool = False) -> StreamDigest:
        """Get the digest of a stream dump.

        Args:
            allow_incomplete: Whether to allow the digest to be incomplete.
               Raises an error if the digest is incomplete and this is False.

        Raises:
            ValueError: If the digest is incomplete and `allow_incomplete` is False.
        """
        ...


class StorageRegistry(Registry[Storage]):
    """A registry of storages."""

    item_description = "Storage"

    def __init__(self, items: Sequence[Storage]) -> None:
        super().__init__(items)
        self.by_type = {type_: s for s in self.items for type_ in s.types}

    def infer_from_data_relation_type(self, cls: type[D]) -> Storage[D]:
        """Get the first item that can handle the given type or its parent classes."""
        for base in cls.mro():
            if item := self.by_type.get(base):
                return item
        msg = f"No {self.item_description.lower()} found for {cls}."
        raise ValueError(msg)
