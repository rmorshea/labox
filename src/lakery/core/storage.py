from __future__ import annotations

import abc
from typing import TYPE_CHECKING
from typing import Generic
from typing import Literal
from typing import LiteralString
from typing import Protocol
from typing import TypedDict
from typing import TypeVar
from typing import overload

from lakery.core._registry import Registry
from lakery.core.schema import DataRelation

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import Sequence


R = TypeVar("R", bound=DataRelation)


class ValueStorage(Generic[R], abc.ABC):
    """A protocol for storing and retrieving values."""

    name: LiteralString
    """The name of the storage backend."""
    types: tuple[type[R], ...]
    """The types that the serializer can handle."""
    version: int

    @abc.abstractmethod
    async def write_value(
        self,
        relation: R,
        value: bytes,
        digest: ValueDigest,
        /,
    ) -> R:
        """Save the given value dump."""
        raise NotImplementedError

    @abc.abstractmethod
    async def read_value(self, relation: R, /) -> bytes:
        """Load the value dump for the given relation."""
        raise NotImplementedError


class StreamStorage(ValueStorage[R]):
    """A protocol for storing and retrieving streams."""

    @abc.abstractmethod
    async def write_stream(
        self,
        relation: R,
        stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
        /,
    ) -> R:
        """Save the given stream dump."""
        raise NotImplementedError

    @abc.abstractmethod
    def read_stream(self, relation: R, /) -> AsyncIterable[bytes]:
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
        """Get the digest of a stream dump."""
        ...


class StorageRegistry(Registry[ValueStorage]):
    """A registry of storages."""

    item_description = "Storage"

    def __init__(self, items: Sequence[ValueStorage]) -> None:
        super().__init__(items)
        self.by_type = {
            type_: storage
            # sort stream storages last in order to prioritize them
            for storage in sorted(self.items, key=lambda i: isinstance(i, StreamStorage))
            for type_ in storage.types
        }

    @overload
    def infer_from_data_relation_type(
        self, cls: type[R], *, stream: Literal[True]
    ) -> StreamStorage[R]: ...

    @overload
    def infer_from_data_relation_type(
        self, cls: type[R], *, stream: bool = ...
    ) -> ValueStorage[R] | StreamStorage[R]: ...

    def infer_from_data_relation_type(
        self, cls: type[R], *, stream: bool = False
    ) -> ValueStorage[R] | StreamStorage[R]:
        """Get the first item that can handle the given type or its parent classes."""
        for base in cls.mro():
            if (item := self.by_type.get(base)) and (not stream or isinstance(item, StreamStorage)):
                return item
        stream_msg = "stream " if stream else ""
        msg = f"No {stream_msg}{self.item_description.lower()} found for {cls}."
        raise ValueError(msg)
