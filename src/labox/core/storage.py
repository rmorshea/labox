from __future__ import annotations

import abc
from typing import TYPE_CHECKING
from typing import Generic
from typing import Protocol
from typing import TypedDict
from typing import TypeVar

from labox._internal._component import Component
from labox._internal._json import DEFAULT_JSON_DECODER
from labox._internal._json import DEFAULT_JSON_ENCODER
from labox._internal._utils import not_implemented

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable

    from labox.common.types import TagMap


T = TypeVar("T")


class Storage(Generic[T], Component):
    """A protocol for storing and retrieving data."""

    @abc.abstractmethod
    @not_implemented
    async def write_data(
        self,
        data: bytes,
        digest: Digest,
        tags: TagMap,
        /,
    ) -> T:
        """Save the given data and return information that can be used to retrieve it.

        Args:
            data: The data to save.
            digest: A digest describing the data.
            tags: Tags from the user or unpacker that describe the data.
        """
        ...

    @abc.abstractmethod
    @not_implemented
    async def read_data(self, info: T, /) -> bytes:
        """Load data using the given information."""
        ...

    @abc.abstractmethod
    @not_implemented
    async def write_data_stream(
        self,
        data_stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
        tags: TagMap,
        /,
    ) -> T:
        """Save the given stream and return information that can be used to retrieve it.

        Args:
            data_stream: An async iterable that yields chunks of data to save.
            get_digest: Function returning the digest of the stream content.
            tags: Tags from the user or unpacker that describe the data.
        """
        ...

    @abc.abstractmethod
    @not_implemented
    def read_data_stream(self, info: T, /) -> AsyncGenerator[bytes]:
        """Load a stream of data using the given information."""
        ...

    def serialize_storage_data(self, info: T) -> str:
        """Dump the storage information to a JSON string."""
        return DEFAULT_JSON_ENCODER.encode(info)

    def deserialize_storage_data(self, data: str) -> T:
        """Load the storage information from a JSON string."""
        return DEFAULT_JSON_DECODER.decode(data)


class Digest(TypedDict):
    """A digest describing serialized data."""

    content_encoding: str | None
    """The encoding of the data."""
    content_type: str
    """The MIME type of the data."""
    content_hash_algorithm: str
    """The algorithm used to hash the data."""
    content_hash: str
    """The hash of the data."""
    content_size: int
    """The size of the data in bytes."""


class StreamDigest(Digest):
    """A digest describing a stream of serialized data."""

    is_complete: bool
    """A flag indicating whether the stream has been read in full."""


class GetStreamDigest(Protocol):
    """A protocol for getting the digest of stream content."""

    def __call__(self, *, allow_incomplete: bool = False) -> StreamDigest:
        """Get the digest of a stream dump.

        Args:
            allow_incomplete: Whether to allow the digest to be incomplete.
               Raises an error if the digest is incomplete and this is False.

        Raises:
            ValueError: If the digest is incomplete and `allow_incomplete` is False.
        """
        ...
