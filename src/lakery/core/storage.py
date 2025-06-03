from __future__ import annotations

import abc
import json
from typing import TYPE_CHECKING
from typing import ClassVar
from typing import Generic
from typing import LiteralString
from typing import Protocol
from typing import TypedDict
from typing import TypeVar

from lakery._internal.utils import validate_versioned_class_name

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable

    from lakery._internal.utils import TagMap


T = TypeVar("T")


class Storage(Generic[T], abc.ABC):
    """A protocol for storing and retrieving data."""

    name: ClassVar[LiteralString]
    """The name of the storage."""

    def __init_subclass__(cls) -> None:
        validate_versioned_class_name(cls)

    @abc.abstractmethod
    async def write_data(
        self,
        data: bytes,
        digest: Digest,
        tags: TagMap,
        /,
    ) -> T:
        """Save the given data and return information that can be used to retrieve it."""
        raise NotImplementedError

    @abc.abstractmethod
    async def read_data(self, info: T, /) -> bytes:
        """Load data using the given information."""
        raise NotImplementedError

    @abc.abstractmethod
    async def write_data_stream(
        self,
        data_stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
        tags: TagMap,
        /,
    ) -> T:
        """Save the given stream and return information that can be used to retrieve it."""
        raise NotImplementedError

    @abc.abstractmethod
    def read_data_stream(self, info: T, /) -> AsyncGenerator[bytes]:
        """Load a stream of data using the given information."""
        raise NotImplementedError

    def dump_json_storage_data(self, info: T) -> str:
        """Dump the storage information to a JSON string."""
        return json.dumps(info)

    def load_json_storage_data(self, data: str) -> T:
        """Load the storage information from a JSON string."""
        return json.loads(data)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name!r})"


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
