from __future__ import annotations

import abc
from typing import TYPE_CHECKING
from typing import Generic
from typing import LiteralString
from typing import Protocol
from typing import TypedDict
from typing import TypeVar

from lakery.core._registry import Registry

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable
    from collections.abc import Sequence

    from lakery.common.utils import TagMap


T = TypeVar("T")


class Storage(Generic[T], abc.ABC):
    """A protocol for storing and retrieving data."""

    name: LiteralString
    """The name of the storage."""
    version: int
    """The version of the storage."""

    @abc.abstractmethod
    async def put_content(
        self,
        value: bytes,
        digest: ValueDigest,
        tags: TagMap,
        /,
    ) -> T:
        """Save the given value and return data that can be used to retrieve it."""
        raise NotImplementedError

    @abc.abstractmethod
    async def get_content(self, data: T, /) -> bytes:
        """Load a value using the given data."""
        raise NotImplementedError

    @abc.abstractmethod
    async def put_content_stream(
        self,
        stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
        tags: TagMap,
        /,
    ) -> T:
        """Save the given stream and return data that can be used to retrieve it."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_content_stream(self, data: T, /) -> AsyncGenerator[bytes]:
        """Load a stream using the given data."""
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


class StorageRegistry(Registry[str, Storage]):
    """A registry of storages."""

    value_description = "Storage"

    def __init__(
        self,
        storages: Sequence[Storage] = (),
        *,
        first_is_default: bool = True,
        ignore_conflicts: bool = False,
    ) -> None:
        super().__init__(storages, ignore_conflicts=ignore_conflicts)
        self._first_is_default = first_is_default

    @property
    def default(self) -> Storage:
        """Get the default storage."""
        if not self._first_is_default:
            msg = f"Usage of default {self.value_description.lower()} disabled."
            raise ValueError(msg)
        return self[next(iter(self))]

    def get_key(self, storage: Storage) -> str:
        """Get the key for the given storage."""
        return storage.name
