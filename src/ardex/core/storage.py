from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING
from typing import LiteralString
from typing import Protocol
from typing import TypedDict
from typing import TypeVar

from ardex.core.registry import Registry
from ardex.core.schema import DataRelation

if TYPE_CHECKING:
    from collections.abc import AsyncIterable


R = TypeVar("R", bound=DataRelation)


class Storage(Protocol[R]):
    """A protocol for storing and retrieving data."""

    name: LiteralString
    """The name of the storage backend."""
    types: tuple[type[R], ...]
    """The types that the serializer can handle."""
    version: int

    async def write_scalar(
        self,
        relation: R,
        scalar: bytes,
        digest: DumpDigest,
        /,
    ) -> R:
        """Save the given scalar dump."""
        ...

    async def read_scalar(self, relation: R, /) -> bytes:
        """Load the scalar dump for the given relation."""
        ...

    async def write_stream(
        self,
        relation: R,
        stream: AsyncIterable[bytes],
        get_digest: DumpDigestGetter,
        /,
    ) -> R:
        """Save the given stream dump."""
        ...

    def read_stream(self, relation: R, /) -> AsyncIterable[bytes]:
        """Load the stream dump for the given relation."""
        ...


class DumpDigest(TypedDict):
    """The digest of a dump."""

    content_hash: str
    """The hash of the dumped content."""
    content_hash_algorithm: str
    """The algorithm used to hash the dumped content."""
    content_size: int
    """The size of the dumped content in bytes."""
    content_type: str
    """The MIME type of the data."""


DumpDigestGetter = Callable[[], DumpDigest]
"""A callable that returns a dump digest."""


class StorageRegistry(Registry[Storage]):
    """A registry of storages."""

    item_description = "Storage"

    if TYPE_CHECKING:

        def get_by_type_inference(self, cls: type[R]) -> Storage[R]: ...  # noqa: D102
