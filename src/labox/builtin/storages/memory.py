from __future__ import annotations

import logging
from io import BytesIO
from typing import TYPE_CHECKING

from labox._internal._logging import PrefixLogger
from labox._internal._temp_path import make_file_name_from_digest
from labox.core.storage import Storage

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable

    from labox.common.types import TagMap
    from labox.core.storage import Digest
    from labox.core.storage import GetStreamDigest

__all__ = ("MemoryStorage", "memory_storage")

_LOG = logging.getLogger(__name__)


class MemoryStorage(Storage[str]):
    """A storage backend for testing that saves data to local files."""

    name = "labox.file@v1"
    version = 1

    def __init__(
        self,
        chunk_size: int = 5 * 1024**2,  # 5 MB
    ) -> None:
        self.store: dict[str, bytes] = {}
        self.chunk_size = chunk_size
        self._log = PrefixLogger(_LOG, self)

    async def write_data(
        self,
        data: bytes,
        digest: Digest,
        _tags: TagMap,
    ) -> str:
        """Save the given data."""
        key = make_file_name_from_digest(digest)
        self._log.debug("saving data to %s", key)
        self.store[key] = data
        return key

    async def read_data(self, key: str) -> bytes:
        """Load data from the given location."""
        self._log.debug("loading data from %s", key)
        return self.store[key]

    async def write_data_stream(
        self,
        data_stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
        _tags: TagMap,
    ) -> str:
        """Save the given data stream."""
        buffer = BytesIO()
        self._log.debug("temporarily saving data to buffer")
        async for chunk in data_stream:
            buffer.write(chunk)
        buffer.seek(0)
        initial_digest = get_digest()
        key = make_file_name_from_digest(initial_digest)
        self.store[key] = buffer.read()
        self._log.debug("moving data to final location %s", key)
        return key

    async def read_data_stream(self, location: str) -> AsyncGenerator[bytes]:
        """Load a stream of data from the given location."""
        self._log.debug("loading data stream from %s", location)
        data = self.store[location]
        while data:
            chunk = data[: self.chunk_size]
            data = data[self.chunk_size :]
            yield chunk


memory_storage = MemoryStorage()
"""Default instance of MemoryStorage for testing purposes."""
