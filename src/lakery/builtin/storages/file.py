from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from anyio import create_task_group

from lakery._internal._anyio import start_as_async_iterator
from lakery._internal._temp_path import make_file_name_from_digest
from lakery.core.storage import Storage

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable
    from collections.abc import Iterator

    from lakery.common.types import TagMap
    from lakery.core.storage import Digest
    from lakery.core.storage import GetStreamDigest

__all__ = ("FileStorage",)

_LOG = logging.getLogger(__name__)


class FileStorage(Storage[str]):
    """A storage backend for testing that saves data to local files."""

    name = "lakery.file@v1"
    version = 1

    def __init__(
        self,
        path: Path | str,
        *,
        mkdir: bool = False,
        chunk_size: int = 5 * 1024**2,  # 5 MB
    ) -> None:
        self.path = Path(path)
        if mkdir:
            self.path.mkdir(parents=True, exist_ok=True)
        self.chunk_size = chunk_size
        (self.path / "temp").mkdir()

    async def write_data(
        self,
        data: bytes,
        digest: Digest,
        _tags: TagMap,
    ) -> str:
        """Save the given data."""
        content_path = self.path / make_file_name_from_digest(digest)
        _LOG.debug("Saving data to %s", content_path)
        if not content_path.exists():
            content_path.parent.mkdir(parents=True, exist_ok=True)
            content_path.write_bytes(data)
        return _path_to_str(content_path)

    async def read_data(self, location: str) -> bytes:
        """Load data from the given location."""
        path = _str_to_path(location)
        _LOG.debug("Loading data from %s", path)
        return path.read_bytes()

    async def write_data_stream(
        self,
        data_stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
        _tags: TagMap,
    ) -> str:
        """Save the given data stream."""
        temp_path = self._get_temp_path()
        _LOG.debug("Temporarily saving data to %s", temp_path)
        with temp_path.open("wb") as file:
            async for chunk in data_stream:
                file.write(chunk)
        try:
            final_digest = get_digest()
            content_path = self.path / make_file_name_from_digest(final_digest)
            _LOG.debug("Moving data to final location %s", content_path)
            if not content_path.exists():
                content_path.parent.mkdir(parents=True, exist_ok=True)
                temp_path.rename(content_path)
        finally:
            _LOG.debug("Deleting temporary file %s", temp_path)
            temp_path.unlink(missing_ok=True)
        return _path_to_str(content_path)

    async def read_data_stream(self, location: str) -> AsyncGenerator[bytes]:
        """Load a stream of data from the given location."""
        path = _str_to_path(location)
        _LOG.debug("Loading data stream from %s", path)
        async with create_task_group() as tg:
            with start_as_async_iterator(tg, _iter_file_chunks(path, self.chunk_size)) as chunks:
                async for c in chunks:
                    yield c

    def _get_temp_path(self) -> Path:
        return self.path / "temp" / uuid4().hex


def _iter_file_chunks(file: Path, chunk_size: int) -> Iterator[bytes]:
    with file.open("rb") as f:
        while chunk := f.read(chunk_size):
            yield chunk


if sys.version_info < (3, 13):
    _path_to_str = str
    _str_to_path = Path
else:
    _path_to_str = Path.as_uri
    _str_to_path = Path.from_uri
