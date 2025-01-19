from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from anyio import create_task_group

from lakery.common.anyio import start_async_iterator
from lakery.core.storage import Storage
from lakery.extra._utils import make_path_parts_from_digest

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable
    from collections.abc import Iterator

    from lakery.common.utils import TagMap
    from lakery.core.storage import Digest
    from lakery.core.storage import GetStreamDigest


class LocalFileStorage(Storage[str]):
    """A storage backend for testing that saves data to local files."""

    name = "lakery.os.local_file"
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
        (self.path / "scratch").mkdir()

    async def put_data(
        self,
        data: bytes,
        digest: Digest,
        _tags: TagMap,
    ) -> str:
        """Save the given data."""
        content_path = self.path.joinpath(*make_path_parts_from_digest(digest))
        if not content_path.exists():
            content_path.parent.mkdir(parents=True, exist_ok=True)
            content_path.write_bytes(data)
        return _path_to_str(content_path)

    async def get_data(self, location: str) -> bytes:
        """Load data from the given location."""
        return _str_to_path(location).read_bytes()

    async def put_data_stream(
        self,
        data_stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
        _tags: TagMap,
    ) -> str:
        """Save the given data stream."""
        scratch_path = self._get_scratch_path()
        with scratch_path.open("wb") as file:
            async for chunk in data_stream:
                file.write(chunk)
        try:
            final_digest = get_digest()
            content_path = self.path.joinpath(*make_path_parts_from_digest(final_digest))
            if not content_path.exists():
                content_path.parent.mkdir(parents=True, exist_ok=True)
                scratch_path.rename(content_path)
        finally:
            scratch_path.unlink(missing_ok=True)
        return _path_to_str(content_path)

    async def get_data_stream(self, location: str) -> AsyncGenerator[bytes]:
        """Load a stream of data from the given location."""
        path = _str_to_path(location)
        async with create_task_group() as tg:
            with start_async_iterator(tg, _iter_file_chunks(path, self.chunk_size)) as chunks:
                async for c in chunks:
                    yield c

    def _get_scratch_path(self) -> Path:
        return self.path / "scratch" / uuid4().hex


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
