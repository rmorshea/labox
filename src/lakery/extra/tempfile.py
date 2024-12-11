from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING
from typing import Any
from typing import Self
from uuid import uuid4

from anyio import create_task_group

from lakery.core.schema import DataRelation
from lakery.core.storage import GetStreamDigest
from lakery.core.storage import StreamStorage
from lakery.utils.anyio import start_async_iterator
from lakery.utils.misc import StorageLocationMaker
from lakery.utils.misc import make_data_relation_path

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator
    from collections.abc import Iterator

    from lakery.core.storage import ValueDigest


class TemporaryDirectoryStorage(StreamStorage[DataRelation]):
    """A storage backend for testing that saves data to a temporary directory."""

    name = "lakery.tempfile"
    version = 1
    types = (DataRelation,)

    def __init__(
        self,
        tempdir: TemporaryDirectory | str | None = None,
        chunk_size: int = 1024**2,  # 1MB chunk size by default
        make_path: StorageLocationMaker = make_data_relation_path,
    ) -> None:
        match tempdir:
            case None:
                self.tempdir = TemporaryDirectory()
                self.path = Path(self.tempdir.name)
            case str(path):
                self.path = Path(path)
            case tempdir:
                self.tempdir = tempdir
                self.path = Path(tempdir.name)
        self.chunk_size = chunk_size
        (self.path / "scratch").mkdir()
        self._make_path = make_path

    def __enter__(self) -> Self:
        if not hasattr(self, "tempdir"):
            msg = f"{self} does not own the temporary directory {self.path!r}"
            raise RuntimeError(msg)
        return self

    def __exit__(self, *_: Any) -> None:
        self.tempdir.cleanup()

    async def write_value(
        self,
        relation: DataRelation,
        value: bytes,
        digest: ValueDigest,
    ) -> DataRelation:
        """Save the given value dump."""
        content_path = Path(self._make_path(self, relation, digest))
        if not content_path.exists():
            content_path.write_bytes(value)
        return relation

    async def read_value(self, relation: DataRelation) -> bytes:
        """Load the value dump for the given relation."""
        content_path = Path(
            self._make_path(
                self,
                relation,
                {
                    "content_type": relation.rel_content_type,
                    "content_hash_algorithm": relation.rel_content_hash_algorithm,
                    "content_hash": relation.rel_content_hash,
                    "content_size": relation.rel_content_size,
                    "content_encoding": relation.rel_content_encoding,
                },
            )
        )
        return content_path.read_bytes()

    async def write_stream(
        self,
        relation: DataRelation,
        stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
    ) -> DataRelation:
        """Save the given stream dump."""
        scratch_path = self._get_scratch_path()
        with scratch_path.open("wb") as file:
            async for chunk in stream:
                file.write(chunk)
        try:
            final_digest = get_digest()
            content_path = Path(self._make_path(self, relation, final_digest))
            if not content_path.exists():
                scratch_path.replace(content_path)
        finally:
            scratch_path.unlink(missing_ok=True)
        return relation

    async def read_stream(self, relation: DataRelation) -> AsyncIterator[bytes]:
        """Load the stream dump for the given relation."""
        path = Path(
            self._make_path(
                self,
                relation,
                {
                    "content_type": relation.rel_content_type,
                    "content_hash_algorithm": relation.rel_content_hash_algorithm,
                    "content_hash": relation.rel_content_hash,
                    "content_size": relation.rel_content_size,
                    "content_encoding": relation.rel_content_encoding,
                },
            ),
        )

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
