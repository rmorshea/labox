from __future__ import annotations

from mimetypes import guess_extension
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING
from typing import Any
from typing import Self
from uuid import uuid4

from datos.core.schema import DataRelation
from datos.core.storage import Storage

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator

    from datos.core.storage import DumpDigest
    from datos.core.storage import DumpDigestGetter


class TemporaryDirectoryStorage(Storage[DataRelation]):
    """A storage backend for testing that saves data to a temporary directory."""

    name = "datos.tempfile"
    version = 1
    types = (DataRelation,)

    def __init__(
        self,
        tempdir: TemporaryDirectory | str | None = None,
        chunk_size: int = 1024,
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

    def __enter__(self) -> Self:
        if not hasattr(self, "tempdir"):
            msg = f"{self} does not own the temporary directory {self.path!r}"
            raise RuntimeError(msg)
        return self

    def __exit__(self, *_: Any) -> None:
        self.tempdir.cleanup()

    async def write_scalar(
        self,
        relation: DataRelation,
        scalar: bytes,
        digest: DumpDigest,
    ) -> DataRelation:
        """Save the given scalar dump."""
        content_path = self._get_content_path(digest["content_type"], digest["content_hash"])
        if not content_path.exists():
            content_path.write_bytes(scalar)
        return relation

    async def read_scalar(self, relation: DataRelation) -> bytes:
        """Load the scalar dump for the given relation."""
        content_path = self._get_content_path(relation.rel_content_type, relation.rel_content_hash)
        return content_path.read_bytes()

    async def write_stream(
        self,
        relation: DataRelation,
        stream: AsyncIterable[bytes],
        get_digest: DumpDigestGetter,
    ) -> DataRelation:
        """Save the given stream dump."""
        scratch_path = self._get_scratch_path()
        with scratch_path.open("wb") as file:
            async for chunk in stream:
                file.write(chunk)
        try:
            digest = get_digest()
            content_path = self._get_content_path(digest["content_type"], digest["content_hash"])
            if not content_path.exists():
                scratch_path.replace(content_path)
        finally:
            scratch_path.unlink(missing_ok=True)
        return relation

    async def read_stream(self, relation: DataRelation) -> AsyncIterator[bytes]:
        """Load the stream dump for the given relation."""
        content_path = self._get_content_path(relation.rel_content_type, relation.rel_content_hash)

        with content_path.open("rb") as file:
            while chunk := file.read(self.chunk_size):
                yield chunk

    def _get_content_path(self, content_type: str, content_hash: str) -> Path:
        ext = guess_extension(content_type) or ""
        return (self.path / f"hash_{content_hash}").with_suffix(ext)

    def _get_scratch_path(self) -> Path:
        return self.path / "scratch" / uuid4().hex
