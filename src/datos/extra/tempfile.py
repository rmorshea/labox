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
    from collections.abc import AsyncIterator

    from datos.core.serializer import ScalarDump
    from datos.core.serializer import StreamDump


class TempDirectoryStorage(Storage[DataRelation]):
    """A storage backend for testing that saves data to a temporary directory."""

    name = "datos.temp_directory"
    version = 1
    types = (DataRelation,)

    def __init__(self, chunk_size: int = 1024) -> None:
        self.tempdir = TemporaryDirectory()
        self.path = Path(self.tempdir.name)
        self.chunk_size = chunk_size

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: Any) -> None:
        self.tempdir.cleanup()

    async def write_scalar(self, relation: DataRelation, dump: ScalarDump) -> DataRelation:
        """Save the given scalar dump."""
        content_path = self._get_content_path(dump["content_type"], dump["content_hash"])
        if not content_path.exists():
            content_path.write_bytes(dump["scalar"])
        return relation

    async def read_scalar(self, relation: DataRelation) -> ScalarDump:
        """Load the scalar dump for the given relation."""
        content_path = self._get_content_path(relation.rel_content_type, relation.rel_content_hash)
        return {
            "content_type": relation.rel_content_type,
            "serializer_name": relation.rel_serializer_name,
            "serializer_version": relation.rel_serializer_version,
            "content_hash": relation.rel_content_hash,
            "content_hash_algorithm": relation.rel_content_hash_algorithm,
            "content_size": relation.rel_content_size,
            "scalar": content_path.read_bytes(),
        }

    async def write_stream(self, relation: DataRelation, dump: StreamDump) -> DataRelation:
        """Save the given stream dump."""
        scratch_path = self._get_scratch_path()
        with scratch_path.open("wb") as file:
            async for chunk in dump["stream"]:
                file.write(chunk)
        try:
            if "content_hash" not in dump:
                msg = "Stream dump missing 'content_hash'"
                raise RuntimeError(msg)
            content_path = self._get_content_path(dump["content_type"], dump["content_hash"])
            if not content_path.exists():
                scratch_path.replace(content_path)
        finally:
            scratch_path.unlink(missing_ok=True)
        return relation

    async def read_stream(self, relation: DataRelation) -> StreamDump:
        """Load the stream dump for the given relation."""
        content_path = self._get_content_path(relation.rel_content_type, relation.rel_content_hash)

        async def stream() -> AsyncIterator[bytes]:  # noqa: RUF029
            with content_path.open("rb") as file:
                while chunk := file.read(self.chunk_size):
                    yield chunk

        return {
            "content_type": relation.rel_content_type,
            "serializer_name": relation.rel_serializer_name,
            "serializer_version": relation.rel_serializer_version,
            "content_hash": relation.rel_content_hash,
            "content_hash_algorithm": relation.rel_content_hash_algorithm,
            "content_size": relation.rel_content_size,
            "stream": stream(),
        }

    def _get_content_path(self, content_type: str, content_hash: str) -> Path:
        ext = guess_extension(content_type) or ""
        return (self.path / f"hash_{content_hash}").with_suffix(ext)

    def _get_scratch_path(self) -> Path:
        return self.path / "scratch" / uuid4().hex
