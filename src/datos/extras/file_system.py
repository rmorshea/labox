from collections.abc import Sequence
from mimetypes import guess_extension
from pathlib import Path

from datos.core.schema import Record
from datos.core.storage import Storage
from datos.utils.misc import slugify


class FileSystemStorage(Storage):
    def __init__(self, root: Path | str):
        self.root = Path(root)
        self.name = f"atery_file-system_v1_{slugify(self.root)}"

    async def create_many(self, refs: Sequence[Record], contents: Sequence[bytes]) -> None:
        for ref, data in zip(refs, contents, strict=False):
            self.create_one(ref, data)

    async def read_many(self, refs: Sequence[Record]) -> Sequence[bytes]:
        return [self.read_one(ref) for ref in refs]

    def create_one(self, ref: Record, content: bytes) -> None:
        path = self.get_ref_path(ref)
        if not path.exists():
            with path.open("wb") as file:
                file.write(content)

    def read_one(self, ref: Record) -> bytes:
        path = self.get_ref_path(ref)
        with path.open("rb") as file:
            return file.read()

    def get_ref_path(self, ref: Record) -> Path:
        file_stem = slugify(ref.content_hash)
        file_ext = guess_extension(ref.content_type)
        return self.root / f"{file_stem}{file_ext}"
