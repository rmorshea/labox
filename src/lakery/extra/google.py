from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING
from typing import ParamSpec
from typing import Protocol
from typing import TypeVar

from anyio import CapacityLimiter
from anyio.to_thread import run_sync
from google.api_core.exceptions import NotFound
from google.cloud.storage.fileio import DEFAULT_CHUNK_SIZE
from google.cloud.storage.fileio import BlobReader
from google.cloud.storage.fileio import BlobWriter

from lakery.common.exceptions import NoStorageData
from lakery.core.storage import GetStreamDigest
from lakery.core.storage import Storage
from lakery.core.storage import ValueDigest
from lakery.extra._utils import make_path_from_digest
from lakery.extra._utils import make_temp_path

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable
    from collections.abc import Callable
    from collections.abc import Coroutine

    from google.cloud.storage import Blob
    from google.cloud.storage import Bucket

    from lakery.common.utils import TagMap

P = ParamSpec("P")
R = TypeVar("R")


class WriterType(Protocol):
    """A protocol for creating a blob writer."""

    def __call__(self, blob: Blob, *, content_type: str) -> BlobWriter:
        """Create a blob writer."""
        ...


class ReaderType(Protocol):
    """A protocol for creating a blob reader."""

    def __call__(self, blob: Blob) -> BlobReader:
        """Create a blob reader."""
        ...


class BlobStorage(Storage[str]):
    """A storage backend that uses Google Cloud Storage."""

    name = "lakery.google.blob"
    version = 1

    def __init__(
        self,
        *,
        bucket_client: Bucket,
        object_name_prefix: str = "",
        object_chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_concurrency: int | None = None,
        writer_type: WriterType = BlobWriter,
        reader_type: ReaderType = BlobReader,
    ):
        self._bucket = bucket_client
        self._limiter = CapacityLimiter(max_concurrency) if max_concurrency else None
        self._object_name_prefix = object_name_prefix
        self._object_chunk_size = object_chunk_size
        self._writer_type = writer_type
        self._reader_type = reader_type
        self.__current_bucket = None

    async def put_value(
        self,
        value: bytes,
        digest: ValueDigest,
        tags: TagMap,
    ) -> str:
        """Save the given value dump."""
        location = make_path_from_digest("/", digest, prefix=self._object_name_prefix)
        blob = self._bucket.blob(location, chunk_size=self._object_chunk_size)
        blob.metadata = tags
        writer = self._writer_type(blob, content_type=digest["content_type"])
        await self._to_thread(writer.write, value)
        return location

    async def get_value(self, location: str) -> bytes:
        """Load the value dump for the given relation."""
        reader = self._reader_type(self._bucket.blob(location, chunk_size=self._object_chunk_size))
        with closing(reader) as reader:
            try:
                return await self._to_thread(reader.read)
            except NotFound as error:
                msg = f"Failed to load value from {location!r}"
                raise NoStorageData(msg) from error

    async def put_stream(
        self,
        stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
        tags: TagMap,
    ) -> str:
        """Save the given stream dump."""
        initial_digest = get_digest(allow_incomplete=True)

        temp_blob = self._bucket.blob(
            make_temp_path("/", initial_digest, prefix=self._object_name_prefix),
            chunk_size=self._object_chunk_size,
        )
        temp_blob.metadata = tags
        writer = self._writer_type(temp_blob, content_type=initial_digest["content_type"])
        try:
            async for chunk in stream:
                await self._to_thread(writer.write, chunk)
        except Exception:
            await self._to_thread(temp_blob.delete)
            raise
        finally:
            await self._to_thread(writer.close)

        try:
            final_location = make_path_from_digest(
                "/",
                get_digest(),
                prefix=self._object_name_prefix,
            )
            await self._to_thread(
                self._bucket.copy_blob,
                temp_blob,
                self._bucket,
                make_path_from_digest("/", get_digest(), prefix=self._object_name_prefix),
                # Avoid potential race conditions and data corruptions. Request is aborted
                # if the object's generation number does not match this precondition.
                if_generation_match=0,
            )
        finally:
            await self._to_thread(temp_blob.delete)

        return final_location

    async def get_stream(self, location: str) -> AsyncGenerator[bytes]:
        """Load the stream dump for the given relation."""
        blob = self._bucket.blob(location, chunk_size=self._object_chunk_size)
        with closing(self._reader_type(blob)) as reader:
            try:
                while chunk := await self._to_thread(reader.read, self._object_chunk_size):
                    yield chunk
            except NotFound as error:
                msg = f"Failed to load stream from {location!r}"
                raise NoStorageData(msg) from error

    def _to_thread(
        self,
        func: Callable[P, R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Coroutine[None, None, R]:
        return run_sync(lambda: func(*args, **kwargs), limiter=self._limiter)
