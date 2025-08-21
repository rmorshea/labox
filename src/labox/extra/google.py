from __future__ import annotations

import logging
from contextlib import closing
from typing import TYPE_CHECKING
from typing import NotRequired
from typing import ParamSpec
from typing import Protocol
from typing import TypedDict
from typing import TypeVar
from weakref import ref

from anyio import CapacityLimiter
from anyio.to_thread import run_sync
from google.api_core.exceptions import NotFound
from google.cloud.storage.fileio import DEFAULT_CHUNK_SIZE
from google.cloud.storage.fileio import BlobReader
from google.cloud.storage.fileio import BlobWriter

from labox._internal._logging import PrefixLogger
from labox._internal._temp_path import make_path_from_digest
from labox._internal._temp_path import make_temp_path
from labox.common.exceptions import NoStorageData
from labox.core.storage import Digest
from labox.core.storage import GetStreamDigest
from labox.core.storage import Storage

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable
    from collections.abc import Callable
    from collections.abc import Coroutine

    from google.cloud.storage import Blob
    from google.cloud.storage import Client

    from labox.common.types import TagMap

__all__ = (
    "BlobStorage",
    "ReaderType",
    "WriterType",
)

P = ParamSpec("P")
R = TypeVar("R")

_LOG = logging.getLogger(__name__)


def simple_blob_router(bucket: str, prefix: str = "") -> BlobRouter:
    """Create a simple blob router that returns a BlobPointer based on the bucket and prefix.

    Args:
        bucket: The name of the Google Cloud Storage bucket.
        prefix: An optional prefix to prepend to the blob path.

    Returns:
        A BlobRouter that generates BlobPointers for the specified bucket and prefix.
    """

    def route(digest: Digest, tags: TagMap, *, temp: bool) -> BlobPointer:  # noqa: ARG001
        if temp:
            blob = make_temp_path("/", digest, prefix=prefix)
        else:
            blob = make_path_from_digest("/", digest, prefix=prefix)
        return {"bucket": bucket, "blob": blob, "user_project": None}

    return route


class BlobStorage(Storage["BlobPointer"]):
    """A storage backend that uses Google Cloud Storage."""

    name = "labox.google.blob@v1"

    def __init__(
        self,
        *,
        storage_client: Client,
        storage_router: BlobRouter | None,
        object_chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_concurrency: int | None = None,
        writer_type: WriterType = BlobWriter,
        reader_type: ReaderType = BlobReader,
    ):
        self._storage_client = storage_client
        self._storage_router = storage_router or _read_only(self)
        self._limiter = CapacityLimiter(max_concurrency) if max_concurrency else None
        self._object_chunk_size = object_chunk_size
        self._writer_type = writer_type
        self._reader_type = reader_type
        self._log = PrefixLogger(_LOG, self)

    async def write_data(
        self,
        data: bytes,
        digest: Digest,
        tags: TagMap,
    ) -> BlobPointer:
        """Save the given data."""
        pointer = self._storage_router(digest, tags, temp=False)
        self._log.debug("saving data to %s", pointer)
        bucket = self._storage_client.bucket(
            pointer["bucket"],
            user_project=pointer.get("user_project"),
        )
        blob = bucket.blob(pointer["blob"], chunk_size=self._object_chunk_size)
        blob.metadata = tags
        writer = self._writer_type(blob, content_type=digest["content_type"])
        await self._to_thread(writer.write, data)
        return pointer

    async def read_data(self, pointer: BlobPointer) -> bytes:
        """Load data from the given location."""
        self._log.debug("loading data from %s", pointer)
        bucket = self._storage_client.bucket(
            pointer["bucket"], user_project=pointer.get("user_project")
        )
        reader = self._reader_type(bucket.blob(pointer["blob"], chunk_size=self._object_chunk_size))
        with closing(reader) as reader:
            try:
                return await self._to_thread(reader.read)
            except NotFound as error:
                msg = f"Failed to load value from {pointer}"
                raise NoStorageData(msg) from error

    async def write_data_stream(
        self,
        data_stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
        tags: TagMap,
    ) -> BlobPointer:
        """Save the given data steam."""
        initial_digest = get_digest(allow_incomplete=True)

        temp_pointer = self._storage_router(initial_digest, tags, temp=True)

        bucket = self._storage_client.bucket(
            temp_pointer["bucket"],
            user_project=temp_pointer.get("user_project"),
        )
        temp_blob = bucket.blob(temp_pointer["blob"], chunk_size=self._object_chunk_size)
        self._log.debug("temporarily saving data to %s", temp_pointer)
        temp_blob.metadata = tags
        writer = self._writer_type(temp_blob, content_type=initial_digest["content_type"])
        try:
            async for chunk in data_stream:
                await self._to_thread(writer.write, chunk)
        except Exception:
            await self._to_thread(temp_blob.delete)
            raise
        finally:
            await self._to_thread(writer.close)

        try:
            final_pointer = self._storage_router(get_digest(), tags, temp=False)
            self._log.debug("moving data to final location %s", final_pointer)

            bucket = self._storage_client.bucket(
                final_pointer["bucket"],
                user_project=final_pointer.get("user_project"),
            )
            await self._to_thread(
                bucket.copy_blob,
                temp_blob,
                bucket,
                final_pointer["blob"],
                # Avoid potential race conditions and data corruptions. Request is aborted
                # if the object's generation number does not match this precondition.
                if_generation_match=0,
            )
        finally:
            self._log.debug("deleting temporary data %s", temp_blob.name)
            await self._to_thread(temp_blob.delete)

        return final_pointer

    async def read_data_stream(self, pointer: BlobPointer) -> AsyncGenerator[bytes]:
        """Load a data stream from the given location."""
        self._log.debug("loading data stream from %s", pointer)
        bucket = self._storage_client.bucket(
            pointer["bucket"],
            user_project=pointer.get("user_project"),
        )
        blob = bucket.blob(pointer["blob"], chunk_size=self._object_chunk_size)
        with closing(self._reader_type(blob)) as reader:
            try:
                while chunk := await self._to_thread(reader.read, self._object_chunk_size):
                    yield chunk
            except NotFound as error:
                msg = f"Failed to load stream from {pointer}"
                raise NoStorageData(msg) from error

    def _to_thread(
        self,
        func: Callable[P, R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Coroutine[None, None, R]:
        return run_sync(lambda: func(*args, **kwargs), limiter=self._limiter)


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


class BlobPointer(TypedDict):
    """A pointer to a blob in Google Cloud Storage."""

    bucket: str
    """The name of the bucket where the blob is stored."""
    blob: str
    """The path to the blob within the bucket."""
    user_project: NotRequired[str | None]
    """The project ID to be billed for API requests made via the bucket."""


class BlobRouter(Protocol):
    """A protocol for routing data to Google Cloud Storage by returning a BlobPointer."""

    def __call__(self, digest: Digest, tags: TagMap, *, temp: bool) -> BlobPointer:
        """Return a BlobPointer for the given digest and tags.

        Args:
            digest: The digest of the data to route.
            tags: The tags to associate with the content.
            temp: Whether to create a temporary path for the data - used for streaming data.

        Returns:
            A BlobPointer that can be used to access the data.
        """
        ...


def _read_only(storage: BlobStorage) -> BlobRouter:
    storage_ref = ref(storage)

    def router(digest: Digest, tags: TagMap, *, temp: bool) -> BlobPointer:
        msg = f"{storage_ref()} is read-only and cannot write data."
        raise NotImplementedError(msg)

    return router
