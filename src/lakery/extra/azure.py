from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from typing import Protocol
from typing import TypedDict
from weakref import ref

from anyio import CapacityLimiter
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import ContentSettings

from lakery._internal._temp_path import make_path_from_digest
from lakery._internal._temp_path import make_temp_path
from lakery.common.exceptions import NoStorageData
from lakery.core.storage import Digest
from lakery.core.storage import GetStreamDigest
from lakery.core.storage import Storage
from lakery.core.storage import StreamDigest

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable

    from azure.storage.blob.aio import BlobServiceClient

    from lakery.common.types import TagMap


__all__ = ("BlobStorage",)

_LOG = logging.getLogger(__name__)


def simple_blob_router(container: str, prefix: str = "") -> BlobRouter:
    """Create a simple blob router that returns a BlobPointer based on the container and prefix.

    Args:
        container: The name of the Azure Blob Storage container.
        prefix: An optional prefix to prepend to the blob path.


    Returns:
        A BlobRouter that generates BlobPointers for the specified container and prefix.
    """

    def route(digest: Digest | StreamDigest, tags: TagMap, *, temp: bool) -> BlobPointer:  # noqa: ARG001
        if temp:
            blob = make_temp_path("/", digest, prefix=prefix)
        else:
            blob = make_path_from_digest("/", digest, prefix=prefix)
        return {"container": container, "blob": blob}

    return route


class BlobStorage(Storage["BlobPointer"]):
    """Storage for Azure Blob data."""

    name = "lakery.azure.blob@v1"

    def __init__(
        self,
        *,
        service_client: BlobServiceClient,
        blob_router: BlobRouter | None,
        path_prefix: str = "",
        max_concurrency: int | None = None,
    ):
        self._service_client = service_client
        self._blob_router = blob_router or _read_only(self)
        self._path_prefix = path_prefix
        self._limiter = CapacityLimiter(max_concurrency) if max_concurrency else None

    async def write_data(
        self,
        data: bytes,
        digest: Digest,
        tags: TagMap,
    ) -> BlobPointer:
        """Save the given value data."""
        pointer = self._blob_router(digest, tags, temp=False)
        _LOG.debug("Saving data to %s", pointer)
        blob_client = self._service_client.get_blob_client(
            container=pointer["container"], blob=pointer["blob"]
        )
        await blob_client.upload_blob(
            data,
            content_settings=ContentSettings(
                content_type=digest["content_type"],
                content_encoding=digest.get("content_encoding"),
            ),
            tags=tags,
        )
        return pointer

    async def read_data(self, pointer: BlobPointer) -> bytes:
        """Load data from the given location."""
        _LOG.debug("Loading data from %s", pointer)
        blob_client = self._service_client.get_blob_client(
            container=pointer["container"], blob=pointer["blob"]
        )
        try:
            blob_reader = await blob_client.download_blob()
        except ResourceNotFoundError as exc:
            msg = f"Failed to load value from {pointer!r}"
            raise NoStorageData(msg) from exc
        return await blob_reader.readall()

    async def write_data_stream(
        self,
        data_stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
        tags: TagMap,
    ) -> BlobPointer:
        """Save the given data stream."""
        initial_digest = get_digest(allow_incomplete=True)
        temp_pointer = self._blob_router(initial_digest, tags, temp=True)
        _LOG.debug("Temporarily saving data to %s", temp_pointer)

        temp_blob_client = self._service_client.get_blob_client(
            container=temp_pointer["container"], blob=temp_pointer["blob"]
        )
        await temp_blob_client.upload_blob(
            data_stream,
            content_settings=ContentSettings(
                content_type=initial_digest["content_type"],
                content_encoding=initial_digest.get("content_encoding"),
            ),
            tags=tags,
        )
        try:
            final_pointer = self._blob_router(get_digest(), tags, temp=False)
            _LOG.debug("Moving data to final location %s", final_pointer)
            final_blob_client = self._service_client.get_blob_client(
                container=final_pointer["container"], blob=final_pointer["blob"]
            )
            await final_blob_client.start_copy_from_url(temp_blob_client.url, requires_sync=True)
        finally:
            _LOG.debug("Deleting temporary data %s", temp_pointer)
            await temp_blob_client.delete_blob()

        return final_pointer

    async def read_data_stream(self, pointer: BlobPointer) -> AsyncGenerator[bytes]:
        """Load a data stream from the given location."""
        _LOG.debug("Loading data stream from %s", pointer)
        blob_client = self._service_client.get_blob_client(
            container=pointer["container"], blob=pointer["blob"]
        )
        try:
            blob_reader = await blob_client.download_blob()
        except ResourceNotFoundError as exc:
            msg = f"Failed to load stream from {pointer!r}"
            raise NoStorageData(msg) from exc

        async for chunk in blob_reader.chunks():
            yield chunk


class BlobPointer(TypedDict):
    """A pointer to a blob in Azure Blob Storage."""

    container: str
    """The name of the container where the blob is stored."""
    blob: str
    """The path to the blob within the container."""


class BlobRouter(Protocol):
    """A protocol for routing data to Azure Blob Storage by returning a BlobPointer."""

    def __call__(self, digest: Digest | StreamDigest, tags: TagMap, *, temp: bool) -> BlobPointer:
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
    """Create a read-only blob router that always returns the same container and blob."""
    get_storage = ref(storage)

    def router(digest: Digest | StreamDigest, tags: TagMap, *, temp: bool) -> BlobPointer:
        msg = f"{get_storage()} is read-only and cannot write data."
        raise NotImplementedError(msg)

    return router
