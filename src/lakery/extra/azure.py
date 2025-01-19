from __future__ import annotations

from typing import TYPE_CHECKING

from anyio import CapacityLimiter
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import ContentSettings

from lakery.common.exceptions import NoStorageData
from lakery.core.storage import Digest
from lakery.core.storage import GetStreamDigest
from lakery.core.storage import Storage
from lakery.extra._utils import make_path_from_digest
from lakery.extra._utils import make_temp_path

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable

    from azure.storage.blob.aio import ContainerClient

    from lakery.common.utils import TagMap


class BlobStorage(Storage[str]):
    """Storage for Azure Blob data."""

    name = "lakery.azure.blob"
    version = 1

    def __init__(
        self,
        *,
        container_client: ContainerClient,
        path_prefix: str = "",
        max_concurrency: int | None = None,
    ):
        self._container_client = container_client
        self._path_prefix = path_prefix
        self._limiter = CapacityLimiter(max_concurrency) if max_concurrency else None

    async def put_data(
        self,
        data: bytes,
        digest: Digest,
        tags: TagMap,
    ) -> str:
        """Save the given value data."""
        location = make_path_from_digest("/", digest, prefix=self._path_prefix)
        blob_client = self._container_client.get_blob_client(blob=location)
        await blob_client.upload_blob(
            data,
            content_settings=ContentSettings(
                content_type=digest["content_type"],
                content_encoding=digest.get("content_encoding"),
            ),
            tags=tags,
        )
        return location

    async def get_data(self, location: str) -> bytes:
        """Load data from the given location."""
        blob_client = self._container_client.get_blob_client(blob=location)
        try:
            blob_reader = await blob_client.download_blob()
        except ResourceNotFoundError as exc:
            msg = f"Failed to load value from {location!r}"
            raise NoStorageData(msg) from exc
        return await blob_reader.readall()

    async def put_data_stream(
        self,
        data_stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
        tags: TagMap,
    ) -> str:
        """Save the given data stream."""
        initial_digest = get_digest(allow_incomplete=True)
        temp_location = make_temp_path("/", initial_digest, prefix=self._path_prefix)

        temp_blob_client = self._container_client.get_blob_client(blob=temp_location)
        await temp_blob_client.upload_blob(
            data_stream,
            content_settings=ContentSettings(
                content_type=initial_digest["content_type"],
                content_encoding=initial_digest.get("content_encoding"),
            ),
            tags=tags,
        )
        try:
            final_location = make_path_from_digest("/", get_digest(), prefix=self._path_prefix)
            final_blob_client = self._container_client.get_blob_client(blob=final_location)
            await final_blob_client.start_copy_from_url(temp_blob_client.url, requires_sync=True)
        finally:
            await temp_blob_client.delete_blob()

        return final_location

    async def get_data_stream(self, location: str) -> AsyncGenerator[bytes]:
        """Load a data stream from the given location."""
        blob_client = self._container_client.get_blob_client(blob=location)
        try:
            blob_reader = await blob_client.download_blob()
        except ResourceNotFoundError as exc:
            msg = f"Failed to load stream from {location!r}"
            raise NoStorageData(msg) from exc

        async for chunk in blob_reader.chunks():
            yield chunk
