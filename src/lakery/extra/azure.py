from __future__ import annotations

from typing import TYPE_CHECKING
from typing import TypeVar

from anyio import CapacityLimiter
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import ContentSettings

from lakery.common.exceptions import NoStorageData
from lakery.core.schema import DataDescriptor
from lakery.core.storage import GetStreamDigest
from lakery.core.storage import Storage
from lakery.core.storage import ValueDigest
from lakery.extra._utils import make_path_from_descriptor
from lakery.extra._utils import make_path_from_digest
from lakery.extra._utils import make_temp_path

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable

    from azure.storage.blob.aio import ContainerClient

D = TypeVar("D", bound=DataDescriptor)


class BlobStorage(Storage[D]):
    """Storage for Azure Blob data."""

    name = "lakery.azure.blob"
    version = 1

    def __init__(
        self,
        *,
        types: tuple[type[D], ...] = (DataDescriptor,),
        container_client: ContainerClient,
        path_prefix: str = "",
        max_concurrency: int | None = None,
    ):
        self.types = types
        self._container_client = container_client
        self._path_prefix = path_prefix
        self._limiter = CapacityLimiter(max_concurrency) if max_concurrency else None

    async def put_value(
        self,
        relation: D,
        value: bytes,
        digest: ValueDigest,
    ) -> D:
        """Save the given value dump."""
        path = make_path_from_digest("/", digest, prefix=self._path_prefix)
        blob_client = self._container_client.get_blob_client(blob=path)
        await blob_client.upload_blob(
            value,
            content_settings=ContentSettings(
                content_type=digest["content_type"],
                content_encoding=digest.get("content_encoding"),
            ),
        )
        return relation

    async def get_value(self, relation: D) -> bytes:
        """Load the value dump for the given relation."""
        path = make_path_from_descriptor("/", relation, prefix=self._path_prefix)
        blob_client = self._container_client.get_blob_client(blob=path)
        try:
            blob_reader = await blob_client.download_blob()
        except ResourceNotFoundError as exc:
            msg = f"Failed to load value from {path}"
            raise NoStorageData(msg) from exc
        return await blob_reader.readall()

    async def put_stream(
        self,
        relation: D,
        stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
    ) -> D:
        """Save the given stream dump."""
        initial_digest = get_digest(allow_incomplete=True)
        temp_path = make_temp_path("/", initial_digest, prefix=self._path_prefix)

        temp_blob_client = self._container_client.get_blob_client(blob=temp_path)
        await temp_blob_client.upload_blob(
            stream,
            content_settings=ContentSettings(
                content_type=initial_digest["content_type"],
                content_encoding=initial_digest.get("content_encoding"),
            ),
        )
        try:
            final_blob_client = self._container_client.get_blob_client(
                blob=make_path_from_digest("/", get_digest(), prefix=self._path_prefix)
            )
            await final_blob_client.start_copy_from_url(temp_blob_client.url, requires_sync=True)
        finally:
            await temp_blob_client.delete_blob()
        return relation

    async def get_stream(self, relation: D) -> AsyncGenerator[bytes]:
        """Load the stream dump for the given relation."""
        path = make_path_from_descriptor("/", relation, prefix=self._path_prefix)
        blob_client = self._container_client.get_blob_client(blob=path)
        try:
            blob_reader = await blob_client.download_blob()
        except ResourceNotFoundError as exc:
            msg = f"Failed to load stream from {path}"
            raise NoStorageData(msg) from exc

        async for chunk in blob_reader.chunks():
            yield chunk
