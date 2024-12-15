from collections.abc import AsyncGenerator
from collections.abc import AsyncIterable
from typing import TypeVar
from uuid import uuid4

from anyio import CapacityLimiter
from azure.storage.blob import ContentSettings
from azure.storage.blob.aio import ContainerClient

from lakery.core.schema import DataRelation
from lakery.core.storage import GetStreamDigest
from lakery.core.storage import StreamDigest
from lakery.core.storage import StreamStorage
from lakery.core.storage import ValueDigest
from lakery.extra._utils import make_path_from_digest

D = TypeVar("D", bound=DataRelation)


class AzureBlobStorage(StreamStorage[D]):
    """Storage for Azure Blob data."""

    name = "lakery.azure.blob"
    version = 1

    def __init__(
        self,
        *,
        types: tuple[type[D], ...] = (DataRelation,),
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
        digest: ValueDigest = {
            "content_encoding": relation.rel_content_encoding,
            "content_type": relation.rel_content_type,
            "content_hash": relation.rel_content_hash,
            "content_hash_algorithm": relation.rel_content_hash_algorithm,
            "content_size": relation.rel_content_size,
        }
        path = make_path_from_digest("/", digest, prefix=self._path_prefix)
        blob_client = self._container_client.get_blob_client(blob=path)
        return await (await blob_client.download_blob()).readall()

    async def put_stream(
        self,
        relation: D,
        stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
    ) -> D:
        """Save the given stream dump."""
        temp_path = _make_temp_path()
        initial_digest = get_digest(allow_incomplete=True)
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
            await final_blob_client.start_copy_from_url(temp_blob_client.url)
        finally:
            await temp_blob_client.delete_blob()
        return relation

    async def get_stream(self, relation: D) -> AsyncGenerator[bytes]:
        """Load the stream dump for the given relation."""
        digest: StreamDigest = {
            "content_encoding": relation.rel_content_encoding,
            "content_type": relation.rel_content_type,
            "content_hash": relation.rel_content_hash,
            "content_hash_algorithm": relation.rel_content_hash_algorithm,
            "content_size": relation.rel_content_size,
            "is_complete": True,
        }
        path = make_path_from_digest("/", digest, prefix=self._path_prefix)
        blob_client = self._container_client.get_blob_client(blob=path)
        async for chunk in (await blob_client.download_blob()).chunks():
            yield chunk


def _make_temp_path() -> str:
    return f"temp/{uuid4().hex}"
