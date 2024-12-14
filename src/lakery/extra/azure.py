from collections.abc import AsyncIterable

from anyio import CapacityLimiter
from azure.storage.blob.aio import BlobServiceClient

from lakery.core.schema import DataRelation
from lakery.core.storage import GetStreamDigest
from lakery.core.storage import StreamStorage
from lakery.core.storage import ValueDigest


class AzureBlobStorage(StreamStorage[DataRelation]):
    """Storage for Azure Blob data."""

    name = "lakery.azure.blob"
    types = (DataRelation,)
    version = 1

    def __init__(
        self,
        *,
        blob_service_client: BlobServiceClient,
        container: str,
        path_prefix: str = "",
        max_concurrency: int | None = None,
    ):
        self._blob_service_client = blob_service_client
        self._container = container
        self._path_prefix = path_prefix
        self._limiter = CapacityLimiter(max_concurrency) if max_concurrency else None

    async def put_value(
        self,
        relation: DataRelation,
        value: bytes,
        digest: ValueDigest,
    ) -> DataRelation: ...

    async def get_value(self, relation: DataRelation) -> bytes: ...

    async def put_stream(
        self,
        relation: DataRelation,
        stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
    ) -> DataRelation: ...

    def get_stream(self, relation: DataRelation) -> AsyncIterable[bytes]: ...
