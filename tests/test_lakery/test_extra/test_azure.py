from __future__ import annotations

import random
from collections.abc import AsyncIterable
from typing import IO
from typing import TYPE_CHECKING
from typing import Any
from typing import TypedDict
from typing import cast
from uuid import uuid4

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobType
from azure.storage.blob import ContentSettings

from lakery.extra.azure import BlobStorage
from lakery.extra.azure import simple_blob_router
from tests.core_storage_utils import parametrize_storage_assertions

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from collections.abc import Iterable
    from collections.abc import Sequence
    from datetime import datetime

    from azure.storage.blob.aio import BlobServiceClient

    from lakery.common.types import TagMap


@parametrize_storage_assertions
async def test_blob_storage(assertion):
    service_client = cast("BlobServiceClient", MockBlobServiceClient())
    await assertion(
        BlobStorage(
            service_client=service_client,
            blob_router=simple_blob_router("fake"),
        )
    )


class MockBlobServiceClient:
    def __init__(self) -> None:
        self._state: dict[str, dict[str, MockBlobState]] = {}

    def get_blob_client(
        self,
        container: str,
        blob: str,
        snapshot: str | None = None,
        *,
        version_id: str | None = None,
    ) -> MockBlobClient:
        blob_state = self._state.setdefault(container, {})
        assert snapshot is None, "Snapshots are not supported in the mock"
        assert version_id is None, "Versioning is not supported in the mock"
        return MockBlobClient(blob_state, url=blob)


class MockBlobClient:
    def __init__(self, state: dict[str, MockBlobState], *, url: str) -> None:
        self.url = url
        self._state = state

    async def upload_blob(
        self,
        data: bytes | str | Iterable[str | bytes] | AsyncIterable[str | bytes] | IO[bytes],
        blob_type: str | BlobType = BlobType.BLOCKBLOB,
        length: int | None = None,
        metadata: dict[str, str] | None = None,
        *,
        content_settings: ContentSettings | None = None,
        tags: TagMap | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        assert blob_type == BlobType.BLOCKBLOB, "Only block blobs are supported in the mock"
        assert length is None, "Length is not supported in the mock"
        assert metadata is None, "Metadata is not supported in the mock"
        assert not kwargs, "Extra arguments are not supported in the mock"
        assert tags is not None, "Expected tags to be provided"

        # check that we're providing this metadata
        assert isinstance(content_settings, ContentSettings), "Content settings are required"

        if isinstance(data, bytes):
            self._state[self.url] = {"data": _make_random_chunks(data)}
        elif isinstance(data, AsyncIterable):
            chunks: list[bytes] = []
            async for chunk in data:
                assert isinstance(chunk, bytes), "Only bytes are supported in the mock"
                chunks.append(chunk)
            self._state[self.url] = {"data": chunks}
        else:  # nocov
            msg = "Only bytes and async iterable of bytes are supported in the mock"
            raise TypeError(msg)

        return {}

    async def download_blob(self) -> MockStorageStreamDownloader:
        try:
            blob_state = self._state[self.url]
        except KeyError:  # nocov
            raise ResourceNotFoundError from None
        return MockStorageStreamDownloader(blob_state)

    async def delete_blob(self, delete_snapshots: str | None = None, **kwargs: Any) -> None:
        assert delete_snapshots is None, "Snapshots are not supported in the mock"
        assert not kwargs, "Extra arguments are not supported in the mock"
        try:
            del self._state[self.url]
        except KeyError:  # nocov
            raise ResourceNotFoundError from None

    async def start_copy_from_url(
        self,
        source_url: str,
        metadata: dict[str, str] | None = None,
        incremental_copy: bool = False,  # noqa: FBT001, FBT002
        *,
        requires_sync: bool = False,
        **kwargs: Any,
    ) -> dict[str, str | datetime]:
        assert metadata is None, "Metadata is not supported in the mock"
        assert not incremental_copy, "Incremental copy is not supported in the mock"
        assert not kwargs, "Extra arguments are not supported in the mock"
        assert requires_sync, "Non-sync copy is not supported in the mock"
        try:
            self._state[self.url] = self._state[source_url]
        except KeyError:  # nocov
            raise ResourceNotFoundError from None
        return {
            "copy_status": "success",
            "copy_id": uuid4().hex,
        }


class MockStorageStreamDownloader:
    def __init__(self, state: MockBlobState) -> None:
        self._state = state

    async def readall(self) -> bytes:
        return b"".join(self._state["data"])

    async def chunks(self) -> AsyncIterator[bytes]:
        for chunk in self._state["data"]:
            yield chunk


class MockBlobState(TypedDict):
    data: Sequence[bytes]


def _make_random_chunks(data: bytes) -> Sequence[bytes]:
    random.seed(0)  # make the test deterministic
    chunks: list[bytes] = []
    while data:
        chunk_size = random.randint(1, len(data))
        chunks.append(data[:chunk_size])
        data = data[chunk_size:]
    return chunks
