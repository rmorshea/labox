from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import TypedDict
from typing import cast

from google.cloud.exceptions import NotFound

from lakery.extra.google import BlobStorage
from tests.storage_utils import parametrize_storage_assertions

if TYPE_CHECKING:
    from google.cloud.storage import Blob
    from google.cloud.storage import Bucket

    from lakery.extra.google import ReaderType
    from lakery.extra.google import WriterType


@parametrize_storage_assertions
async def test_blob_storage(assertion):
    await assertion(
        BlobStorage(
            bucket_client=cast("Bucket", MockBucketClient()),
            writer_type=cast("WriterType", MockBlobWriter),
            reader_type=cast("ReaderType", MockBlobReader),
        )
    )


class MockBucketClient:
    def __init__(self) -> None:
        self._state: dict[str, MockBlobState] = {}

    def blob(
        self,
        blob_name: str,
        chunk_size: int | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> MockBlobClient:
        assert isinstance(chunk_size, int), "Chunk size is required"
        assert not args, "Extra arguments are not supported in the mock"
        assert not kwargs, "Extra arguments are not supported in the mock"
        return MockBlobClient(self._state, blob_name=blob_name, chunk_size=chunk_size)

    def copy_blob(
        self,
        blob: Blob,
        destination_bucket: Bucket,
        new_name: str,
        *args: Any,
        if_generation_match: int | None = None,
        **kwargs: Any,
    ) -> MockBlobClient:
        assert destination_bucket is self, "Destination bucket must be the same"
        assert if_generation_match == 0, "Expected race condition protection with generation match"
        assert not args, "Extra arguments are not supported in the mock"
        assert not kwargs, "Extra arguments are not supported in the mock"
        self._state[new_name] = self._state[blob.name]  # type: ignore[reportIndexIssue]
        return self.blob(new_name, chunk_size=blob.chunk_size)


class MockBlobClient:
    def __init__(self, state: dict[str, MockBlobState], *, blob_name: str, chunk_size: int) -> None:
        self.name = blob_name
        self.chunk_size = chunk_size
        self._state = state

    def delete(self) -> None:
        self._state.pop(self.name, None)


class MockBlobWriter:
    def __init__(self, blob: MockBlobClient, *, content_type: str) -> None:
        assert content_type, "Content type is required"
        self._state = blob._state  # noqa: SLF001
        self._blob_name = blob.name

    def write(self, new_data: bytes, /) -> int:
        old_data = self._state.get(self._blob_name, {"data": b""})["data"]
        self._state[self._blob_name] = {"data": old_data + new_data}
        return len(new_data)

    def close(self) -> None:
        pass


class MockBlobReader:
    def __init__(self, blob: MockBlobClient) -> None:
        self._state = blob._state  # noqa: SLF001
        self._blob_name = blob.name
        self._pos = 0

    def read(self, size: int = -1) -> bytes:
        try:
            state = self._state[self._blob_name]
        except KeyError:
            msg = "Blob not found"
            raise NotFound(msg) from None
        data = state["data"]

        if size == -1:
            result = data
            self._pos = len(data)
        else:
            next_pos = self._pos + size
            result = data[self._pos : next_pos]
            self._pos = next_pos
        return result

    def close(self) -> None:
        pass


class MockBlobState(TypedDict):
    data: bytes
