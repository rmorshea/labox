from __future__ import annotations

from collections.abc import AsyncGenerator
from collections.abc import Callable
from collections.abc import Coroutine
from contextlib import AbstractContextManager
from tempfile import SpooledTemporaryFile
from typing import IO
from typing import TYPE_CHECKING
from typing import ParamSpec
from typing import Protocol
from typing import TypedDict
from typing import TypeVar
from urllib.parse import urlencode
from weakref import ref

from anyio import create_task_group
from anyio.abc import CapacityLimiter
from anyio.to_thread import run_sync

from labox._internal._temp_path import make_path_from_digest
from labox._internal._temp_path import make_temp_path
from labox.common.anyio import as_async_iterator
from labox.common.exceptions import NoStorageData
from labox.common.streaming import write_async_byte_stream_into
from labox.core.storage import Digest
from labox.core.storage import GetStreamDigest
from labox.core.storage import Storage
from labox.core.storage import StreamDigest

if TYPE_CHECKING:
    from collections.abc import AsyncIterable

    from types_boto3_s3 import S3Client
    from types_boto3_s3.type_defs import CreateMultipartUploadRequestRequestTypeDef
    from types_boto3_s3.type_defs import PutObjectRequestRequestTypeDef

    from labox.common.types import TagMap

__all__ = ("S3Storage",)

P = ParamSpec("P")
R = TypeVar("R")


_5MB = 5 * (1024**2)
_5GB = 5 * (1024**3)


StreamBufferType = Callable[[], AbstractContextManager[IO[bytes]]]
"""A function that returns a context manager for a stream buffer."""


def simple_s3_router(
    bucket: str,
    prefix: str = "",
) -> S3Router:
    """Create a simple S3 router that routes digests to S3 pointers.

    Object paths are of the form:

        <prefix>/<content_hash>.<extension>

    Args:
        bucket: The S3 bucket to use for routing.
        prefix: An optional prefix to add to the S3 object key.
        temp: If True, the router will create temporary paths for the data.
    """

    def router(digest: Digest, tags: TagMap, *, temp: bool) -> S3Pointer:  # noqa: ARG001
        """Route a digest and name to an S3 pointer."""
        if temp:
            key = make_temp_path("/", digest, prefix=prefix)
        else:
            key = make_path_from_digest("/", digest, prefix=prefix)
        return S3Pointer(bucket=bucket, key=key)

    return router


class S3Storage(Storage["S3Pointer"]):
    """Storage for S3 data."""

    name = "labox.aws.s3@v1"

    def __init__(
        self,
        *,
        s3_client: S3Client,
        s3_router: S3Router | None,
        max_concurrency: int | None = None,
        stream_writer_min_part_size: int = _5MB,
        stream_writer_buffer_type: StreamBufferType = lambda: SpooledTemporaryFile(max_size=_5MB),  # noqa: SIM115
        stream_reader_part_size: int = _5MB,
    ):
        if not (_5MB <= stream_writer_min_part_size <= _5GB):
            msg = (
                "Expected multipart_min_part_size to between 5mb "
                f"and 5gb, not {stream_writer_min_part_size} - refer to "
                "https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html"
            )
            raise ValueError(msg)
        self._client = s3_client
        self._router = s3_router or _read_only(self)
        self._limiter = CapacityLimiter(max_concurrency) if max_concurrency else None
        self._stream_writer_min_part_size = stream_writer_min_part_size
        self._stream_writer_buffer_type = stream_writer_buffer_type
        self._stream_reader_part_size = stream_reader_part_size

    async def write_data(
        self,
        data: bytes,
        digest: Digest,
        tags: TagMap,
    ) -> S3Pointer:
        """Save the given value."""
        pointer = self._router(digest, tags, temp=False)
        put_request: PutObjectRequestRequestTypeDef = {
            "Bucket": pointer["bucket"],
            "Key": pointer["key"],
            "Body": data,
            "ContentType": digest["content_type"],
            "Tagging": urlencode(tags),
        }
        if digest["content_encoding"]:
            put_request["ContentEncoding"] = digest["content_encoding"]
        await self._to_thread(self._client.put_object, **put_request)
        # include the bucket name in the location so this storage can
        # read from any bucket that the client has access to
        return pointer

    async def read_data(self, pointer: S3Pointer) -> bytes:
        """Load the value from the given location."""
        try:
            result = await self._to_thread(
                self._client.get_object,
                Bucket=pointer["bucket"],
                Key=pointer["key"],
            )
            return result["Body"].read()
        except self._client.exceptions.NoSuchKey as error:
            msg = f"No data found for {pointer}."
            raise NoStorageData(msg) from error

    async def write_data_stream(
        self,
        data_stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
        tags: TagMap,
    ) -> S3Pointer:
        """Save the given data stream.

        This works by first saving the stream to a temporary key becuase the content
        hash is not known until the stream is fully read. The data has been written
        to the temporary key it's copied to its final location based on the content
        hash.
        """
        if not self._router:
            msg = f"{self} is read-only and cannot write data."
            raise NotImplementedError(msg)

        initial_digest = get_digest(allow_incomplete=True)
        temp_pointer = self._router(initial_digest, tags, temp=True)
        tagging = urlencode(tags)

        create_multipart_upload: CreateMultipartUploadRequestRequestTypeDef = {
            "Bucket": temp_pointer["bucket"],
            "Key": temp_pointer["key"],
            "ContentType": initial_digest["content_type"],
            "Tagging": tagging,
        }
        if initial_digest["content_encoding"]:
            create_multipart_upload["ContentEncoding"] = initial_digest["content_encoding"]
        upload_id = (
            await self._to_thread(self._client.create_multipart_upload, **create_multipart_upload)
        )["UploadId"]

        with self._stream_writer_buffer_type() as buffer:
            try:
                part_num = 1
                etags: list[str] = []
                while await write_async_byte_stream_into(
                    data_stream, buffer, min_size=self._stream_writer_min_part_size
                ):
                    buffer.seek(0)
                    etags.append(
                        (
                            await self._to_thread(
                                self._client.upload_part,
                                Bucket=temp_pointer["bucket"],
                                Key=temp_pointer["key"],
                                Body=buffer,
                                PartNumber=part_num,
                                UploadId=upload_id,
                            )
                        )["ETag"]
                    )
                    buffer.seek(0)
                    buffer.truncate()
                    part_num += 1
                await self._to_thread(
                    self._client.complete_multipart_upload,
                    Bucket=temp_pointer["bucket"],
                    Key=temp_pointer["key"],
                    UploadId=upload_id,
                    MultipartUpload={
                        "Parts": [{"ETag": e, "PartNumber": i} for i, e in enumerate(etags, 1)]
                    },
                )
            except Exception:
                await self._to_thread(
                    self._client.abort_multipart_upload,
                    Bucket=temp_pointer["bucket"],
                    Key=temp_pointer["key"],
                    UploadId=upload_id,
                )
                raise

            try:
                final_pointer = self._router(get_digest(), tags, temp=False)
                await self._to_thread(
                    self._client.copy_object,
                    Bucket=final_pointer["bucket"],
                    CopySource={"Bucket": temp_pointer["bucket"], "Key": temp_pointer["key"]},
                    Key=final_pointer["key"],
                    Tagging=tagging,
                )
            finally:
                await self._to_thread(
                    self._client.delete_object,
                    Bucket=temp_pointer["bucket"],
                    Key=temp_pointer["key"],
                )

        return final_pointer

    async def read_data_stream(self, pointer: S3Pointer) -> AsyncGenerator[bytes]:
        """Load the stream from the given location."""
        try:
            result = await self._to_thread(
                self._client.get_object,
                Bucket=pointer["bucket"],
                Key=pointer["key"],
            )
        except self._client.exceptions.NoSuchKey as error:
            msg = f"No data found for {pointer}."
            raise NoStorageData(msg) from error

        async with create_task_group() as tg:
            with as_async_iterator(
                tg,
                result["Body"].iter_chunks(self._stream_reader_part_size),
            ) as chunks:
                async for c in chunks:
                    yield c

    def _to_thread(
        self,
        func: Callable[P, R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Coroutine[None, None, R]:
        return run_sync(lambda: func(*args, **kwargs), limiter=self._limiter)


class S3Pointer(TypedDict):
    """A pointer to a location in S3."""

    bucket: str
    """The S3 bucket where the data is stored."""
    key: str
    """The S3 object key where the data is stored."""


class S3Router(Protocol):
    """A protocol for routing data to S3 buckets by returning an S3Pointer."""

    def __call__(self, digest: Digest | StreamDigest, tags: TagMap, *, temp: bool) -> S3Pointer:
        """Return an S3 pointer for the given digest and name.

        Args:
            digest: The digest of the data to route.
            tags: Tags from the user or unpacker that describe the data.
            temp: Whether to create a temporary path for the data - used for streaming data.

        Returns:
            An S3Pointer that can be used to access the data.
        """
        ...


def _read_only(storage: S3Storage) -> S3Router:
    get_storage = ref(storage)

    def router(digest: Digest, tags: TagMap, *, temp: bool) -> S3Pointer:
        msg = f"{get_storage()} is read-only and cannot write data."
        raise NotImplementedError(msg)

    return router
