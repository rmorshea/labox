from __future__ import annotations

from collections.abc import AsyncGenerator
from collections.abc import Callable
from collections.abc import Coroutine
from contextlib import AbstractContextManager
from tempfile import SpooledTemporaryFile
from typing import IO
from typing import TYPE_CHECKING
from typing import ParamSpec
from typing import TypeVar
from urllib.parse import urlencode

from anyio import create_task_group
from anyio.abc import CapacityLimiter
from anyio.to_thread import run_sync

from lakery.common.anyio import start_async_iterator
from lakery.common.exceptions import NoStorageData
from lakery.common.streaming import write_async_byte_stream_into
from lakery.core.storage import GetStreamDigest
from lakery.core.storage import Storage
from lakery.core.storage import ValueDigest
from lakery.extra._utils import make_path_from_digest
from lakery.extra._utils import make_temp_path

if TYPE_CHECKING:
    from collections.abc import AsyncIterable

    from types_boto3_s3 import S3Client
    from types_boto3_s3.type_defs import CreateMultipartUploadRequestRequestTypeDef
    from types_boto3_s3.type_defs import PutObjectRequestRequestTypeDef

    from lakery.common.utils import TagMap

P = ParamSpec("P")
R = TypeVar("R")


_5MB = 5 * (1024**2)
_5GB = 5 * (1024**3)


StreamBufferType = Callable[[], AbstractContextManager[IO[bytes]]]
"""A function that returns a context manager for a stream buffer."""


class S3Storage(Storage[str]):
    """Storage for S3 data."""

    name = "lakery.aws.s3"
    version = 1

    def __init__(
        self,
        *,
        s3_client: S3Client,
        bucket_name: str,
        object_key_prefix: str = "",
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
        self._bucket_name = bucket_name
        self._object_key_prefix = object_key_prefix
        self._limiter = CapacityLimiter(max_concurrency) if max_concurrency else None
        self._stream_writer_min_part_size = stream_writer_min_part_size
        self._stream_writer_buffer_type = stream_writer_buffer_type
        self._stream_reader_part_size = stream_reader_part_size

    async def put_value(
        self,
        value: bytes,
        digest: ValueDigest,
        tags: TagMap,
    ) -> str:
        """Save the given value."""
        location = make_path_from_digest("/", digest, prefix=self._object_key_prefix)
        put_request: PutObjectRequestRequestTypeDef = {
            "Bucket": self._bucket_name,
            "Key": location,
            "Body": value,
            "ContentType": digest["content_type"],
            "Tagging": urlencode(tags),
        }
        if digest["content_encoding"]:
            put_request["ContentEncoding"] = digest["content_encoding"]
        await self._to_thread(self._client.put_object, **put_request)
        return location

    async def get_value(self, location: str) -> bytes:
        """Load the value from the given location."""
        try:
            result = await self._to_thread(
                self._client.get_object,
                Bucket=self._bucket_name,
                Key=location,
            )
            return result["Body"].read()
        except self._client.exceptions.NoSuchKey as error:
            msg = f"No data found for {location!r}."
            raise NoStorageData(msg) from error

    async def put_stream(
        self,
        stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
        tags: TagMap,
    ) -> str:
        """Save the given stream dump.

        This works by first saving the stream to a temporary key becuase the content
        hash is not known until the stream is fully read. The data has been written
        to the temporary key it's copied to its final location based on the content
        hash.
        """
        initial_digest = get_digest(allow_incomplete=True)
        temp_location = make_temp_path("/", initial_digest, prefix=self._object_key_prefix)
        tagging = urlencode(tags)

        create_multipart_upload: CreateMultipartUploadRequestRequestTypeDef = {
            "Bucket": self._bucket_name,
            "Key": temp_location,
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
                    stream, buffer, min_size=self._stream_writer_min_part_size
                ):
                    buffer.seek(0)
                    etags.append(
                        (
                            await self._to_thread(
                                self._client.upload_part,
                                Bucket=self._bucket_name,
                                Key=temp_location,
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
                    Bucket=self._bucket_name,
                    Key=temp_location,
                    UploadId=upload_id,
                    MultipartUpload={
                        "Parts": [{"ETag": e, "PartNumber": i} for i, e in enumerate(etags, 1)]
                    },
                )
            except Exception:
                await self._to_thread(
                    self._client.abort_multipart_upload,
                    Bucket=self._bucket_name,
                    Key=temp_location,
                    UploadId=upload_id,
                )
                raise

            try:
                final_location = make_path_from_digest(
                    "/",
                    get_digest(),
                    prefix=self._object_key_prefix,
                )
                await self._to_thread(
                    self._client.copy_object,
                    Bucket=self._bucket_name,
                    CopySource={"Bucket": self._bucket_name, "Key": temp_location},
                    Key=final_location,
                    Tagging=tagging,
                )
            finally:
                await self._to_thread(
                    self._client.delete_object,
                    Bucket=self._bucket_name,
                    Key=temp_location,
                )

        return final_location

    async def get_stream(self, location: str) -> AsyncGenerator[bytes]:
        """Load the stream from the given location."""
        try:
            result = await self._to_thread(
                self._client.get_object,
                Bucket=self._bucket_name,
                Key=location,
            )
        except self._client.exceptions.NoSuchKey as error:
            msg = f"No data found for {location!r}."
            raise NoStorageData(msg) from error

        async with create_task_group() as tg:
            with start_async_iterator(
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
