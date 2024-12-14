from __future__ import annotations

from collections.abc import Callable
from tempfile import SpooledTemporaryFile
from typing import IO
from typing import TYPE_CHECKING
from typing import ParamSpec
from typing import TypeVar
from uuid import uuid4

from anyio import create_task_group
from anyio.abc import CapacityLimiter
from anyio.to_thread import run_sync
from typing_extensions import ContextManager

from lakery.core.schema import DataRelation
from lakery.core.storage import GetStreamDigest
from lakery.core.storage import StreamDigest
from lakery.core.storage import StreamStorage
from lakery.core.storage import ValueDigest
from lakery.extra._utils import make_path_parts_from_digest
from lakery.utils.anyio import start_async_iterator
from lakery.utils.streaming import write_async_byte_stream_into

if TYPE_CHECKING:
    from collections.abc import AsyncIterable

    from anysync.core import AsyncIterator
    from anysync.core import Coroutine
    from types_boto3_s3 import S3Client
    from types_boto3_s3.type_defs import CreateMultipartUploadRequestRequestTypeDef
    from types_boto3_s3.type_defs import PutObjectRequestRequestTypeDef

P = ParamSpec("P")
R = TypeVar("R")


_5MB = 5 * (1024**2)
_5GB = 5 * (1024**3)


_StreamBufferType = Callable[[], ContextManager[IO[bytes]]]


class S3Storage(StreamStorage[DataRelation]):
    """Storage for S3 data."""

    name = "lakery.aws.boto3.s3"
    types = (DataRelation,)
    version = 1

    def __init__(
        self,
        *,
        s3_client: S3Client,
        bucket: str,
        key_prefix: str = "",
        max_concurrency: int | None = None,
        stream_write_min_size: int = _5MB,
        stream_write_buffer_type: _StreamBufferType = lambda: SpooledTemporaryFile(max_size=_5MB),  # noqa: SIM115
        stream_read_size: int = _5MB,
    ):
        if not (_5MB <= stream_write_min_size <= _5GB):
            msg = (
                "Expected multipart_min_part_size to between 5mb "
                f"and 5gb, not {stream_write_min_size} - refer to "
                "https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html"
            )
            raise ValueError(msg)
        self._s3_client = s3_client
        self._bucket = bucket
        self._key_prefix = key_prefix
        self._limiter = CapacityLimiter(max_concurrency) if max_concurrency else None
        self._stream_write_min_size = stream_write_min_size
        self._stream_write_buffer_type = stream_write_buffer_type
        self._stream_read_size = stream_read_size

    async def put_value(
        self,
        relation: DataRelation,
        value: bytes,
        digest: ValueDigest,
    ) -> DataRelation:
        """Save the given value dump."""
        put_request: PutObjectRequestRequestTypeDef = {
            "Bucket": self._bucket,
            "Key": self._make_key(digest),
            "Body": value,
            "ContentType": digest["content_type"],
        }
        if digest["content_encoding"]:
            put_request["ContentEncoding"] = digest["content_encoding"]
        await self._to_thread(self._s3_client.put_object, **put_request)
        return relation

    async def get_value(self, relation: DataRelation) -> bytes:
        """Load the value dump for the given relation."""
        result = await self._to_thread(
            self._s3_client.get_object,
            Bucket=self._bucket,
            Key=self._make_key(
                {
                    "content_type": relation.rel_content_type,
                    "content_hash_algorithm": relation.rel_content_hash_algorithm,
                    "content_hash": relation.rel_content_hash,
                    "content_size": relation.rel_content_size,
                    "content_encoding": relation.rel_content_encoding,
                },
            ),
        )
        return result["Body"].read()

    async def put_stream(
        self,
        relation: DataRelation,
        stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
    ) -> DataRelation:
        """Save the given stream dump.

        This works by first saving the stream to a temporary key becuase the content
        hash is not known until the stream is fully read. The data has been written
        to the temporary key it's copied to its final location based on the content
        hash.
        """
        temp_key = _make_temp_key()
        initial_digest = get_digest(allow_incomplete=True)

        create_multipart_upload: CreateMultipartUploadRequestRequestTypeDef = {
            "Bucket": self._bucket,
            "Key": temp_key,
            "ContentType": initial_digest["content_type"],
        }
        if initial_digest["content_encoding"]:
            create_multipart_upload["ContentEncoding"] = initial_digest["content_encoding"]
        upload_id = (
            await self._to_thread(
                self._s3_client.create_multipart_upload, **create_multipart_upload
            )
        )["UploadId"]

        with self._stream_write_buffer_type() as buffer:
            try:
                part_num = 1
                etags: list[str] = []
                while await write_async_byte_stream_into(
                    stream, buffer, min_size=self._stream_write_min_size
                ):
                    buffer.seek(0)
                    etags.append(
                        (
                            await self._to_thread(
                                self._s3_client.upload_part,
                                Bucket=self._bucket,
                                Key=temp_key,
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
                    self._s3_client.complete_multipart_upload,
                    Bucket=self._bucket,
                    Key=temp_key,
                    UploadId=upload_id,
                    MultipartUpload={
                        "Parts": [{"ETag": e, "PartNumber": i} for i, e in enumerate(etags, 1)]
                    },
                )
            except Exception:
                await self._to_thread(
                    self._s3_client.abort_multipart_upload,
                    Bucket=self._bucket,
                    Key=temp_key,
                    UploadId=upload_id,
                )
                raise

            try:
                final_digest = get_digest()
                await self._to_thread(
                    self._s3_client.copy_object,
                    Bucket=self._bucket,
                    CopySource={"Bucket": self._bucket, "Key": temp_key},
                    Key=self._make_key(final_digest),
                )
            finally:
                await self._to_thread(
                    self._s3_client.delete_object, Bucket=self._bucket, Key=temp_key
                )

        return relation

    async def get_stream(self, relation: DataRelation) -> AsyncIterator[bytes]:
        """Load the stream dump for the given relation."""
        result = await self._to_thread(
            self._s3_client.get_object,
            Bucket=self._bucket,
            Key=self._make_key(
                {
                    "content_type": relation.rel_content_type,
                    "content_hash_algorithm": relation.rel_content_hash_algorithm,
                    "content_hash": relation.rel_content_hash,
                    "content_size": relation.rel_content_size,
                    "content_encoding": relation.rel_content_encoding,
                },
            ),
        )

        async with create_task_group() as tg:
            with start_async_iterator(
                tg,
                result["Body"].iter_chunks(self._stream_read_size),
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

    def _make_key(self, digest: ValueDigest | StreamDigest) -> str:
        parts = make_path_parts_from_digest(digest)
        if self._key_prefix:
            parts = (self._key_prefix, *parts)
        return "/".join(parts)


def _make_temp_key() -> str:
    return f"temp/{uuid4()}"
