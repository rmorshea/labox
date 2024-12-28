from __future__ import annotations

from collections.abc import AsyncGenerator
from collections.abc import Callable
from collections.abc import Coroutine
from tempfile import SpooledTemporaryFile
from typing import IO
from typing import TYPE_CHECKING
from typing import ParamSpec
from typing import TypeVar

from anyio import create_task_group
from anyio.abc import CapacityLimiter
from anyio.to_thread import run_sync
from typing_extensions import ContextManager

from lakery.core.schema import DataRelation
from lakery.core.storage import GetStreamDigest
from lakery.core.storage import StreamStorage
from lakery.core.storage import ValueDigest
from lakery.extra._utils import make_path_from_data_relation
from lakery.extra._utils import make_path_from_digest
from lakery.extra._utils import make_temp_path
from lakery.utils.anyio import start_async_iterator
from lakery.utils.errors import NoStorageDataError
from lakery.utils.streaming import write_async_byte_stream_into

if TYPE_CHECKING:
    from collections.abc import AsyncIterable

    from types_boto3_s3 import S3Client
    from types_boto3_s3.type_defs import CreateMultipartUploadRequestRequestTypeDef
    from types_boto3_s3.type_defs import PutObjectRequestRequestTypeDef

P = ParamSpec("P")
R = TypeVar("R")
D = TypeVar("D", bound=DataRelation)


_5MB = 5 * (1024**2)
_5GB = 5 * (1024**3)


StreamBufferType = Callable[[], ContextManager[IO[bytes]]]
"""A function that returns a context manager for a stream buffer."""


class S3Storage(StreamStorage[D]):
    """Storage for S3 data."""

    name = "lakery.aws.s3"
    version = 1

    def __init__(
        self,
        *,
        types: tuple[type[D], ...] = (DataRelation,),
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
        self.types = types
        self._client = s3_client
        self._bucket_name = bucket_name
        self._object_key_prefix = object_key_prefix
        self._limiter = CapacityLimiter(max_concurrency) if max_concurrency else None
        self._stream_writer_min_part_size = stream_writer_min_part_size
        self._stream_writer_buffer_type = stream_writer_buffer_type
        self._stream_reader_part_size = stream_reader_part_size

    async def put_value(
        self,
        relation: D,
        value: bytes,
        digest: ValueDigest,
    ) -> D:
        """Save the given value dump."""
        put_request: PutObjectRequestRequestTypeDef = {
            "Bucket": self._bucket_name,
            "Key": make_path_from_digest("/", digest, prefix=self._object_key_prefix),
            "Body": value,
            "ContentType": digest["content_type"],
        }
        if digest["content_encoding"]:
            put_request["ContentEncoding"] = digest["content_encoding"]
        await self._to_thread(self._client.put_object, **put_request)
        return relation

    async def get_value(self, relation: D) -> bytes:
        """Load the value dump for the given relation."""
        try:
            result = await self._to_thread(
                self._client.get_object,
                Bucket=self._bucket_name,
                Key=make_path_from_data_relation("/", relation, prefix=self._object_key_prefix),
            )
            return result["Body"].read()
        except self._client.exceptions.NoSuchKey as error:
            msg = f"No data found for {relation}."
            raise NoStorageDataError(msg) from error

    async def put_stream(
        self,
        relation: D,
        stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
    ) -> D:
        """Save the given stream dump.

        This works by first saving the stream to a temporary key becuase the content
        hash is not known until the stream is fully read. The data has been written
        to the temporary key it's copied to its final location based on the content
        hash.
        """
        initial_digest = get_digest(allow_incomplete=True)
        temp_key = make_temp_path("/", initial_digest, prefix=self._object_key_prefix)

        create_multipart_upload: CreateMultipartUploadRequestRequestTypeDef = {
            "Bucket": self._bucket_name,
            "Key": temp_key,
            "ContentType": initial_digest["content_type"],
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
                    self._client.complete_multipart_upload,
                    Bucket=self._bucket_name,
                    Key=temp_key,
                    UploadId=upload_id,
                    MultipartUpload={
                        "Parts": [{"ETag": e, "PartNumber": i} for i, e in enumerate(etags, 1)]
                    },
                )
            except Exception:
                await self._to_thread(
                    self._client.abort_multipart_upload,
                    Bucket=self._bucket_name,
                    Key=temp_key,
                    UploadId=upload_id,
                )
                raise

            try:
                final_digest = get_digest()
                await self._to_thread(
                    self._client.copy_object,
                    Bucket=self._bucket_name,
                    CopySource={"Bucket": self._bucket_name, "Key": temp_key},
                    Key=make_path_from_digest("/", (final_digest), prefix=self._object_key_prefix),
                )
            finally:
                await self._to_thread(
                    self._client.delete_object, Bucket=self._bucket_name, Key=temp_key
                )

        return relation

    async def get_stream(self, relation: D) -> AsyncGenerator[bytes]:
        """Load the stream dump for the given relation."""
        try:
            result = await self._to_thread(
                self._client.get_object,
                Bucket=self._bucket_name,
                Key=make_path_from_digest(
                    "/",
                    {
                        "content_type": relation.rel_content_type,
                        "content_hash_algorithm": relation.rel_content_hash_algorithm,
                        "content_hash": relation.rel_content_hash,
                        "content_size": relation.rel_content_size,
                        "content_encoding": relation.rel_content_encoding,
                    },
                    prefix=self._object_key_prefix,
                ),
            )
        except self._client.exceptions.NoSuchKey as error:
            msg = f"No data found for {relation}."
            raise NoStorageDataError(msg) from error

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
