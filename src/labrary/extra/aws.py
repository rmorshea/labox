from __future__ import annotations

from mimetypes import guess_extension
from typing import TYPE_CHECKING
from typing import ParamSpec
from typing import TypeVar
from uuid import uuid4

from anyio import create_task_group
from anyio.to_thread import run_sync
from sqlalchemy.util.typing import Protocol

from labrary.core.schema import DataRelation
from labrary.core.storage import GetStreamDigest
from labrary.core.storage import Storage
from labrary.core.storage import ValueDigest
from labrary.utils.anyio import start_async_iterator
from labrary.utils.anyio import start_sync_iterator
from labrary.utils.misc import slugify
from labrary.utils.streaming import ByteStreamReader

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import Awaitable

    from anyio.abc import CapacityLimiter
    from anysync.core import AsyncIterator
    from paramorator import Callable
    from types_boto3_s3 import S3Client
    from types_boto3_s3.type_defs import PutObjectRequestRequestTypeDef

P = ParamSpec("P")
R = TypeVar("R")


def make_data_relation_key(
    relation: DataRelation,
    content_type: str,
    content_hash_algorithm: str,
    content_hash: str,
    storage_version: int,
) -> str:
    """Make a path for the given data relation."""
    return "/".join(
        (
            f"v{storage_version}",
            slugify(relation.rel_type),
            slugify(content_hash_algorithm),
            f"{slugify(content_hash)}.{guess_extension(content_type)}",
        )
    )


class KeyMaker(Protocol):
    """A protocol for making paths for data relations."""

    def __call__(  # noqa: D102
        self,
        relation: DataRelation,
        content_type: str,
        content_hash_algorithm: str,
        content_hash: str,
        storage_version: int,
    ) -> str: ...


class S3Storage(Storage[DataRelation]):
    """Storage for S3 data."""

    name = "labrary.aws.boto3.s3"
    types = (DataRelation,)
    version = 1

    def __init__(
        self,
        client: S3Client,
        bucket: str,
        limiter: CapacityLimiter | None = None,
        make_key: KeyMaker = make_data_relation_key,
    ):
        self._client = client
        self._bucket = bucket
        self._limiter = limiter
        self._make_key = make_key

    async def write_value(
        self,
        relation: DataRelation,
        value: bytes,
        digest: ValueDigest,
    ) -> DataRelation:
        """Save the given value dump."""
        put_request: PutObjectRequestRequestTypeDef = {
            "Bucket": self._bucket,
            "Key": self._make_key(
                relation,
                digest["content_type"],
                digest["content_hash_algorithm"],
                digest["content_hash"],
                self.version,
            ),
            "Body": value,
            "ContentType": digest["content_type"],
        }
        if digest["content_encoding"]:
            put_request["ContentEncoding"] = digest["content_encoding"]
        await self._to_thread(self._client.put_object, **put_request)
        return relation

    async def read_value(self, relation: DataRelation) -> bytes:
        """Load the value dump for the given relation."""
        result = await self._to_thread(
            self._client.get_object,
            Bucket=self._bucket,
            Key=self._make_key(
                relation,
                relation.rel_content_type,
                relation.rel_content_hash_algorithm,
                relation.rel_content_hash,
                relation.rel_storage_version,
            ),
        )
        return result["Body"].read()

    async def write_stream(
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
        async with create_task_group() as tg:
            body = ByteStreamReader(start_sync_iterator(tg, stream))
            put_request: PutObjectRequestRequestTypeDef = {
                "Bucket": self._bucket,
                "Key": temp_key,
                "Body": body,
                "ContentType": initial_digest["content_type"],
            }
            if initial_digest["content_encoding"]:
                put_request["ContentEncoding"] = initial_digest["content_encoding"]
            await self._to_thread(self._client.put_object, **put_request)

        try:
            final_digest = get_digest()
            self._to_thread(
                self._client.copy_object,
                Bucket=self._bucket,
                CopySource={"Bucket": self._bucket, "Key": temp_key},
                Key=self._make_key(
                    relation,
                    final_digest["content_type"],
                    final_digest["content_hash_algorithm"],
                    final_digest["content_hash"],
                    self.version,
                ),
            )
        finally:
            self._to_thread(self._client.delete_object, Bucket=self._bucket, Key=temp_key)

        return relation

    async def read_stream(self, relation: DataRelation) -> AsyncIterator[bytes]:
        """Load the stream dump for the given relation."""
        result = await self._to_thread(
            self._client.get_object,
            Bucket=self._bucket,
            Key=self._make_key(
                relation,
                relation.rel_content_type,
                relation.rel_content_hash_algorithm,
                relation.rel_content_hash,
                relation.rel_storage_version,
            ),
        )
        async with create_task_group() as tg:
            iter_chunks = start_async_iterator(tg, result["Body"].iter_chunks())
            try:
                async for chunk in iter_chunks:
                    yield chunk
            finally:
                iter_chunks.close()

    def _to_thread(self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Awaitable[R]:
        return run_sync(lambda: func(*args, **kwargs), limiter=self._limiter)


def _make_temp_key() -> str:
    return f"temp/{uuid4()}"
