from __future__ import annotations

from typing import TYPE_CHECKING
from typing import ParamSpec
from typing import TypeVar
from uuid import uuid4

from anyio import create_task_group
from anyio.to_thread import run_sync
from sqlalchemy.util.typing import Protocol

from lakery.core.schema import DataRelation
from lakery.core.storage import GetStreamDigest
from lakery.core.storage import StreamStorage
from lakery.core.storage import ValueDigest
from lakery.utils.anyio import start_async_iterator
from lakery.utils.anyio import start_sync_iterator
from lakery.utils.misc import StorageLocationMaker
from lakery.utils.misc import make_data_relation_path
from lakery.utils.streaming import ByteStreamReader

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


class S3Storage(StreamStorage[DataRelation]):
    """Storage for S3 data."""

    name = "lakery.aws.boto3.s3"
    types = (DataRelation,)
    version = 1

    def __init__(
        self,
        client: S3Client,
        bucket: str,
        limiter: CapacityLimiter | None = None,
        make_key: StorageLocationMaker = make_data_relation_path,
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
            "Key": self._make_key(self, relation, digest),
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
                self,
                relation,
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
                Key=self._make_key(self, relation, final_digest),
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
                self,
                relation,
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
            with start_async_iterator(tg, result["Body"].iter_chunks()) as chunks:
                async for c in chunks:
                    yield c

    def _to_thread(self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Awaitable[R]:
        return run_sync(lambda: func(*args, **kwargs), limiter=self._limiter)


def _make_temp_key() -> str:
    return f"temp/{uuid4()}"
