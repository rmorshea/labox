from __future__ import annotations

from typing import TYPE_CHECKING

import boto3
import moto
import pytest

from lakery.extra.aws import S3Storage
from tests.test_lakery.test_extra.fake import make_fake_stream_data
from tests.test_lakery.test_extra.fake import make_fake_value_data

if TYPE_CHECKING:
    from types_boto3_s3 import S3Client


@pytest.fixture(autouse=True)
def mock_aws():
    with moto.mock_aws():
        yield


@pytest.fixture
def s3_client(mock_aws) -> S3Client:
    return boto3.client("s3")


@pytest.fixture
def s3_bucket(s3_client) -> str:
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)
    return bucket_name


async def test_put_get_value(s3_bucket):
    storage = S3Storage(s3_client=boto3.client("s3"), bucket=s3_bucket)
    relation, digest, value = make_fake_value_data(1024)
    relation = await storage.put_value(relation, value, digest)
    assert (await storage.get_value(relation)) == value


async def test_write_get_stream(s3_bucket):
    storage = S3Storage(s3_client=boto3.client("s3"), bucket=s3_bucket)
    relation, digest, stream, expected_value = make_fake_stream_data(1024 * 10, chunk_size=1024)
    relation = await storage.put_stream(relation, stream, digest)
    actual_value = b"".join([chunk async for chunk in storage.get_stream(relation)])
    assert actual_value == expected_value
