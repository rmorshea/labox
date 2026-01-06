from __future__ import annotations

from typing import TYPE_CHECKING

import boto3
import moto
import pytest

from labox.extra.aws import S3Storage
from labox.extra.aws import simple_s3_router
from tests.core_storage_utils import parametrize_storage_assertions

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


@parametrize_storage_assertions
async def test_s3_storage(assertion, s3_bucket):
    await assertion(
        S3Storage(
            s3_client=boto3.client("s3"),
            s3_router=simple_s3_router(s3_bucket),
        )
    )
