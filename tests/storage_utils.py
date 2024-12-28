from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from collections.abc import Callable
from contextlib import aclosing
from contextlib import asynccontextmanager
from hashlib import sha256
from typing import TYPE_CHECKING
from typing import TypeVar

import pytest

from lakery.common.exceptions import NoStorageData
from lakery.core.api.saver import _wrap_stream_dump
from lakery.core.schema import DataRelation

if TYPE_CHECKING:
    from lakery.core.storage import GetStreamDigest
    from lakery.core.storage import StreamStorage
    from lakery.core.storage import ValueDigest
    from lakery.core.storage import ValueStorage


F = TypeVar("F", bound=Callable)


def parametrize_storage_assertions(test_function: F) -> F:
    """Decorator that parametrizes a test function with storage assertions.

    The decorated test function is responsible for constructing the storage
    and calling the given assertion function with the storage instance.
    """
    return pytest.mark.parametrize(
        ("assertion"),
        [
            assert_storage_can_put_and_get_value,
            assert_storage_can_put_and_get_stream,
            assert_storage_can_put_stream_and_get_value,
            assert_storage_can_put_value_and_get_stream,
            assert_storage_cleans_up_after_stream_error,
        ],
    )(test_function)


async def assert_storage_can_put_and_get_value(storage: ValueStorage) -> None:
    relation, digest, value = make_fake_value_data(1024)
    relation = await storage.put_value(relation, value, digest)
    assert (await storage.get_value(relation)) == value


async def assert_storage_can_put_and_get_stream(storage: StreamStorage):
    async with make_fake_stream_data(1024 * 10, chunk_size=1024) as (
        relation,
        digest,
        stream,
        expected_value,
    ):
        relation = await storage.put_stream(relation, stream, digest)
        actual_value = b"".join([chunk async for chunk in storage.get_stream(relation)])
        assert actual_value == expected_value


async def assert_storage_can_put_stream_and_get_value(storage: StreamStorage):
    async with make_fake_stream_data(1024 * 10, chunk_size=1024) as (
        relation,
        digest,
        stream,
        expected_value,
    ):
        relation = await storage.put_stream(relation, stream, digest)
        actual_value = await storage.get_value(relation)
        assert actual_value == expected_value


async def assert_storage_can_put_value_and_get_stream(storage: StreamStorage):
    relation, digest, value = make_fake_value_data(1024)
    relation = await storage.put_value(relation, value, digest)
    actual_value = b"".join([chunk async for chunk in storage.get_stream(relation)])
    assert actual_value == value


async def assert_storage_cleans_up_after_stream_error(storage: StreamStorage):
    async with make_fake_stream_data(1024 * 10, chunk_size=1024) as (
        relation,
        digest,
        stream,
        _,
    ):

        async def make_bad_stream():
            async for chunk in stream:
                yield chunk
                msg = "Bad stream"
                raise ValueError(msg)

        with pytest.raises(ValueError, match="Bad stream"):
            await storage.put_stream(relation, make_bad_stream(), digest)

    try:
        with pytest.raises(NoStorageData):
            await storage.get_value(relation)
    except Exception as error:
        msg = "Expected a NoStorageDataError error - storage may not have cleaned up properly"
        raise AssertionError(msg) from error

    load_stream = storage.get_stream(relation)

    try:
        iter_load_stream = aiter(load_stream)
        with pytest.raises(NoStorageData):
            await anext(iter_load_stream)
    except Exception as error:
        msg = "Expected a NoStorageDataError error - storage may not have cleaned up properly"
        raise AssertionError(msg) from error


def make_fake_value_data(size: int) -> tuple[DataRelation, ValueDigest, bytes]:
    value = os.urandom(size)
    value_hash = sha256(value)
    hash_str = value_hash.hexdigest()

    data_relation = DataRelation()
    data_relation.rel_type = "fake"
    data_relation.rel_content_type = "application/octet-stream"
    data_relation.rel_content_size = size
    data_relation.rel_content_hash = hash_str
    data_relation.rel_content_hash_algorithm = "sha256"
    data_relation.rel_serializer_name = "fake"
    data_relation.rel_serializer_version = 1

    digest: ValueDigest = {
        "content_encoding": None,
        "content_hash_algorithm": value_hash.name,
        "content_hash": hash_str,
        "content_size": size,
        "content_type": "application/octet-stream",
    }
    return data_relation, digest, value


@asynccontextmanager
async def make_fake_stream_data(
    total_size: int,
    *,
    chunk_size: int,
) -> AsyncIterator[tuple[DataRelation, GetStreamDigest, AsyncGenerator[bytes], bytes]]:
    current_size = 0
    current_hash = sha256()

    value_chunks: list[bytes] = []
    for _ in range(total_size // chunk_size):
        chunk = os.urandom(chunk_size)
        current_hash.update(chunk)
        current_size += chunk_size
        value_chunks.append(chunk)
    else:
        chunk = os.urandom(total_size % chunk_size)
        current_hash.update(chunk)
        current_size += len(chunk)
        value_chunks.append(chunk)

    data_relation = DataRelation()
    data_relation.rel_type = "fake"
    data_relation.rel_content_type = "application/octet-stream"
    data_relation.rel_content_hash_algorithm = current_hash.name
    data_relation.rel_serializer_name = "fake"
    data_relation.rel_serializer_version = 1

    async def make_stream():
        for chunk in value_chunks:
            yield chunk

    stream, get_digest = _wrap_stream_dump(
        data_relation,
        {
            "content_encoding": None,
            "content_type": "application/octet-stream",
            "serializer_name": "fake",
            "serializer_version": 1,
            "stream": make_stream(),
        },
    )

    async with aclosing(stream):
        yield data_relation, get_digest, stream, b"".join(value_chunks)
