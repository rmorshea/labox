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

from lakery.core.api.saver import _wrap_stream_dump

if TYPE_CHECKING:
    from lakery.core.storage import Digest
    from lakery.core.storage import GetStreamDigest
    from lakery.core.storage import Storage


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
        ],
    )(test_function)


async def assert_storage_can_put_and_get_value(storage: Storage) -> None:
    value, digest = make_fake_value_data(1024)
    storage_data = await storage.write_data(value, digest, {})
    assert (await storage.read_data(storage_data)) == value


async def assert_storage_can_put_and_get_stream(storage: Storage):
    async with make_fake_stream_data(1024 * 10, chunk_size=1024) as (
        stream,
        expected_value,
        digest,
    ):
        relation = await storage.write_data_stream(stream, digest, {})
        actual_value = b"".join([chunk async for chunk in storage.read_data_stream(relation)])
        assert actual_value == expected_value


async def assert_storage_can_put_stream_and_get_value(storage: Storage):
    async with make_fake_stream_data(1024 * 10, chunk_size=1024) as (
        stream,
        expected_value,
        digest,
    ):
        relation = await storage.write_data_stream(stream, digest, {})
        actual_value = await storage.read_data(relation)
        assert actual_value == expected_value


async def assert_storage_can_put_value_and_get_stream(storage: Storage):
    value, digest = make_fake_value_data(1024)
    relation = await storage.write_data(value, digest, {})
    actual_value = b"".join([chunk async for chunk in storage.read_data_stream(relation)])
    assert actual_value == value


def make_fake_value_data(size: int) -> tuple[bytes, Digest]:
    value = os.urandom(size)
    value_hash = sha256(value)
    hash_str = value_hash.hexdigest()

    digest: Digest = {
        "content_encoding": None,
        "content_hash_algorithm": value_hash.name,
        "content_hash": hash_str,
        "content_size": size,
        "content_type": "application/octet-stream",
    }

    return value, digest


@asynccontextmanager
async def make_fake_stream_data(
    total_size: int,
    *,
    chunk_size: int,
) -> AsyncIterator[tuple[AsyncGenerator[bytes], bytes, GetStreamDigest]]:
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

    async def make_stream():
        for chunk in value_chunks:
            yield chunk

    stream, get_digest = _wrap_stream_dump(
        {
            "content_encoding": None,
            "data_stream": make_stream(),
            "content_type": "application/octet-stream",
        },
    )

    async with aclosing(stream):
        yield stream, b"".join(value_chunks), get_digest
