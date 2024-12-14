from __future__ import annotations

import os
from hashlib import sha256
from typing import TYPE_CHECKING

from lakery.core.schema import DataRelation

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from lakery.core.storage import GetStreamDigest
    from lakery.core.storage import StreamDigest
    from lakery.core.storage import ValueDigest


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


def make_fake_stream_data(
    total_size: int,
    *,
    chunk_size: int,
) -> tuple[DataRelation, GetStreamDigest, AsyncIterator[bytes], bytes]:
    is_complete = False
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
        is_complete = True

    data_relation = DataRelation()
    data_relation.rel_type = "fake"
    data_relation.rel_content_type = "application/octet-stream"
    data_relation.rel_content_hash_algorithm = current_hash.name
    data_relation.rel_serializer_name = "fake"
    data_relation.rel_serializer_version = 1

    def get_digest(*, allow_incomplete: bool = False) -> StreamDigest:
        if not allow_incomplete and not is_complete:
            msg = "The stream has not been fully read."
            raise ValueError(msg)
        return {
            "content_encoding": None,
            "content_hash_algorithm": "sha256",
            "content_hash": current_hash.hexdigest(),
            "content_size": current_size,
            "content_type": "application/octet-stream",
            "is_complete": is_complete,
        }

    async def stream():
        nonlocal current_size, is_complete
        for chunk in value_chunks:
            current_hash.update(chunk)
            current_size += chunk_size
            yield chunk
        data_relation.rel_content_size = current_size
        data_relation.rel_content_hash = current_hash.hexdigest()

    return data_relation, get_digest, stream(), b"".join(value_chunks)
