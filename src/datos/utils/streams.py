from __future__ import annotations

from hashlib import _Hash as HashTypeAlias
from hashlib import new as new_hash
from typing import TYPE_CHECKING
from typing import TypedDict

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import AsyncIterator


def byte_stream_info(
    algorithm_name: str,
    stream: AsyncIterable[bytes],
) -> tuple[AsyncIterator[bytes], ByteStreamInfo]:
    """Wrap a byte stream to hash its content and keep track of its total size."""
    content_hash = new_hash(algorithm_name)
    info: ByteStreamInfo = {"hash": content_hash, "size": 0}

    async def hash_stream() -> AsyncIterator[bytes]:
        async for chunk in stream:
            content_hash.update(chunk)
            info["size"] += len(chunk)
            yield chunk

    return hash_stream(), info


class ByteStreamInfo(TypedDict):
    """Information about a byte stream."""

    hash: HashTypeAlias
    size: int
