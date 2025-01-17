from __future__ import annotations

from mimetypes import guess_extension
from typing import TYPE_CHECKING
from uuid import uuid4

from lakery.common.utils import slugify

if TYPE_CHECKING:
    from collections.abc import Sequence

    from lakery.core.storage import StreamDigest
    from lakery.core.storage import ValueDigest


def make_temp_path(sep: str, digest: StreamDigest | ValueDigest, *, prefix: str = "") -> str:
    ext = guess_extension(digest["content_type"])
    return _join_with_prefix(sep, ("temp", f"{uuid4().hex}{ext}"), prefix)


def make_path_from_digest(sep: str, digest: ValueDigest | StreamDigest, *, prefix: str = "") -> str:
    parts = make_path_parts_from_digest(digest)
    return _join_with_prefix(sep, parts, prefix)


def make_path_parts_from_digest(digest: ValueDigest | StreamDigest) -> Sequence[str]:
    if ext := guess_extension(digest["content_type"]):
        name = f"{slugify(digest['content_hash'])}{ext}"
    else:
        name = slugify(digest["content_hash"])
    return (slugify(digest["content_hash_algorithm"]), name)


def _join_with_prefix(sep, parts: Sequence[str], prefix: str) -> str:
    return sep.join((prefix, *parts) if prefix else parts)
