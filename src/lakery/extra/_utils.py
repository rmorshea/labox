from __future__ import annotations

from mimetypes import guess_extension
from typing import TYPE_CHECKING
from uuid import uuid4

from lakery.common.utils import slugify

if TYPE_CHECKING:
    from collections.abc import Sequence

    from lakery.core.schema import DataRelation
    from lakery.core.storage import StreamDigest
    from lakery.core.storage import ValueDigest


def make_temp_path(sep: str, digest: StreamDigest | ValueDigest, *, prefix: str = "") -> str:
    ext = guess_extension(digest["content_type"])
    return _join_with_prefix(sep, ("temp", f"{uuid4().hex}{ext}"), prefix)


def make_path_from_digest(sep: str, digest: ValueDigest | StreamDigest, *, prefix: str = "") -> str:
    parts = make_path_parts_from_digest(digest)
    return _join_with_prefix(sep, parts, prefix)


def make_path_from_data_relation(sep: str, relation: DataRelation, *, prefix: str = "") -> str:
    return make_path_from_digest(sep, _make_digest_from_data_relation(relation), prefix=prefix)


def make_path_parts_from_digest(digest: ValueDigest | StreamDigest) -> Sequence[str]:
    if ext := guess_extension(digest["content_type"]):
        name = f"{slugify(digest['content_hash'])}{ext}"
    else:
        name = slugify(digest["content_hash"])
    return (slugify(digest["content_hash_algorithm"]), name)


def _make_digest_from_data_relation(relation: DataRelation) -> ValueDigest:
    return {
        "content_encoding": relation.rel_content_encoding,
        "content_hash": relation.rel_content_hash,
        "content_hash_algorithm": relation.rel_content_hash_algorithm,
        "content_size": relation.rel_content_size,
        "content_type": relation.rel_content_type,
    }


def _join_with_prefix(sep, parts: Sequence[str], prefix: str) -> str:
    if prefix:
        parts = (prefix, *parts)
    return sep.join(parts)
