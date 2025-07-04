from __future__ import annotations

from mimetypes import guess_extension
from typing import TYPE_CHECKING
from uuid import uuid4

from lakery._internal._utils import slugify

if TYPE_CHECKING:
    from lakery.core.storage import Digest
    from lakery.core.storage import StreamDigest


def make_temp_path(sep: str, digest: StreamDigest | Digest, *, prefix: str = "") -> str:
    if ext := guess_extension(digest["content_type"]):
        name = f"{uuid4().hex}{ext}"
    else:
        name = uuid4().hex

    return _join_with_prefix(sep, prefix, "temp", name)


def make_path_from_digest(sep: str, digest: Digest | StreamDigest, *, prefix: str = "") -> str:
    return _join_with_prefix(sep, prefix, make_file_name_from_digest(digest))


def make_file_name_from_digest(digest: Digest | StreamDigest) -> str:
    if ext := guess_extension(digest["content_type"]):
        return f"{slugify(digest['content_hash'])}{ext}"
    else:
        return slugify(digest["content_hash"])


def _join_with_prefix(sep: str, prefix: str, *body: str) -> str:
    joined_body = sep.join(s.lstrip(sep) for s in body)
    if prefix:
        return f"{prefix.rstrip(sep)}{sep}{joined_body}"
    else:
        return joined_body
