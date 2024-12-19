from collections.abc import Sequence
from mimetypes import guess_extension

from lakery.core.storage import StreamDigest
from lakery.core.storage import ValueDigest
from lakery.utils.misc import slugify


def make_path_from_digest(sep: str, digest: ValueDigest | StreamDigest, *, prefix: str = "") -> str:
    parts = make_path_parts_from_digest(digest)
    if prefix:
        parts = (prefix, *parts)
    return sep.join(parts)


def make_path_parts_from_digest(digest: ValueDigest | StreamDigest) -> Sequence[str]:
    if ext := guess_extension(digest["content_type"]):
        name = f"{slugify(digest["content_hash"])}{ext}"
    else:
        name = slugify(digest["content_hash"])
    return (slugify(digest["content_hash_algorithm"]), name)
