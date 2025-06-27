from collections.abc import Sequence
from io import BytesIO
from mimetypes import MimeTypes

import numpy as np
from imageio import v3 as imageio
from numpy.typing import NDArray

from lakery._internal._utils import frozenclass
from lakery.core.serializer import SerializedData
from lakery.core.serializer import Serializer

__all__ = (
    "Media",
    "MediaSerializer",
    "media_serializer",
)


@frozenclass
class Media:
    """A dataclass representing some media."""

    data: NDArray | Sequence[NDArray]
    """The media data."""
    content_type: str
    """The type of media the data represents (e.g. `"image/png"`)."""
    content_encoding: str | None = None
    """The encoding of the media data (e.g. `"base64"`)."""
    strict: bool = True
    """Whether to use [mimetypes.common_types][mimetypes.common_types] to guess the extension."""


_DEFAULT_MIMETYPES = MimeTypes()


class MediaSerializer(Serializer[Media]):
    """Serializer for ImageIO Arrays."""

    name = "lakery.imageio@v1"
    types = (Media,)

    def __init__(self, mimetypes: MimeTypes = _DEFAULT_MIMETYPES) -> None:
        self.mimetypes = mimetypes

    def serialize_data(self, media: Media) -> SerializedData:
        """Serialize the given array."""
        buffer = BytesIO()
        imageio.imwrite(
            buffer,
            media.data if isinstance(media.data, np.ndarray) else list(media.data),
            extension=self._guess_extension(media.content_type),
        )
        return {
            "data": buffer.getvalue(),
            "content_type": media.content_type,
            "content_encoding": media.content_encoding,
        }

    def deserialize_data(self, content: SerializedData) -> Media:
        """Deserialize the given array."""
        ext = self._guess_extension(content["content_type"])
        buffer = BytesIO(content["data"])
        data = imageio.imread(buffer, extension=ext)
        return Media(
            data=data,
            content_type=content["content_type"],
            content_encoding=content.get("content_encoding"),
        )

    def _guess_extension(self, content_type: str) -> str:
        """Guess the extension for the given content type."""
        ext = self.mimetypes.guess_extension(content_type)
        if ext is None:
            msg = f"{self} ould not guess extension for content type {content_type!r}."
            raise ValueError(msg)
        return ext


media_serializer = MediaSerializer()
"""MediaSerializer with default settings."""
