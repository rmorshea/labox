from __future__ import annotations

from io import BytesIO
from mimetypes import MimeTypes
from typing import TYPE_CHECKING
from typing import TypedDict

from imageio import v3 as imageio

from labox._internal._utils import frozenclass
from labox.core.serializer import SerializedData
from labox.core.serializer import Serializer

if TYPE_CHECKING:
    from collections.abc import Sequence

    from numpy.typing import NDArray

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
    extension: str | None = None
    """The file extension of the media data."""
    plugin: str | None = None
    """The plugin used to read/write the media data."""

    def __post_init__(self) -> None:
        if self.extension and self.plugin:
            msg = "Cannot set both extension and plugin. Use one or the other."
            raise ValueError(msg)


_DEFAULT_MIMETYPES = MimeTypes()


class MediaSerializer(Serializer[Media, "_MediaSerializerConfig"]):
    """Serializer for ImageIO Arrays.

    Args:
        mimetypes: The MIME types to use for guessing the content type and encoding.
    """

    name = "labox.imageio@v1"
    types = (Media,)

    def __init__(self, mimetypes: MimeTypes = _DEFAULT_MIMETYPES) -> None:
        self.mimetypes = mimetypes

    def serialize_data(self, media: Media) -> SerializedData[_MediaSerializerConfig]:
        """Serialize the given array."""
        buffer = BytesIO()
        imageio.imwrite(
            buffer,
            media.data,
            extension=media.extension,  # type: ignore
            plugin=media.plugin,  # type: ignore
        )

        if media.extension is not None:
            filename = f"file{media.extension}"  # mimetypes needs a filename to guess the type
            content_type, content_encoding = self.mimetypes.guess_file_type(filename)
        else:
            content_type = None
            content_encoding = None

        return {
            "data": buffer.getvalue(),
            "content_type": content_type or "application/octet-stream",
            "content_encoding": content_encoding,
            "config": {"plugin": media.plugin, "extension": media.extension},
        }

    def deserialize_data(self, content: SerializedData[_MediaSerializerConfig]) -> Media:
        """Deserialize the given array."""
        config = content.get("config", {"plugin": None, "extension": None})
        buffer = BytesIO(content["data"])
        data = imageio.imread(
            buffer,
            extension=config.get("extension"),  # type: ignore
            plugin=config.get("plugin"),  # type: ignore
        )
        return Media(
            data=data,
            extension=config.get("extension"),
            plugin=config.get("plugin"),
        )


class _MediaSerializerConfig(TypedDict):
    plugin: str | None
    """The plugin used to read/write the media data."""
    extension: str | None
    """The file extension of the media data."""


media_serializer = MediaSerializer()
"""MediaSerializer with default settings."""
