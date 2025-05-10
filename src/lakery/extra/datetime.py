from __future__ import annotations

from datetime import datetime

from lakery.core.serializer import Archive
from lakery.core.serializer import Serializer

__all__ = ("Iso8601Serializer",)


class Iso8601Serializer(Serializer[datetime]):
    """A serializer for JSON data."""

    name = "lakery.json.value"
    version = 1
    types = (datetime,)
    content_type = "application/text"

    def dump(self, value: datetime) -> Archive:
        """Serialize the given value to JSON."""
        return {
            "content_encoding": "utf-8",
            "content_type": self.content_type,
            "data": value.isoformat().encode("utf-8"),
        }

    def load(self, content: Archive) -> datetime:
        """Deserialize the given JSON data."""
        return datetime.fromisoformat(content["data"].decode("utf-8"))
