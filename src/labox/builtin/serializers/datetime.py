from __future__ import annotations

from datetime import datetime

from labox.core.serializer import SerializedData
from labox.core.serializer import Serializer

__all__ = ("Iso8601Serializer", "iso8601_serializer")


class Iso8601Serializer(Serializer[datetime]):
    """A serializer for JSON data."""

    name = "labox.iso8601@v1"
    types = (datetime,)

    def serialize_data(self, value: datetime) -> SerializedData:
        """Serialize the given value to JSON."""
        return {
            "content_encoding": "utf-8",
            "content_type": "application/text",
            "data": value.isoformat().encode("utf-8"),
        }

    def deserialize_data(self, content: SerializedData) -> datetime:
        """Deserialize the given JSON data."""
        return datetime.fromisoformat(content["data"].decode("utf-8"))


iso8601_serializer = Iso8601Serializer()
"""Iso8601Serializer with default settings."""
