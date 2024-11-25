import hashlib
import json
from typing import TypeAlias

from artery.core.serializer import DataDump
from artery.core.serializer import Serializer

JsonType: TypeAlias = "int | str | float | bool | None | dict[str, JsonType] | list[JsonType]"
"""A type alias for JSON data."""


class JsonSerializer(Serializer[JsonType]):
    """A serializer for JSON data."""

    name = "artery_json_v1"
    types = (int, str, float, bool, type(None), dict, list)

    def dump(self, value: JsonType) -> DataDump:
        content_bytes = json.dumps(value).encode("utf-8")
        content_hash = hashlib.sha256(content_bytes)
        return {
            "content_bytes": content_bytes,
            "content_type": "application/json",
            "content_hash": content_hash.hexdigest(),
            "content_hash_algorithm": content_hash.name,
        }

    def load(self, data: DataDump) -> JsonType:
        return json.loads(data["content_bytes"].decode("utf-8"))
