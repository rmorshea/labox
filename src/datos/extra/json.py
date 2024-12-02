import hashlib
import json
from typing import TypeAlias

from datos.core.serializer import ScalarDump
from datos.core.serializer import ScalarSerializer

JsonType: TypeAlias = int | str | float | bool | dict[str, "JsonType"] | list["JsonType"] | None
"""A type alias for JSON data."""


class JsonSerializer(ScalarSerializer[JsonType]):
    """A serializer for JSON data."""

    name = "datos_json"
    version = 1
    types = (int, str, float, bool, type(None), dict, list)

    def dump_scalar(self, value: JsonType) -> ScalarDump:
        """Serialize the given value to JSON."""
        content_bytes = json.dumps(value).encode("utf-8")
        content_hash = hashlib.sha256(content_bytes)
        return {
            "scalar": content_bytes,
            "content_type": "application/json",
            "content_hash": content_hash.hexdigest(),
            "content_size": len(content_bytes),
            "content_hash_algorithm": content_hash.name,
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_scalar(self, dump: ScalarDump) -> JsonType:
        """Deserialize the given JSON data."""
        return json.loads(dump["scalar"].decode("utf-8"))
