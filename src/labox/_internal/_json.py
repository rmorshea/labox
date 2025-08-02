from __future__ import annotations

import json
from typing import TYPE_CHECKING
from typing import Any
from typing import TypeVar

if TYPE_CHECKING:
    from labox.common.types import JsonType

T = TypeVar("T", default=Any)

_JsonPrimitiveType = int | str | float | bool | None
"""Type alias for primitive JSON types."""
JsonType = _JsonPrimitiveType | dict[str, "JsonType"] | list["JsonType"] | tuple["JsonType", ...]
"""Type alias for JSON-compatible types."""
JSON_TYPES = (int, str, float, bool, type(None), dict, list, tuple)
"""The types that can be serialized to JSON."""
DEFAULT_JSON_ENCODER = json.JSONEncoder(separators=(",", ":"), allow_nan=False)
"""The default JSON encoder used for serialization."""
DEFAULT_JSON_DECODER = json.JSONDecoder()
"""The default JSON decoder used for deserialization."""
