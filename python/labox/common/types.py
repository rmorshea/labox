from collections.abc import Mapping

TagMap = Mapping[str, str]
"""A simple mapping from tag names to values."""
JsonType = (
    int
    | str
    | float
    | bool
    | dict[str, "JsonType"]
    | list["JsonType"]
    | tuple["JsonType", ...]
    | None
)
"""A type alias for JSON data."""
