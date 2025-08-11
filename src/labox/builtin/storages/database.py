import re
from collections.abc import AsyncGenerator
from warnings import warn

from labox.common.types import TagMap
from labox.core.storage import Digest
from labox.core.storage import Storage

__all__ = ("DatabaseStorage", "database_storage")

WARN_SIZE_DEFAULT = 10 * 1024  # 10 KB
ERROR_SIZE_DEFAULT = 100 * 1024  # 100 KB
JSON_CONTENT_TYPE_PATTERN = re.compile(r"^application/(.*?\+?json|json\+?.*?)\;?.*$")


class DatabaseStorage(Storage[str]):
    """Stores data directly in the database instead of remotely.

    Is constrained to:

    - JSON content types (e.g., `application/json`, `application/vnd.api+json`)
    - No data streams, only single JSON strings
    - A maximum size. By default it warns at 10 KB and raises an error at 100 KB.

    Is most useful in cases where
    """

    name = "labox.json_storage_data@v1"

    def __init__(self, *, warn_size: int = WARN_SIZE_DEFAULT, error_size: int = ERROR_SIZE_DEFAULT):
        super().__init__()
        self.warn_size = warn_size
        self.error_size = error_size

    async def write_data(self, data: bytes, digest: Digest, _tags: TagMap) -> str:
        """Save the given value data."""
        if not JSON_CONTENT_TYPE_PATTERN.match(digest["content_type"]):
            msg = f"{self} only supports JSON content types, got {digest['content_type']!r}."
            raise ValueError(msg)

        content_size = digest["content_size"]
        if content_size > self.error_size:
            msg = f"{self} supports a maximum size of {self.error_size} bytes, got {content_size}."
            raise ValueError(msg)
        if content_size > self.warn_size:
            warn(
                f"{self} only supports a maximum size of {self.warn_size} bytes to "
                f"avoid large table sizes, got {content_size}. "
                "Consider using a different storage backend for larger data.",
                UserWarning,
                stacklevel=2,
            )

        return data.decode("utf-8")

    async def read_data(self, config: str) -> bytes:
        """Load data using the given information."""
        return config.encode("utf-8")

    async def write_data_stream(self, data_stream, get_digest, _tags: TagMap) -> str:
        """Save the given data stream."""
        msg = (
            f"{self} does not support writing data streams. "
            "Use `write_data` with a single JSON string instead."
        )
        raise NotImplementedError(msg)

    def read_data_stream(self, config: str) -> AsyncGenerator[bytes]:
        """Load a stream of data using the given information."""
        msg = (
            f"{self} does not support reading data streams. "
            "Use `read_data` to get a single JSON string instead."
        )
        raise NotImplementedError(msg)

    def serialize_config(self, config: str, /) -> str:
        """Return the config as-is because it is already a JSON string."""
        return config

    def deserialize_config(self, data: str) -> str:
        """Return the data as-is because it is already a JSON string."""
        return data


database_storage = DatabaseStorage()
"""A singleton instance of DatabaseStorage for convenience."""
