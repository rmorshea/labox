from __future__ import annotations

from io import BytesIO
from typing import Literal
from typing import TypedDict

import numpy as np

from lakery.core.serializer import SerializedData
from lakery.core.serializer import Serializer

__all__ = (
    "NpyDumpArgs",
    "NpyLoadArgs",
    "NpySerializer",
)


class NpySerializer(Serializer[np.ndarray]):
    """Serializer for Pandas DataFrames using Arrow."""

    name = "lakery.numpy.npy@v1"
    types = (np.ndarray,)

    def __init__(
        self,
        *,
        dump_args: NpyDumpArgs | None = None,
        load_args: NpyLoadArgs | None = None,
    ) -> None:
        self._dump_args = dump_args or {}
        self._load_args = load_args or {}

    def serialize_data(self, value: np.ndarray, /) -> SerializedData:
        """Serialize the given DataFrame."""
        buffer = BytesIO()
        np.save(buffer, value, **self._dump_args)
        return {
            "content_encoding": None,
            "content_type": "application/x-npy.v3",
            "data": buffer.getvalue(),
        }

    def deserialize_data(self, content: SerializedData, /) -> np.ndarray:
        """Deserialize the given DataFrame."""
        return np.load(BytesIO(content["data"]), **self._load_args)


class NpyDumpArgs(TypedDict, total=False):
    """Arguments for dumping a NumPy array."""

    allow_pickle: bool
    fix_imports: bool


class NpyLoadArgs(TypedDict, total=False):
    """Arguments for loading a NumPy array."""

    mmap_mode: Literal["r+", "r", "w+", "c"] | None
    allow_pickle: bool
    fix_imports: bool


npy_serializer = NpySerializer()
"""NpySerializer with default settings."""
