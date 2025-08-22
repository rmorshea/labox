from __future__ import annotations

from io import BytesIO
from typing import Literal
from typing import TypedDict

import numpy as np

from labox.core.serializer import SerializedData
from labox.core.serializer import Serializer

__all__ = (
    "NpyDumpArgs",
    "NpyLoadArgs",
    "NpySerializer",
)


class NpySerializer(Serializer[np.ndarray]):
    """Serializer for NumPy arrays using the native .npy format.

    This serializer leverages NumPy's built-in numpy.save and numpy.load functions
    to efficiently serialize and deserialize numpy.ndarray objects.

    Args:
        dump_args: Optional arguments passed to numpy.save.
        load_args: Optional arguments passed to numpy.load.
    """

    name = "labox.numpy.npy@v1"
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
        np.save(buffer, value, allow_pickle=False, **self._dump_args)
        return {
            "content_encoding": None,
            "content_type": "application/x-npy.v3",
            "data": buffer.getvalue(),
        }

    def deserialize_data(self, content: SerializedData, /) -> np.ndarray:
        """Deserialize the given DataFrame."""
        return np.load(BytesIO(content["data"]), allow_pickle=False, **self._load_args)


class NpyDumpArgs(TypedDict, total=False):
    """Arguments for dumping a NumPy array."""


class NpyLoadArgs(TypedDict, total=False):
    """Arguments for loading a NumPy array."""

    mmap_mode: Literal["r+", "r", "w+", "c"] | None
    """If not None, then memory-map the file, using the given mode.

    See [numpy.memmap][] for a detailed description of the modes.
    A memory-mapped array is kept on disk. However, it can be accessed and sliced like
    any ndarray. Memory mapping is especially useful for accessing small fragments of
    large files without reading the entire file into memory.
    """


npy_serializer = NpySerializer()
"""NpySerializer with default settings."""
