from io import BytesIO
from typing import Literal
from typing import TypedDict

import numpy as np

from lakery.core.serializer import ValueDump
from lakery.core.serializer import ValueSerializer


class NpyDumpArgs(TypedDict, total=False):
    """Arguments for dumping a NumPy array."""

    allow_pickle: bool
    fix_imports: bool


class NpyLoadArgs(TypedDict, total=False):
    """Arguments for loading a NumPy array."""

    mmap_mode: Literal["r+", "r", "w+", "c"] | None
    allow_pickle: bool
    fix_imports: bool


class NpySerializer(ValueSerializer[np.ndarray]):
    """Serializer for Pandas DataFrames using Arrow."""

    name = "lakery.numpy.npy"
    version = 1
    types = (np.ndarray,)

    def __init__(
        self,
        *,
        dump_args: NpyDumpArgs | None = None,
        load_args: NpyLoadArgs | None = None,
    ) -> None:
        self._dump_args = dump_args or {}
        self._load_args = load_args or {}

    def dump_value(self, value: np.ndarray, /) -> ValueDump:
        """Serialize the given DataFrame."""
        buffer = BytesIO()
        np.save(buffer, value, **self._dump_args)
        return {
            "content_encoding": None,
            "content_type": "application/x-npy.v3",
            "content_value": buffer.getvalue(),
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_value(self, dump: ValueDump, /) -> np.ndarray:
        """Deserialize the given DataFrame."""
        return np.load(BytesIO(dump["content_value"]), **self._load_args)
