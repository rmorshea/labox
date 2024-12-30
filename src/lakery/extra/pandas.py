from __future__ import annotations

import pandas as pd
import pyarrow as pa

from lakery.core.serializer import ValueDump
from lakery.core.serializer import ValueSerializer
from lakery.extra.pyarrow import ArrowTableSerializer
from lakery.extra.pyarrow import ParquetReadOptions
from lakery.extra.pyarrow import ParquetTableSerializer
from lakery.extra.pyarrow import ParquetWriteOptions

__all__ = [
    "ArrowDataFrameSerializer",
    "ParquetDataFrameSerializer",
    "ParquetReadOptions",
    "ParquetWriteOptions",
]


class ArrowDataFrameSerializer(ValueSerializer[pd.DataFrame]):
    """Serializer for Pandas DataFrames using Arrow."""

    name = "lakery.pandas.arrow.file"
    version = 1
    types = (pd.DataFrame,)

    def __init__(
        self,
        write_options: pa.ipc.IpcReadOptions | None = None,
        read_options: pa.ipc.IpcReadOptions | None = None,
    ) -> None:
        self._arrow_serializer = ArrowTableSerializer(
            write_options=write_options,
            read_options=read_options,
        )

    def dump_value(self, value: pd.DataFrame, /) -> ValueDump:
        """Serialize the given DataFrame."""
        table = pa.Table.from_pandas(value)
        return {
            **self._arrow_serializer.dump_value(table),
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_value(self, dump: ValueDump, /) -> pd.DataFrame:
        """Deserialize the given DataFrame."""
        table = self._arrow_serializer.load_value(dump)
        return table.to_pandas()


class ParquetDataFrameSerializer(ValueSerializer[pd.DataFrame]):
    """Serializer for Pandas DataFrames using Parquet."""

    name = "lakery.pandas.parquet.file"
    version = 1
    types = (pd.DataFrame,)

    def __init__(
        self,
        *,
        write_options: ParquetWriteOptions | None = None,
        read_options: ParquetReadOptions | None = None,
    ) -> None:
        self._parquet_serializer = ParquetTableSerializer(
            write_options=write_options,
            read_options=read_options,
        )

    def dump_value(self, value: pd.DataFrame, /) -> ValueDump:
        """Serialize the given DataFrame."""
        table = pa.Table.from_pandas(value)
        return {
            **self._parquet_serializer.dump_value(table),
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_value(self, dump: ValueDump, /) -> pd.DataFrame:
        """Deserialize the given DataFrame."""
        table = self._parquet_serializer.load_value(dump)
        return table.to_pandas()
