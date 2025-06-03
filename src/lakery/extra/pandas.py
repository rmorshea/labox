from __future__ import annotations

import pandas as pd
import pyarrow as pa

from lakery.core.serializer import SerializedData
from lakery.core.serializer import Serializer
from lakery.extra.pyarrow import ArrowTableSerializer
from lakery.extra.pyarrow import ParquetReadOptions
from lakery.extra.pyarrow import ParquetTableSerializer
from lakery.extra.pyarrow import ParquetWriteOptions

__all__ = [
    "ArrowDataFrameSerializer",
    "ParquetDataFrameSerializer",
    "ParquetReadOptions",
    "ParquetWriteOptions",
    "arrow_dataframe_serializer",
    "parquet_dataframe_serializer",
]


class ArrowDataFrameSerializer(Serializer[pd.DataFrame]):
    """Serializer for Pandas DataFrames using Arrow."""

    name = "lakery.pandas.arrow.file@v1"
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

    def serialize_data(self, value: pd.DataFrame, /) -> SerializedData:
        """Serialize the given DataFrame."""
        table = pa.Table.from_pandas(value)
        return self._arrow_serializer.serialize_data(table)

    def deserialize_data(self, content: SerializedData, /) -> pd.DataFrame:
        """Deserialize the given DataFrame."""
        table = self._arrow_serializer.deserialize_data(content)
        return table.to_pandas()


class ParquetDataFrameSerializer(Serializer[pd.DataFrame]):
    """Serializer for Pandas DataFrames using Parquet."""

    name = "lakery.pandas.parquet.file@v1"
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

    def serialize_data(self, value: pd.DataFrame, /) -> SerializedData:
        """Serialize the given DataFrame."""
        table = pa.Table.from_pandas(value)
        return self._parquet_serializer.serialize_data(table)

    def deserialize_data(self, content: SerializedData, /) -> pd.DataFrame:
        """Deserialize the given DataFrame."""
        table = self._parquet_serializer.deserialize_data(content)
        return table.to_pandas()


arrow_dataframe_serializer = ArrowDataFrameSerializer()
"""ArrowDataFrameSerializer with default settings."""

parquet_dataframe_serializer = ParquetDataFrameSerializer()
"""ParquetDataFrameSerializer with default settings."""
