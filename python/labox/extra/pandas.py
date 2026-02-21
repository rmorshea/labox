from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pyarrow as pa

from labox.core.serializer import SerializedData
from labox.core.serializer import SerializedDataStream
from labox.core.serializer import Serializer
from labox.core.serializer import StreamSerializer
from labox.extra.pyarrow import ArrowRecordBatchStreamSerializer
from labox.extra.pyarrow import ArrowTableSerializer
from labox.extra.pyarrow import ParquetReadOptions
from labox.extra.pyarrow import ParquetRecordBatchStreamSerializer
from labox.extra.pyarrow import ParquetTableSerializer
from labox.extra.pyarrow import ParquetWriteOptions

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable

__all__ = [
    "ArrowDataFrameSerializer",
    "ArrowDataFrameStreamSerializer",
    "ParquetDataFrameSerializer",
    "ParquetDataFrameStreamSerializer",
    "ParquetReadOptions",
    "ParquetWriteOptions",
    "arrow_dataframe_serializer",
    "arrow_dataframe_stream_serializer",
    "parquet_dataframe_serializer",
    "parquet_dataframe_stream_serializer",
]


class ArrowDataFrameSerializer(Serializer[pd.DataFrame]):
    """Serializer for Pandas DataFrames using Arrow."""

    name = "labox.pandas.arrow.file@v1"
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


class ArrowDataFrameStreamSerializer(StreamSerializer[pd.DataFrame]):
    """Serializer for Pandas DataFrames using Arrow in a streaming manner."""

    name = "labox.pandas.arrow.stream@v1"
    types = (pd.DataFrame,)

    def __init__(
        self,
        write_options: pa.ipc.IpcReadOptions | None = None,
        read_options: pa.ipc.IpcReadOptions | None = None,
    ) -> None:
        self._arrow_serializer = ArrowRecordBatchStreamSerializer(
            write_options=write_options,
            read_options=read_options,
        )

    def serialize_data_stream(self, stream: AsyncIterable[pd.DataFrame]) -> SerializedDataStream:
        """Serialize the given DataFrame stream."""

        async def stream_record_batches() -> AsyncIterable[pa.RecordBatch]:
            async for df in stream:
                yield pa.RecordBatch.from_pandas(df)

        return self._arrow_serializer.serialize_data_stream(stream_record_batches())

    def deserialize_data_stream(
        self, content: SerializedDataStream
    ) -> AsyncGenerator[pd.DataFrame]:
        """Deserialize the given DataFrame stream."""

        async def stream_dataframes() -> AsyncGenerator[pd.DataFrame]:
            async for record_batch in self._arrow_serializer.deserialize_data_stream(content):
                yield record_batch.to_pandas()

        return stream_dataframes()


class ParquetDataFrameSerializer(Serializer[pd.DataFrame]):
    """Serializer for Pandas DataFrames using Parquet."""

    name = "labox.pandas.parquet.file@v1"
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


class ParquetDataFrameStreamSerializer(StreamSerializer[pd.DataFrame]):
    """Serializer for Pandas DataFrames using Parquet in a streaming manner."""

    name = "labox.pandas.parquet.stream@v1"
    types = (pd.DataFrame,)

    def __init__(
        self,
        *,
        write_options: ParquetWriteOptions | None = None,
        read_options: ParquetReadOptions | None = None,
    ) -> None:
        self._parquet_serializer = ParquetRecordBatchStreamSerializer(
            write_options=write_options,
            read_options=read_options,
        )

    def serialize_data_stream(self, stream: AsyncIterable[pd.DataFrame]) -> SerializedDataStream:
        """Serialize the given DataFrame stream."""

        async def stream_record_batches() -> AsyncIterable[pa.RecordBatch]:
            async for df in stream:
                yield pa.RecordBatch.from_pandas(df)

        return self._parquet_serializer.serialize_data_stream(stream_record_batches())

    def deserialize_data_stream(
        self, content: SerializedDataStream
    ) -> AsyncGenerator[pd.DataFrame]:
        """Deserialize the given DataFrame stream."""

        async def stream_dataframes() -> AsyncGenerator[pd.DataFrame]:
            async for record_batch in self._parquet_serializer.deserialize_data_stream(content):
                yield record_batch.to_pandas()

        return stream_dataframes()


arrow_dataframe_serializer = ArrowDataFrameSerializer()
"""ArrowDataFrameSerializer with default settings."""

arrow_dataframe_stream_serializer = ArrowDataFrameStreamSerializer()
"""ArrowDataFrameStreamSerializer with default settings."""

parquet_dataframe_serializer = ParquetDataFrameSerializer()
"""ParquetDataFrameSerializer with default settings."""

parquet_dataframe_stream_serializer = ParquetDataFrameStreamSerializer()
"""ParquetDataFrameStreamSerializer with default settings."""
