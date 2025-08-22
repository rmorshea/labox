# Pandas

!!! note

    Install with `pip install labox[pandas]`

Labox provides comprehensive [serializers](../concepts/serializers.md) for
[Pandas DataFrames](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html),
leveraging [PyArrow](pyarrow.md) under the hood for efficient storage and retrieval. The
pandas integration supports both individual DataFrames and streaming DataFrames, with
options for Arrow IPC format and Parquet columnar format.

## DataFrame Serializers

### ArrowDataFrameSerializer

The [`ArrowDataFrameSerializer`][labox.extra.pandas.ArrowDataFrameSerializer] serializes
Pandas DataFrames using the Arrow IPC file format. For details about Arrow IPC format
benefits and options, see the
[PyArrow Table documentation](pyarrow.md#arrow-table-serializers):

```python
import pandas as pd

from labox.extra.pandas import arrow_dataframe_serializer

df = pd.DataFrame(
    {
        "id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "score": [95.5, 87.2, 92.8, 89.1],
        "active": [True, False, True, True],
    }
)

serialized = arrow_dataframe_serializer.serialize_data(df)
```

You can also create a custom instance with specific Arrow IPC options:

```python
import pyarrow as pa

from labox.extra.pandas import ArrowDataFrameSerializer

serializer = ArrowDataFrameSerializer(
    write_options=pa.ipc.IpcWriteOptions(compression="zstd"),
    read_options=pa.ipc.IpcReadOptions(use_threads=True),
)
```

### ParquetDataFrameSerializer

The [`ParquetDataFrameSerializer`][labox.extra.pandas.ParquetDataFrameSerializer] uses
the Parquet columnar format. For details about Parquet format benefits and options, see
the [PyArrow Parquet documentation](pyarrow.md#parquettableserializer):

```python
from labox.extra.pandas import parquet_dataframe_serializer

df = pd.DataFrame(
    {
        "timestamp": pd.date_range("2024-01-01", periods=1000, freq="1h"),
        "sensor_id": range(1000),
        "temperature": [20.5 + i * 0.1 for i in range(1000)],
    }
)

serialized = parquet_dataframe_serializer.serialize_data(df)
```

For custom Parquet options:

```python
from labox.extra.pandas import ParquetDataFrameSerializer
from labox.extra.pyarrow import ParquetReadOptions
from labox.extra.pyarrow import ParquetWriteOptions

serializer = ParquetDataFrameSerializer(
    write_options=ParquetWriteOptions(compression="snappy"),
    read_options=ParquetReadOptions(use_pandas_metadata=True),
)
```

## Stream Serializers

For streaming large datasets, see
[stream serializers](../concepts/serializers.md#stream-serializers) in the serializers
documentation.

### ArrowDataFrameStreamSerializer

The
[`ArrowDataFrameStreamSerializer`][labox.extra.pandas.ArrowDataFrameStreamSerializer]
handles streams of DataFrames using Arrow record batches:

```python
from labox.extra.pandas import arrow_dataframe_stream_serializer


async def generate_dataframes():
    for i in range(10):
        yield pd.DataFrame({"batch": [i] * 100, "value": range(i * 100, (i + 1) * 100)})


stream = generate_dataframes()
serialized_stream = arrow_dataframe_stream_serializer.serialize_data_stream(stream)

# Deserialize back to stream
async for df in arrow_dataframe_stream_serializer.deserialize_data_stream(serialized_stream):
    print(f"Batch shape: {df.shape}")
```

### ParquetDataFrameStreamSerializer

The
[`ParquetDataFrameStreamSerializer`][labox.extra.pandas.ParquetDataFrameStreamSerializer]
provides streaming with Parquet format:

```python
from labox.extra.pandas import parquet_dataframe_stream_serializer

# Use the same pattern as above with parquet_dataframe_stream_serializer
```
