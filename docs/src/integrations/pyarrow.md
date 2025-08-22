# PyArrow

!!! note

    Install with `pip install labox[pyarrow]`

Labox provides comprehensive [serializers](../concepts/serializers.md) for
[Apache Arrow](https://arrow.apache.org/) data structures, supporting both the native
Arrow IPC format and the popular Parquet columnar format. These serializers enable
efficient storage and retrieval of tabular data with high performance and cross-language
compatibility.

## Arrow Table Serializers

### ArrowTableSerializer

The [`ArrowTableSerializer`][labox.extra.pyarrow.ArrowTableSerializer] serializes
PyArrow tables using the native Arrow IPC file format:

```python
import pyarrow as pa

from labox.extra.pyarrow import arrow_table_serializer

table = pa.table(
    {
        "id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "score": [95.5, 87.2, 92.8, 89.1],
    }
)

serialized = arrow_table_serializer.serialize_data(table)
```

You can also create a custom instance with specific options:

```python
import pyarrow as pa

from labox.extra.pyarrow import ArrowTableSerializer

serializer = ArrowTableSerializer(
    write_options=pa.ipc.IpcWriteOptions(compression="zstd"),
    read_options=pa.ipc.IpcReadOptions(use_threads=True),
)
```

### ParquetTableSerializer

The [`ParquetTableSerializer`][labox.extra.pyarrow.ParquetTableSerializer] serializes
PyArrow tables using the Parquet columnar format:

```python
from labox.extra.pyarrow import parquet_table_serializer

# Serialize using the default Parquet serializer
serialized = parquet_table_serializer.serialize_data(table)
```

For advanced Parquet configuration:

```python
from labox.extra.pyarrow import ParquetTableSerializer

serializer = ParquetTableSerializer(
    write_options={
        "compression": "SNAPPY",
        "use_dictionary": True,
        "write_statistics": True,
    },
    read_options={
        "use_threads": True,
        "buffer_size": 8192,
    },
)
```

## Record Batch Serializers

For handling streams of record batches, Labox provides specialized stream serializers.

### ArrowRecordBatchStreamSerializer

The
[`ArrowRecordBatchStreamSerializer`][labox.extra.pyarrow.ArrowRecordBatchStreamSerializer]
handles streams of PyArrow record batches using the Arrow IPC stream format:

```python
import pyarrow as pa

from labox.extra.pyarrow import arrow_record_batch_stream_serializer


async def generate_batches():
    """Generate a stream of record batches."""
    schema = pa.schema(
        [
            ("timestamp", pa.timestamp("ms")),
            ("value", pa.float64()),
        ]
    )

    for i in range(10):
        batch = pa.record_batch(
            [
                [1000 * i, 1000 * (i + 1)],
                [i * 2.5, (i + 1) * 2.5],
            ],
            schema=schema,
        )
        yield batch


# Serialize the stream
stream_data = arrow_record_batch_stream_serializer.serialize_data_stream(generate_batches())
```

### ParquetRecordBatchStreamSerializer

The
[`ParquetRecordBatchStreamSerializer`][labox.extra.pyarrow.ParquetRecordBatchStreamSerializer]
handles streams of record batches using the Parquet format:

```python
from labox.extra.pyarrow import parquet_record_batch_stream_serializer

# Serialize stream to Parquet format
stream_data = parquet_record_batch_stream_serializer.serialize_data_stream(generate_batches())
```

## Configuration Options

### Parquet Write Options

The [`ParquetWriteOptions`][labox.extra.pyarrow.ParquetWriteOptions] TypedDict provides
type-safe configuration for Parquet writing:

```python
from labox.extra.pyarrow import ParquetTableSerializer

write_options = {
    "version": "2.6",
    "compression": "ZSTD",
    "compression_level": 3,
    "use_dictionary": ["string_column", "category_column"],
    "write_statistics": True,
    "coerce_timestamps": "ms",
    "data_page_size": 1024 * 1024,  # 1MB pages
}

serializer = ParquetTableSerializer(write_options=write_options)
```

### Parquet Read Options

The [`ParquetReadOptions`][labox.extra.pyarrow.ParquetReadOptions] TypedDict provides
type-safe configuration for Parquet reading:

```python
read_options = {
    "use_threads": True,
    "buffer_size": 8192,
    "pre_buffer": True,
    "memory_map": True,
}

serializer = ParquetTableSerializer(read_options=read_options)
```

## Performance Considerations

### Arrow vs Parquet

- **Arrow IPC format**: Faster serialization/deserialization, preserves exact schema
    including metadata, ideal for temporary storage and inter-process communication
- **Parquet format**: Better compression ratios, optimized for analytical queries,
    better for long-term storage and cross-system compatibility

### Streaming vs Table

- **Table serializers**: Best for complete datasets that fit in memory
- **Stream serializers**: Better for large datasets, enables processing data in
    chunks, reduces memory usage

## Content Types

The serializers use the following content types:

- **Arrow formats**: `"application/vnd.apache.arrow.file"`
- **Parquet formats**: `"application/vnd.apache.parquet"`

## Default Instances

Pre-configured serializer instances are available for common use cases:

- [`arrow_table_serializer`][labox.extra.pyarrow.arrow_table_serializer]
- [`arrow_record_batch_stream_serializer`][labox.extra.pyarrow.arrow_record_batch_stream_serializer]
- [`parquet_table_serializer`][labox.extra.pyarrow.parquet_table_serializer]
- [`parquet_record_batch_stream_serializer`][labox.extra.pyarrow.parquet_record_batch_stream_serializer]
