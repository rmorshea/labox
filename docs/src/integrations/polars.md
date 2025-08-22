# Polars

!!! note

    Install with `pip install labox[polars]`

Labox provides a high-performance [serializer](../concepts/serializers.md) for
[Polars](https://pola.rs/) DataFrames using the efficient Parquet columnar format. This
integration leverages Polars' native Parquet I/O capabilities to ensure optimal
performance and compatibility with the broader data ecosystem.

## DataFrame Serializer

The [`ParquetDataFrameSerializer`][labox.extra.polars.ParquetDataFrameSerializer]
serializes Polars DataFrames using the Parquet format, which provides excellent
compression ratios and fast read/write performance for analytical workloads.

### Basic Usage

A default instance of the serializer is available as `parquet_dataframe_serializer`:

```python
import polars as pl

from labox.extra.polars import parquet_dataframe_serializer

df = pl.DataFrame(
    {
        "id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "score": [95.5, 87.2, 92.8, 89.1],
        "active": [True, False, True, True],
    }
)

serialized_data = parquet_dataframe_serializer.serialize_data(df)
```

### Custom Configuration

You can create a custom serializer instance with specific Parquet options:

```python
from labox.extra.polars import ParquetDataFrameSerializer

# Configure write options for better compression
serializer = ParquetDataFrameSerializer(
    dump_args={
        "compression": "zstd",
        "compression_level": 3,
        "statistics": True,
        "row_group_size": 50000,
    },
    load_args={
        "parallel": "auto",
        "use_statistics": True,
        "rechunk": True,
    },
)
```

### Advanced Options

The serializer supports extensive Parquet configuration through
[`ParquetDumpArgs`][labox.extra.polars.ParquetDumpArgs] and
[`ParquetLoadArgs`][labox.extra.polars.ParquetLoadArgs]:

```python
# Example with advanced write options
write_optimized_serializer = ParquetDataFrameSerializer(
    dump_args={
        "compression": "snappy",
        "statistics": {"string_columns": True, "null_count": False},
        "data_page_size": 1024 * 1024,  # 1MB pages
        "use_pyarrow": False,  # Use Polars native engine
    }
)

# Example with advanced read options
read_optimized_serializer = ParquetDataFrameSerializer(
    load_args={
        "parallel": "row_groups",
        "low_memory": True,
        "use_statistics": True,
        "rechunk": False,  # Keep original chunking
    }
)
```

### Content Type

The serializer uses the standard content type `"application/vnd.apache.parquet"` to
identify Parquet format data, ensuring compatibility with other Parquet-based tools and
systems.

### Performance Considerations

- **Compression**: The default compression provides a good balance of speed and size.
    For write-heavy workloads, consider `"lz4"` or `"snappy"`. For storage-optimized
    scenarios, use `"zstd"` or `"brotli"`.
- **Parallel Processing**: Polars can leverage multiple CPU cores during read
    operations. Set `parallel="auto"` in `load_args` for optimal performance.
- **Statistics**: Enabling statistics collection during write can significantly speed
    up filtered reads but may slow down write operations.

For more details on the available options, refer to the
[Polars Parquet documentation](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.write_parquet.html).
