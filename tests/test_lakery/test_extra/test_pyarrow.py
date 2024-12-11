import pyarrow as pa

from lakery.extra.pyarrow import ArrowRecordBatchStreamSerializer
from lakery.extra.pyarrow import ArrowTableSerializer
from lakery.extra.pyarrow import ParquetRecordBatchStreamSerializer
from lakery.extra.pyarrow import ParquetTableSerializer
from tests.serializer_utils import make_stream_serializer_test
from tests.serializer_utils import make_value_serializer_test

TABLES = [
    pa.table({"a": [1, 2, 3], "b": [4, 5, 6]}),
    pa.table({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}),
]
BATCHES = [
    [
        pa.record_batch({"a": [1, 2, 3], "b": [4, 5, 6]}),
        pa.record_batch({"a": [7, 8, 9], "b": [10, 11, 12]}),
    ],
    [
        pa.record_batch({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}),
        pa.record_batch({"a": [10, 11, 12], "b": [13, 14, 15], "c": [16, 17, 18]}),
    ],
]

test_arrow_table_serializer = make_value_serializer_test(
    ArrowTableSerializer(),
    *TABLES,
)
test_arrow_record_batch_stream_serializer = make_stream_serializer_test(
    ArrowRecordBatchStreamSerializer(),
    *BATCHES,
)
test_parquet_table_serializer = make_value_serializer_test(
    ParquetTableSerializer(),
    *TABLES,
)
test_parquet_record_batch_stream_serializer = make_stream_serializer_test(
    ParquetRecordBatchStreamSerializer(),
    *BATCHES,
)
