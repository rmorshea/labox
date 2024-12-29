import polars as pl

from lakery.extra.polars import ParquetDataFrameSerializer
from tests.serializer_utils import make_value_serializer_test


def _assert_equal(a: pl.DataFrame, b: pl.DataFrame):
    assert a.equals(b)


test_parquet_df_serializer = make_value_serializer_test(
    ParquetDataFrameSerializer(),
    pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
    assertion=_assert_equal,
)
