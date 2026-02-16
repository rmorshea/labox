from collections.abc import Sequence

import pandas as pd

from labox.extra.pandas import ArrowDataFrameSerializer
from labox.extra.pandas import ArrowDataFrameStreamSerializer
from labox.extra.pandas import ParquetDataFrameSerializer
from labox.extra.pandas import ParquetDataFrameStreamSerializer
from labox.test.core_serializer_utils import make_stream_serializer_test
from labox.test.core_serializer_utils import make_value_serializer_test

DATAFRAMES = [
    pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
    pd.DataFrame({"a": [7, 8, 9], "b": [10, 11, 12]}),
    pd.DataFrame({"a": [13, 14, 15], "b": [16, 17, 18]}),
]


def _assert_equal(a: pd.DataFrame, b: pd.DataFrame):
    assert a.equals(b)


def _assert_equal_stream(a: Sequence[pd.DataFrame], b: Sequence[pd.DataFrame]):
    for df_a, df_b in zip(a, b, strict=True):
        assert df_a.equals(df_b)


test_arrow_df_serializer = make_value_serializer_test(
    ArrowDataFrameSerializer(),
    *DATAFRAMES,
    assertion=_assert_equal,
)
test_arrow_df_stream_serializer = make_stream_serializer_test(
    ArrowDataFrameStreamSerializer(),
    DATAFRAMES,
    assertion=_assert_equal_stream,
)
test_parquet_df_serializer = make_value_serializer_test(
    ParquetDataFrameSerializer(),
    *DATAFRAMES,
    assertion=_assert_equal,
)
test_parquet_df_stream_serializer = make_stream_serializer_test(
    ParquetDataFrameStreamSerializer(),
    DATAFRAMES,
    assertion=_assert_equal_stream,
)
