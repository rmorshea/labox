import pandas as pd

from lakery.extra.pandas import ArrowDataFrameSerializer
from lakery.extra.pandas import ParquetDataFrameSerializer
from tests.core_serializer_utils import make_value_serializer_test

DATAFRAMES = [pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})]


def _assert_equal(a: pd.DataFrame, b: pd.DataFrame):
    assert a.equals(b)


test_arrow_df_serializer = make_value_serializer_test(
    ArrowDataFrameSerializer(),
    *DATAFRAMES,
    assertion=_assert_equal,
)
test_parquet_df_serializer = make_value_serializer_test(
    ParquetDataFrameSerializer(),
    *DATAFRAMES,
    assertion=_assert_equal,
)
