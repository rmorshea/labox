import numpy as np

from labox.extra.numpy import NpySerializer
from tests.core_serializer_utils import make_value_serializer_test


def _assert_equal(a: np.ndarray, b: np.ndarray):
    np.testing.assert_array_equal(a, b)


test_npy_value_serializer = make_value_serializer_test(
    NpySerializer(),
    np.array([1, 2, 3]),
    assertion=_assert_equal,
)
