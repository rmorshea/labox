from ardex.extra.json import JsonStreamSerializer
from tests.serializer_utils import make_stream_serializer_test

test_json_stream_serializer = make_stream_serializer_test(
    JsonStreamSerializer(),
    [{"a": 1, "b": 2}, [1, 2, 3], {"a": [1, 2, 3], "b": {"c": 4}}],
    [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
    [["hello", "world"], ["foo", "bar"], ["baz", "qux"]],
)
