from ardex.extra.json import JsonScalarSerializer
from ardex.extra.json import JsonStreamSerializer
from tests.serializer_utils import make_scalar_serializer_test
from tests.serializer_utils import make_stream_serializer_test

test_json_stream_serializer = make_stream_serializer_test(
    JsonStreamSerializer(),
    [{"a": 1, "b": None}, [1, 2, 3], {"a": [1, None, "hi"], "b": {"c": 4}}],
    [[1, 2, 3], [4, 5, 6], [7, 8, 9, None]],
    [["hello", "world"], ["foo", None, "bar"], ["baz", "qux"]],
)


test_json_scalar_serializer = make_scalar_serializer_test(
    JsonScalarSerializer(),
    {"a": 1, "b": 2},
    [1, 2, 3],
    {"a": [1, 2, 3], "b": {"c": 4}},
    "hello",
    123,
    None,
)
