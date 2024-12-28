from lakery.extra.msgpack import MsgPackSerializer
from lakery.extra.msgpack import MsgPackStreamSerializer
from tests.serializer_utils import make_stream_serializer_test
from tests.serializer_utils import make_value_serializer_test

test_msgpack_stream_serializer = make_stream_serializer_test(
    MsgPackStreamSerializer(),
    [{"a": 1, "b": None}, [1, 2, 3], {"a": [1, None, "hi"], "b": {"c": 4}}],
    [[1, 2, 3], [4, 5, 6], [7, 8, 9, None]],
    [["hello", "world"], ["foo", None, "bar"], ["baz", "qux"]],
)


test_msgpack_value_serializer = make_value_serializer_test(
    MsgPackSerializer(),
    {"a": 1, "b": 2},
    [1, 2, 3],
    {"a": [1, 2, 3], "b": {"c": 4}},
    "hello",
    123,
    None,
)


def test_msgpack_value_serializer_with_ext_hook():
    raise AssertionError


def test_msgpack_stream_serializer_with_ext_hook():
    raise AssertionError
