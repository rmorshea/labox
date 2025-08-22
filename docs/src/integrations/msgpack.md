# MessagePack

!!! note

    Install with `pip install labox[msgpack]`

Labox provides two [serializers](../concepts/serializers.md) for the efficient
[MessagePack](https://msgpack.org/) binary format: one for individual values and another
for streams of values. MessagePack is a compact binary serialization format that's
faster and more space-efficient than JSON while supporting similar data types.

## MsgPackSerializer

The [`MsgPackSerializer`][labox.extra.msgpack.MsgPackSerializer] handles individual
MessagePack values:

```python
from labox.extra.msgpack import msgpack_serializer

data = {
    "name": "experiment_1",
    "parameters": {"temperature": 298.15, "pressure": 1.0},
    "results": [12.3, 15.7, 9.8],
    "success": True,
}

serialized_data = msgpack_serializer.serialize_data(data)
```

## MsgPackStreamSerializer

The [`MsgPackStreamSerializer`][labox.extra.msgpack.MsgPackStreamSerializer] handles
streams of MessagePack values, serializing each value separately in the stream:

```python
from labox.extra.msgpack import msgpack_stream_serializer


# For streaming multiple MessagePack values
async def generate_data():
    for i in range(100):
        yield {"id": i, "value": i * 2.5}


# The stream serializer handles async iterables of MessagePack data
```

## Supported Data Types

MessagePack serializers work with the types defined in
[`MSG_PACK_TYPES`][labox.extra.msgpack.MSG_PACK_TYPES]:

- **Primitives**: `int`, `str`, `float`, `bool`, `None`
- **Collections**: `dict`, `list` (with MessagePack-compatible keys and values)
- **Extensions**: Custom MessagePack extension types (via the `Any` type annotation)

The [`MsgPackType`][labox.extra.msgpack.MsgPackType] type alias provides a recursive
type definition for MessagePack-compatible data structures.

## Custom Configuration

Both serializers can be customized with different packer and unpacker implementations:

```python
from msgpack import Packer
from msgpack import Unpacker

from labox.extra.msgpack import MsgPackSerializer
from labox.extra.msgpack import MsgPackStreamSerializer


# Custom packer/unpacker with specific options
def custom_packer():
    return Packer(use_bin_type=True, strict_types=True)


def custom_unpacker():
    return Unpacker(raw=False, strict_map_key=False)


value_serializer = MsgPackSerializer(packer=custom_packer, unpacker=custom_unpacker)

stream_serializer = MsgPackStreamSerializer(packer=custom_packer, unpacker=custom_unpacker)
```

### Content Type

Both serializers use the content type `"application/msgpack"` to identify MessagePack
data, following the standard MIME type for the MessagePack format.
