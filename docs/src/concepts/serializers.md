# Serializers

Lakery serializers convert values or streams of values into binary data that
[storage backends](storages.md) can save. Likewise, they can also convert binary data
back into values or streams of values.

## Basic Serializers

Basic serializers are used to convert singular values to and from binary data. To define
one you must inherit from the [`Serializer`][lakery.core.serializer.Serializer] class
and provide the following:

-   `name` - a string that uniquely and permanently identifies the serializer.
-   `types` - a tuple of types that this serializer can handle. This is used for
    serializer type inference.
-   `serialize_data` - a method that takes a value and returns a dictionary of
    serialized data.
-   `deserialize_data` - a method that takes serialized data and returns the original
    value.

The code below shows a serializer that turns UTF-8 strings into binary data and back.

```python
from lakery.core.serializer import SerializedData
from lakery.core.serializer import Serializer


class Utf8Serializer(Serializer[str]):
    # Globally unique name for this serializer
    name = "examples.utf8"
    # Used for serializer type inference
    types = (str,)

    def serialize_data(self, value: str) -> SerializedData:
        return {
            "data": value.encode("utf-8"),
            "content_encoding": "utf-8",
            "content_type": "application/text",
        }

    def deserialize_data(self, data: SerializedData) -> Any:
        return data["data"].decode("utf-8")
```

## Stream Serializers

Stream serializers are used to asynchronsouly convert streams of values to and from
streams of binary data. To define one you must inherit from the
[`StreamSerializer`][lakery.core.serializer.StreamSerializer] class and provide the
following:

-   `name` - a string that uniquely and permanently identifies the serializer.
-   `types` - a tuple of types that this serializer can handle. This is used for
    serializer type inference.
-   `serialize_data_stream` - a method that takes an
    [`AsyncIterator`][collections.abc.AsyncIterator] of values and returns a
    [`SerializedDataStream`][lakery.core.serializer.SerializedDataStream].
-   `deserialize_data_stream` - a method that takes a
    [`SerializedDataStream`][lakery.core.serializer.SerializedDataStream] and returns an
    [`AsyncIterator`][collections.abc.AsyncIterator] of values.

It's usually easiest to start with the `serialize_data_stream` method. The code snippets
below show a stream serializer that turns UTF-8 strings into a stream of bytes and back.

```python
from collections.abc import AsyncGenerator

from lakery.core.serializer import SerializedDataStream
from lakery.core.serializer import StreamSerializer


class Utf8StreamSerializer(StreamSerializer[str]):
    ...

    async def serialize_data_stream(self, values: AsyncIterator[str]) -> SerializedDataStream:
        async def byte_stream() -> AsyncGenerator[bytes]:
            async for chunk in values:
                yield chunk.encode("utf-8")

        return {
            "data_stream": byte_stream(),
            "content_encoding": "utf-8",
            "content_type": "application/text",
        }
```

For the `deserialize_data_stream` method, it which must return an `AsyncIterator` of
strings given a `SerializedDataStream` of binary data. Implementing this method can be
tricky since it must be able to handle arbitrary chunking of the originally dumped data.
This is because it's the responsibility of the storage backend to determine how the data
is chunked when its loaded.

Thankfully, in the case of decoding UTF-8 strings, Python's standard [`codecs`][codecs]
library supplies an [`IncrementalDecoder`][codecs.IncrementalDecoder] class that will
handle this for us. The code below implements a utility function that will be useful for
implementing a `Utf8StreamSerializer.deserialize_data_stream` method.

```python
from codecs import IncrementalDecoder
from collections.abc import AsyncIterable
from collections.abc import AsyncIterator


async def decode_async_byte_stream(
    decoder: IncrementalDecoder,
    stream: AsyncIterable[bytes],
) -> AsyncIterator[str]:
    async for byte_chunk in stream:
        if str_chunk := decoder.decode(byte_chunk, final=False):
            yield str_chunk
    yield (last := decoder.decode(b"", final=True))
    if last:
        yield ""
```

!!! note

    You can import this utility from
    [`lakery.common.streaming.decode_async_byte_stream`][lakery.common.streaming.decode_async_byte_stream].

With this you'll be able to implement the `deserialize_data_stream` method like so:

```python
from codecs import getincrementaldecoder

get_utf8_decoder = getincrementaldecoder("utf-8")


class Utf8StreamSerializer(StreamSerializer[str]):
    ...

    async def deserialize_data_stream(self, data: SerializedDataStream) -> AsyncIterator[str]:
        async for str_chunk in decode_async_byte_stream(get_utf8_decoder(), data):
            yield str_chunk
```

## Serializer Names

Serializers are identified by a globally unique name associated with each serializer
class within a [registry](./registry.md#adding-serializers). Once a serializer has been
used to saved data this name must never be changed and the implementation of the
serializer must always remain backwards compatible. If you need to change the
implementation of a serializer you should create a new one with a new name. In order to
continue loading old data you'll have to preserve and registry the old serializer.
