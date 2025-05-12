# Serializers

Serializers tell Lakery how to convert values or streams of values into binary data that
[storage backends](storages.md) can save. Likewise, they can also convert binary data
back into values or streams of values.

## Normal Serializers

Normal serializers are used to convert singular values to and from binary data. To
define one you must inherit from the [`Serializer`][lakery.core.serializers.Serializer]
class, define a [globally unique name](#serializer-names), as well as implement `load`
and `dump` methods. The code below shows a serializer that turns UTF-8 strings into
binary data and back.

```python
from lakery.core.serializer import SerializedData
from lakery.core.serializer import Serializer


class Utf8Serializer(Serializer[str]):
    # Globally unique name for this serializer
    name = "examples.utf8"
    # Used for serializer type inference
    types = (str,)

    def dump(self, value: str) -> SerializedData:
        return {
            "data": value.encode("utf-8"),
            "content_encoding": "utf-8",
            "content_type": "application/text",
        }

    def load(self, data: SerializedData) -> Any:
        return data["data"].decode("utf-8")
```

## Stream Serializers

Stream serializers are used to asynchronsouly convert streams of values to and from
streams of binary data. To define one you must inherit from the
[`StreamSerializer`][lakery.core.serializers.StreamSerializer] class. Since the
`StreamSerializer` class is a subclass of the `Serializer` class, you can use the same
`dump` and `load` methods as you would for a normal serializer. In addition, you must
implement the `dump_stream` and `load_stream` methods. To demonstrate what this looks
like the code samples below will walk you though defining a stream serializer that
converts a stream of UTF-8 strings into a stream of binary data and back beginning with
the `dump` and `load` methods. Notably these take and return an `Iterable` of values
instead of a singular value.

```python
from collections.abc import Iterable

from lakery.core.serializer import SerializedData
from lakery.core.serializer import StreamSerializer


class Utf8StreamSerializer(StreamSerializer[str]):

    # Globally unique name for this serializer
    name = "examples.utf8_stream"
    # Used for serializer type inference
    types = (str,)

    def dump(self, value: Iterable[str]) -> SerializedData:
        return {
            "data": b"".join([s.encode("utf-8") for s in value]),
            "content_encoding": "utf-8",
            "content_type": "application/text",
        }

    def load(self, data: SerializedData) -> Iterable[str]:
        return [data["data"].decode("utf-8")]
```

Next you'll need to implement the `dump_stream` method which takes an `AsyncIterator`:

```python
class Utf8StreamSerializer(StreamSerializer[str]):
    ...

    async def dump_stream(self, value: AsyncIterator[str]) -> AsyncIterator[bytes]:
        async for str_chunk in value:
            yield str_chunk.encode("utf-8")
```

And lastly, the `load_stream` method which must return an `AsyncIterator`. Implementing
this method can be tricky since it must be able to handle arbitrary chunking of the
originally dumped data. This is because it's the responsibility of the storage backend
to determine how the data is chunked when its saved and loaded.

Thankfully, in the case of decoding UTF-8 strings Python's standard [`codecs`][codecs]
library supplies an [`IncrementalDecoder`][codecs.IncrementalDecoder] class that will
handle this for us. The code below implements an `decode_async_byte_stream` utility
function that will be useful for implementing a `Utf8StreamSerializer.load_stream`
method.

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

With this you'll be able to implement the `load_stream` method like so:

```python
from codecs import getincrementaldecoder

get_utf8_decoder = getincrementaldecoder("utf-8")


class Utf8StreamSerializer(StreamSerializer[str]):
    ...

    async def load_stream(self, data: SerializedDataStream) -> AsyncIterator[str]:
        async for str_chunk in decode_async_byte_stream(get_utf8_decoder(), data):
            yield str_chunk
```

## Serializer Names

Serializers are identified by a globally unique name when data is saved and then loaded
later. Once a serializer has been used to saved data this name must never be changed and
the implementation of the serializer must always remain backwards compatible. If you
need to change the implementation of a serializer you should create a new one with a new
name.

## Serializer Type Inference

Lakery inspects the [`types`][lakery.core.serializer.Serializer.types] tuples assigned
of serializers in order to automatically infer which serializer to use when saving a
value if one was not explicitly specified. If multiple serializers declare the same
`type`s they are resolved based on the order they were added to the
[serializer registry](./registries.md#infering-serializers) being used to save the
value.
