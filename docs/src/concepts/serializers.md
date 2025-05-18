# Serializers

Lakery serializers convert values or streams of values into binary data that
[storage backends](storages.md) can save. Likewise, they can also convert binary data
back into values or streams of values.

## Basic Serializers

Basic serializers are used to convert singular values to and from binary data. To define
one you must inherit from the [`Serializer`][lakery.core.serializer.Serializer] class,
define a [globally unique name](#serializer-names), as well as implement `load_data` and
`dump_data` methods. The code below shows a serializer that turns UTF-8 strings into
binary data and back.

```python
from lakery.core.serializer import SerializedData
from lakery.core.serializer import Serializer


class Utf8Serializer(Serializer[str]):
    # Globally unique name for this serializer
    name = "examples.utf8"
    # Used for serializer type inference
    types = (str,)

    def dump_data(self, value: str) -> SerializedData:
        return {
            "data": value.encode("utf-8"),
            "content_encoding": "utf-8",
            "content_type": "application/text",
        }

    def load_data(self, data: SerializedData) -> Any:
        return data["data"].decode("utf-8")
```

## Stream Serializers

Stream serializers are used to asynchronsouly convert streams of values to and from
streams of binary data. To define one you must inherit from the
[`StreamSerializer`][lakery.core.serializer.StreamSerializer] class. Since the
`StreamSerializer` class is a subclass of the `Serializer` class, you can use the same
`dump_data` and `load_data` methods as you would for a basic serializer. In addition,
you must implement the `dump_data_stream` and `load_data_stream` methods. To demonstrate
what this looks like the code samples below will walk you though defining a stream
serializer that converts a stream of UTF-8 strings into a stream of binary data and back
beginning with the `dump_data` and `load_data` methods. Notably these take and return an
`Iterable` of values instead of a singular value.

```python
from collections.abc import Iterable

from lakery.core.serializer import SerializedData
from lakery.core.serializer import StreamSerializer


class Utf8StreamSerializer(StreamSerializer[str]):
    # Globally unique name for this serializer
    name = "examples.utf8_stream"
    # Used for serializer type inference
    types = (str,)

    def dump_data(self, value: Iterable[str]) -> SerializedData:
        return {
            "data": b"".join([s.encode("utf-8") for s in value]),
            "content_encoding": "utf-8",
            "content_type": "application/text",
        }

    def load_data(self, data: SerializedData) -> Iterable[str]:
        return [data["data"].decode("utf-8")]
```

Next you'll need to implement the `dump_data_stream` method which takes an
`AsyncIterator`:

```python
class Utf8StreamSerializer(StreamSerializer[str]):
    ...

    async def dump_data_stream(self, value: AsyncIterator[str]) -> AsyncIterator[bytes]:
        async for str_chunk in value:
            yield str_chunk.encode("utf-8")
```

And lastly, the `load_data_stream` method which must return an `AsyncIterator`.
Implementing this method can be tricky since it must be able to handle arbitrary
chunking of the originally dumped data. This is because it's the responsibility of the
storage backend to determine how the data is chunked when its saved and loaded.

Thankfully, in the case of decoding UTF-8 strings Python's standard [`codecs`][codecs]
library supplies an [`IncrementalDecoder`][codecs.IncrementalDecoder] class that will
handle this for us. The code below implements an `decode_async_byte_stream` utility
function that will be useful for implementing a `Utf8StreamSerializer.load_data_stream`
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

With this you'll be able to implement the `load_data_stream` method like so:

```python
from codecs import getincrementaldecoder

get_utf8_decoder = getincrementaldecoder("utf-8")


class Utf8StreamSerializer(StreamSerializer[str]):
    ...

    async def load_data_stream(self, data: SerializedDataStream) -> AsyncIterator[str]:
        async for str_chunk in decode_async_byte_stream(get_utf8_decoder(), data):
            yield str_chunk
```

## Serializer Names

Serializers are identified by a globally unique name when data is saved and then loaded
later. Once a serializer has been used to saved data this name must never be changed and
the implementation of the serializer must always remain backwards compatible. If you
need to change the implementation of a serializer you should create a new one with a new
name.

One strategy for managing data saved with an older serializer might be to register a
compatibility layer under the name of the old serializer. When dumping data you can warn
the user about the deprecation and when loading you can forward the call to the new
serializer implementation via the compatibility layer. The code below shows an example
of how to do this:

```python
from collections.abc import Callable
from typing import TypeVar
from warnings import warn

T = TypeVar("T")


class DeprecatedSerializer(Serializer[T]):
    # We don't wwant this to be used for type inference
    types = ()

    def __init__(
        self,
        old_serializer: Serializer[T],
        compatibility_layer: Callable[[SerializedData], SerializedData],
        new_serializer: Serializer[T],
        reason: str = "deprecated",
    ) -> None:
        self.name = old_serializer.name
        self.old_serializer = old_serializer
        self.compatibility_layer = compatibility_layer
        self.new_serializer = new_serializer
        self.reason = reason

    def dump_data(self, value: T) -> SerializedData:
        warn(
            f"{self.name} is deprecated and will be removed in a future version. "
            f"Use {self.new_serializer.name} instead.",
            DeprecationWarning,
            stacklevel=1,
        )
        # Do as the user says and dump the data with the old serializer
        return self.old_serializer.dump_data(value)

    def load_data(self, data: SerializedData) -> T:
        # Apply the compatibility layer to the data
        data = self.compatibility_layer(data)
        # Forward the call to the new serializer
        return self.new_serializer.load_data(data)
```

In the future you might eventually replace the warning with an exception to force users
to migrate to the new serializer. This would also give you an opportunity to remove the
old serializer implementation. You should continue to maintain the compatibility layer
until you've either migrated all data to the new format or your retention policy no
longer requires you to keep the data.

## Serializer Type Inference

Lakery inspects the [`types`][lakery.core.serializer.Serializer.types] tuples assigned
of serializers in order to automatically infer which serializer to use when saving a
value if one was not explicitly specified. If multiple serializers declare the same
`type`s they are resolved based on the order they were added to the
[serializer registry](./registries.md#infering-serializers) being used to save the
value.
