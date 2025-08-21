# Storables

Labox provides a handful of built-in storables. Most notably for dataclasses.

## Dataclasses

Labox provides a base [`DataclassStorable`][labox.builtin.storables.DataclassStorable]
class that can be added to a dataclass to make it
[storable](../../usage/index.md#saving-storables). After inheriting from this base class
each field metadata can be used to specify an explicit serializer and storage for that
field.

```python
from dataclasses import dataclass, field
from labox.builtin import DataclassStorable, CsvSerializer

@dataclass
class MyDataClass(DataclassStorable):
    data: list[list[int]] = field(
        metadata={
            "serializer": CsvSerializer
        }
    )
```

Ultimately a dataclass is stored under one or more
[`ContentRecord`s](../../concepts/database.md#content-records). The majority of the
dataclass is stored in a "body" record, which is nominally a JSON serializable object.
The serializer and storage for this body record can be customized by overriding the
[`storable_body_serializer`][labox.builtin.storables.DataclassStorable.storable_body_serializer]
and/or
[`storable_body_storage`][labox.builtin.storables.DataclassStorable.storable_body_storage]
methods in the dataclass itself.

Fields within the dataclass are stored within this body record unless they have a
separate `storage` or
[stream serializer](../../concepts/serializers.md#stream-serializers) specified in their
metadata.

## Simple Values

If all you need to do is store a single value you can do so using the
[`ValueStorable`][labox.builtin.storables.ValueStorable] class. In addition to the value
you want to save you can manually specify its
[serializer](../../concepts/serializers.md) and [storage](../../concepts/storages.md):

```python
from labox.builtin import ValueStorable, JsonSerializer

storable = ValueStorable(value="Hello, World!", serializer=JsonSerializer)
```

## Simple Streams

Similarly, if you want to store a stream of values, you can use the
[`StreamStorable`][labox.builtin.storables.StreamStorable] class. It works similarly to
[`ValueStorable`][labox.builtin.storables.ValueStorable] but is designed for storing an
asynchronous stream of values. You can also specify a serializer and storage:

```python
from labox.builtin import StreamStorable, JsonStreamSerializer

storable = StreamStorable(
    value="Hello, World!",
    serializer=JsonStreamSerializer
)
```
