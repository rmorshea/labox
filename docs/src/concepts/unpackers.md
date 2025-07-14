# Unpackers

Unpackers define how to destructure a [`Storable`](./storables.md) class into its
constituent parts as well as how to reconstitute it from those parts. Unpackers are
typically specialized for a specific set of `Storable` subclasses that share a common
framework. For example, all [Pydantic](../integrations/3rd-party/pydantic.md) classes
use the same unpacker, which understand how to convert Pydantic models into a dictionary
of fields and values. Unpackers live independently of the `Storable` classes so that the
shape of the data is defined independently of how it is stored and serialized. It also
allows for multiple versions of an unpacker to exist for backwards compatibility.

## Defining Unpackers

To define an unpacker you need to first decide what types of `Storable` classes it
should handle. Then you can implement the `Unpacker` interface, which requires you to
provide the following:

-   `name` - a string that uniquely and permanently identifies the unpacker.
-   `unpack_object` - a method that takes a `Storable` instance and returns a dictionary
    of fields with their values as well as where and how to store them.
-   `repack_object` - a method that takes a `Storable` class and the aforementioned
    dictionary, and returns a new instance of the `Storable` class.

In the simplest case, this might look something like the following:

```python
from lakery import UnpackedValue
from lakery import Unpacker


class MyUnpacker(Unpacker):
    name = "my-unpacker@v1"

    def unpack_object(self, obj, registry):
        return {k: UnpackedValue(value=v) for k, v in obj.__dict__.items()}

    def repack_object(self, cls, contents, registry):
        return cls(**{k: u["value"] for k, u in contents.items()})
```

Then in your `Storable` class you can use this unpacker:

```python
from lakery import Storable


class MyStorable(Storable, class_id="abc123", unpacker=MyUnpacker()): ...
```

All subclasses of `MyStorable` will now use the `MyUnpacker` to unpack and repack their
data.

## Content Names

Each [value](#unpacked-values) or [stream](#unpacked-streams) returned by an unpacker is
identified by a string within the unpacked dictionary. This string is called the
"content name". This string gets passed along to:

-   The [`ContentRecord`](./database.md#content-records) that is saved as a pointer to
    the data. The name is unique amongst all the content records for a given
    [`ManifestRecord`](./database.md#manifest-records).
-   The [storage](./storages.md#storage-names) where the data is saved. From the
    storage's perspective, the content name is not globally unique and is not suitable
    as a storage key on its own.

## Unpacked Values

When you unpack a `Storable` class, the values are returned as
[`UnpackedValue`][lakery.core.unpacker.UnpackedValue] dicts. These values contain the
following information:

-   `value`: The actual value of the field.
-   `serializer` (optional): The serializer to use when saving the value.
-   `storage` (optional): The storage to use when saving the value.

If you do not specify a serializer. One will be inferred based on the type of the value.
If you do not specify a storage, the default storage in the given
[registry](./registry.md) will be used.

Unpacked values and value streams may be mixed within the same unpacked dictionary,
allowing you to unpack some fields into memory while streaming others.

## Unpacked Streams

In addition to unpacked values, you can also unpack the fields of a `Storable` class
into `UnpackedValueStream` dicts. This is useful when you can't or don't want to load
large amounts of data into memory at once. An `UnpackedValueStream` contains the
following information:

-   `value_stream`: An async iterable that yields the values of the fields.
-   `serializer` (recommended): The serializer to use when saving the values.
-   `storage` (optional): The storage to use when saving the values.

In this case providing an explicit serializer is recommended because attempting to infer
the appropriate serializer will raise a late error since the first value from the stream
must be inspected. As before, if you do not specify a storage, the default storage in
the [registry](./registry.md) will be used.

Unpacked values and value streams may be mixed within the same unpacked dictionary,
allowing you to unpack some fields into memory while streaming others.

## Unpacker Names

Unpackers are identified by a globally unique name associated with each unpacker class
within a [registry](./registry.md#adding-unpackers). Once a unpacker has been used to
saved data this name must never be changed and the implementation of the unpacker must
always remain backwards compatible. If you need to change the implementation of a
unpacker you should create a new one with a new name. In order to continue loading old
data you'll have to preserve and registry the old unpacker.
